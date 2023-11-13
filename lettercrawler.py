from bs4 import BeautifulSoup
import requests
import polars as pl
import os
from dotenv import load_dotenv
import time
import asyncio
import aiohttp

load_dotenv()


async def fetch_tmdb_id(session, slug):
    async with session.get(f"https://letterboxd.com/film/{slug}/details") as response:
        film = BeautifulSoup(await response.text(), 'lxml')
        film_id = film.body["data-tmdb-id"]
        return film_id


async def fetch_film_details(session, id):
    req_link = f"https://api.themoviedb.org/3/movie/{id}"
    head = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.environ.get('api')}" 
        }
    
    async with session.get(req_link, headers=head) as response:
        return await response.json()
    
def get_total_pages(username):
    link = f"https://letterboxd.com/{username}/films/diary/"
    
    try:
        response = requests.get(link)
        response.raise_for_status()  # Check if the request was successful
    except requests.exceptions.RequestException as e:
        print(f"Error making the request: {e}")
        return None

    try:
        pages_soup = BeautifulSoup(response.text, 'lxml')
        count_pages = pages_soup.find("div", {"class": "paginate-pages"})
        
        if count_pages:
            li_tags = count_pages.ul.find_all('li')
            
            if li_tags:
                total_pages = int(li_tags[-1].get_text())
                return total_pages
            else:
                print("No <li> tags found")
                return None
        else:
            print("No paginate-pages class found")
            return None
    except Exception as e:
        print(f"Error parsing HTML: {e}")
        return None


async def crawl(username,page): #Creates dataframe for data analysis
    user_films = []

    async with aiohttp.ClientSession() as session:
        for page in range(1, page + 1):
            link = f"https://letterboxd.com/{username}/films/diary/page/{page}"
            async with session.get(link) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'lxml') #Wraps the http request in BS4

                    for i in soup.tbody.find_all("tr"):
                        film_slug = i.find("td", {"class": "td-film-details"}).div["data-film-slug"]

                        tmdb_id = await fetch_tmdb_id(session, film_slug)
                        tmdb_data = await fetch_film_details(session, tmdb_id)

                        parse_rating = i.find("td", {"class": "td-rating rating-green"})
                        parse_log_date = i.find("td", {"class": "td-day diary-day center"})

                        film_rating = int(parse_rating.find('input').get('value'))
                        film_log_date = parse_log_date.find('a').get('href').split('/')[5:8]
                        film_ob = {
                            "Name": tmdb_data.get("title", ""),
                            "Letterboxd Rating": film_rating / 2,
                            "TMDb ID": tmdb_id,
                            "Log Date": f"{film_log_date[1]}-{film_log_date[2]}-{film_log_date[0]}",
                            "Release Date": tmdb_data.get("release_date", ""),
                            "Budget": tmdb_data.get("budget", ""),
                            "Production Countries":[country.get("iso_3166_1") for country in tmdb_data.get("production_countries", []) ],
                            "Genre(s)": [genre.get("name") for genre in tmdb_data.get("genres", [])],
                            "Runtime (Minutes)": tmdb_data.get("runtime", "")
                        }
                        user_films.append(film_ob)
                await asyncio.sleep(1)
            
            film_df = pl.DataFrame._from_dicts(user_films)
            return film_df
                # else:
                #     print(f"Error: Failed to fetch data from {link}. Status code: {response.status}")
                #     return pl.DataFrame()  # Return an empty dataframe in case of an error

        
if __name__ == "__main__":
    start_time = time.time()
    loop = asyncio.get_event_loop()
    final_df = loop.run_until_complete(crawl('FavourOshio', get_total_pages("FavourOshio")))
    pl.Config.set_tbl_rows(500)
    print(final_df)
    print(get_total_pages("FavourOshio"))
    print("--- %s seconds ---" % (time.time() - start_time)) #task runtime