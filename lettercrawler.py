from bs4 import BeautifulSoup
import requests
import pandas as pd
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
    


async def crawl(username,page): #Creates dataframe for data analysis
    user_films = []

    async with aiohttp.ClientSession() as session:
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
                        "Name": tmdb_data["title"],
                        "Letterboxd Rating": film_rating,
                        "TMDb ID": tmdb_id,
                        "Log Date": f"{film_log_date[1]}-{film_log_date[2]}-{film_log_date[0]}",
                        "Release Date": tmdb_data["release_date"],
                        "Budget": tmdb_data["budget"],
                        "Production Countries":[country.get("iso_3166_1") for country in tmdb_data["production_countries"] ],
                        "Genre(s)": [genre.get("name") for genre in tmdb_data["genres"]],
                        "Runtime (Minutes)": tmdb_data["runtime"]
                    }
                    user_films.append(film_ob)
                        # break #For testing!
        
                film_df = pd.DataFrame.from_records(user_films)
                return film_df
        
async def crawl_all(user):
    tasks = [] 
    response = requests.get("https://letterboxd.com/FavourOshio/films/diary/")
    soup = BeautifulSoup(await response.text(), 'lxml')
    count_pages = soup.find("div", {"class": "paginate-pages"}).ul.find_all('li')
    total_pages = int(count_pages[-1].get_text())
    
    for i in range(1, total_pages + 1):
        tasks.append(crawl(user, i))
    
    print(tasks)



if __name__ == "__main__":
    start_time = time.time()
    # loop = asyncio.get_event_loop()
    # final_df = loop.run_until_complete(crawl('Favourshio',2))
    # print(final_df)
    asyncio.run(crawl_all("FavourOshio"))
    # print(film_details(27256))
    print("--- %s seconds ---" % (time.time() - start_time)) #task runtime