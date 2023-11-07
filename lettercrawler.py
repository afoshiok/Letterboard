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
        film = BeautifulSoup(await response.text(), 'html.parser')
        film_id = film.body["data-tmdb-id"]
        return film_id
    # film_page = requests.get(f"https://letterboxd.com/film/{slug}/details")
    # film = BeautifulSoup(film_page.content, 'html.parser')
    # film_id = film.body["data-tmdb-id"]
    # return film_id

async def fetch_film_details(session, id):
    req_link = f"https://api.themoviedb.org/3/movie/{id}"
    head = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.environ.get('api')}" 
        }
    
    async with session.get(req_link, headers=head) as response:
        return await response.json()
    
    # response = requests.get(req_link, headers=head)
    # return response.json()

async def crawl(username):
    async with aiohttp.ClientSession() as session:
        link = f"https://letterboxd.com/{username}/films/diary/"
        async with session.get(link) as response:
            soup = BeautifulSoup(await response.text(), 'lxml') #Wraps the http request in BS4
            pages = soup.find("div", {"class": "paginate-pages"}).ul
    # page = requests.get(link)
    # soup = BeautifulSoup(page.content, 'lxml') #Wraps the http request in BS4
    # pages = soup.find("div", {"class": "paginate-pages"}).ul

            film_slugs = []

            for i in soup.tbody.find_all("tr"):
                film_slug = i.find("td", {"class": "td-film-details"}).div["data-film-slug"]

                tmdb_id = await fetch_tmdb_id(session, film_slug)
                tmdb_data = await fetch_film_details(session, tmdb_id)

                # tmdb_data = film_details(
                #         get_tmdb_id(film_slug)
                #     )

                parse_rating = i.find("td", {"class": "td-rating rating-green"})
                parse_log_date = i.find("td", {"class": "td-day diary-day center"})

                film_rating = int(parse_rating.find('input').get('value'))
                film_log_date = parse_log_date.find('a').get('href').split('/')[5:8]
                film_ob = {
                    "Film Rating": film_rating,
                    "TMDb Rating": tmdb_id,
                    "Log Date": f"{film_log_date[1]}-{film_log_date[2]}-{film_log_date[0]}",
                    "Adult": tmdb_data["adult"],
                    "Budget": tmdb_data["budget"]
                }
                film_slugs.append(film_ob)
                # break #For testing!
        

    print(film_slugs)
    # print(tmdb_data)



if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(crawl('FavourOshio'))
    # print(film_details(27256))
    print("--- %s seconds ---" % (time.time() - start_time)) #task runtime