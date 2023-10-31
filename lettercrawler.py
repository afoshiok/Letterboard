from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
from dotenv import load_dotenv
import time
import asyncio
import aiohttp

load_dotenv()


def get_tmdb_id(slug):
    film_page = requests.get(f"https://letterboxd.com/film/{slug}/details")
    film = BeautifulSoup(film_page.content, 'html.parser')
    film_id = film.body["data-tmdb-id"]
    return film_id

def film_details(id):
    req_link = f"https://api.themoviedb.org/3/movie/{id}"
    head = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.environ.get('api')}" 
        }
    
    response = requests.get(req_link, headers=head)
    return response.json()

def crawl(username):
    link = f"https://letterboxd.com/{username}/films/diary/"
    page = requests.get(link)
    soup = BeautifulSoup(page.content, 'lxml') #Wraps the http request in BS4
    pages = soup.find("div", {"class": "paginate-pages"}).ul

    film_slugs = []

    for i in soup.tbody.find_all("tr"):
        film_slug = i.find("td", {"class": "td-film-details"}).div["data-film-slug"]

        parse_rating = i.find("td", {"class": "td-rating rating-green"})
        parse_log_date = i.find("td", {"class": "td-day diary-day center"})

        film_rating = int(parse_rating.find('input').get('value'))
        film_log_date = parse_log_date.find('a').get('href').split('/')[5:8]
        film_ob = {
            "Film Rating": film_rating,
            "TMDb Rating": get_tmdb_id(film_slug),
            "Log Date": f"{film_log_date[1]}-{film_log_date[2]}-{film_log_date[0]}"
        }
        film_slugs.append(film_ob)
        break #For testing!
        

    print(film_slugs)



if __name__ == "__main__":
    start_time = time.time()
    crawl('FavourOshio')
    # print(film_details(27256))
    print("--- %s seconds ---" % (time.time() - start_time)) #task runtime