from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
from dotenv import load_dotenv
import time
import asyncio

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
    soup = BeautifulSoup(page.content, 'html.parser').tbody #Wraps the http request in BS4

    film_slugs = []

    for i in soup.find_all("tr"):
        film_slug = i.find("td", {"class": "td-film-details"}).div["data-film-slug"]
        film_slugs.append(get_tmdb_id(film_slug))

    print(film_slugs)



if __name__ == "__main__":
    start_time = time.time()
    crawl('FavourOshio')
    # print(film_details(27256))
    print("--- %s seconds ---" % (time.time() - start_time)) #task runtime