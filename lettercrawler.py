from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
from dotenv import load_dotenv
import time

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

def process_film(film):
    film_id = film.find("td", {"class": "td-film-details"}).div["data-film-id"]
    film_slug = film.find("td", {"class": "td-film-details"}).div["data-film-slug"]
    tmdb_id = get_tmdb_id(film_slug)

    film_name = film.find("h3", {"class": "headline-3 prettify"}).text
    release_year = film_details(tmdb_id)["release_date"]

    return {
        "Film Name": film_name,
        "LetterID": film_id,
        "TMDb ID": tmdb_id,
        "Release Year": release_year
    }

def crawl(username):
    link = f"https://letterboxd.com/{username}/films/diary/"
    page = requests.get(link)
    soup = BeautifulSoup(page.content, 'html.parser').tbody #Wraps the http request in BS4
    data = {
        "Film Name" : [],
        "LetterID": [],
        "TMDb ID": [],
        # "Film Slug": [],
        "Release Year": []
    }
    for i in soup.find_all("tr"):
        film_id = i.find("td", {"class": "td-film-details"}).div["data-film-id"]
        film_slug = i.find("td", {"class": "td-film-details"}).div["data-film-slug"]
        tmdb_id = get_tmdb_id(film_slug)

        data["Film Name"].append(i.find("h3", {"class": "headline-3 prettify"}).text)
        data["LetterID"].append(film_id)
        data["TMDb ID"].append(tmdb_id)
        data["Release Year"].append(film_details(tmdb_id)["release_date"])

    film_df = pd.DataFrame(data)
    
    print(film_df)
    



if __name__ == "__main__":
    start_time = time.time()
    crawl('FavourOshio')
    print("--- %s seconds ---" % (time.time() - start_time)) #task runtime