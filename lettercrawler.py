from datetime import datetime
from bs4 import BeautifulSoup
import requests
import polars as pl
import os
from dotenv import load_dotenv
import time
import asyncio
import aiohttp
import pycountry
import streamlit as st
load_dotenv()


async def fetch_tmdb_id(session, slug):
    async with session.get(f"https://letterboxd.com/film/{slug}/details") as response:
        film = BeautifulSoup(await response.text(), 'lxml')
        film_id = film.body["data-tmdb-id"]
        return film_id


async def fetch_film_details(session, id):
    film_req_link = f"https://api.themoviedb.org/3/movie/{id}"
    head = {
        "accept": "application/json",
        "Authorization": f"Bearer {st.secrets['API']}" 
        }
    crew_request_link = f"https://api.themoviedb.org/3/movie/{id}/credits"
    async with session.get(film_req_link, headers=head) as response:
        film_data = await response.json()
    async with session.get(crew_request_link, headers=head) as crew_response:
        crew_data = await crew_response.json()
    
    return film_data,crew_data
    
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
            return 1 #User has less than 50 films logged
    except Exception as e:
        print(f"Error parsing HTML: {e}")
        return None


async def crawl(username, page, session): #Creates dataframe for data analysis
    user_films = []

    async with aiohttp.ClientSession() as session:
        link = f"https://letterboxd.com/{username}/films/diary/page/{page}"
        async with session.get(link) as response:
            if response.status == 200:
                soup = BeautifulSoup(await response.text(), 'lxml') #Wraps the http request in BS4

                for i in soup.tbody.find_all("tr"):
                    film_slug = i.find("td", {"class": "td-film-details"}).div["data-film-slug"]

                    tmdb_id = await fetch_tmdb_id(session, film_slug)
                    film_tmdb_data, crew_tmdb_data = await fetch_film_details(session, tmdb_id)

                    parse_rating = i.find("td", {"class": "td-rating rating-green"})
                    parse_log_date = i.find("td", {"class": "td-day diary-day center"})

                    film_rating = int(parse_rating.find('input').get('value'))
                    film_log_date = parse_log_date.find('a').get('href').split('/')[5:8]
                    directors = []
                    director_gender = [] #Index corresponds to directors index.
                    writers = []
                    writer_gender = []
                    for crew_member in crew_tmdb_data.get("crew", []):
                        if crew_member['job'] == 'Director':
                            directors.append(crew_member['name'])
                            director_gender.append(crew_member['gender'])
                    for crew_member in crew_tmdb_data.get("crew", []):
                        if crew_member['job'] == 'Writer' or crew_member['job'] =='Screenplay':
                            writers.append(crew_member['name'])
                            writer_gender.append(crew_member['gender'])
                    release_date_str = film_tmdb_data.get("release_date", "")
                    try:
                        release_date = datetime.strptime(release_date_str, '%Y-%m-%d').date()
                    except ValueError:
                            release_date = None  
                    film_ob = {
                        "Name": film_tmdb_data.get("title", ""),
                        "Letterboxd Rating": film_rating / 2,
                        "TMDb ID": tmdb_id,
                        "Log Date": datetime.strptime(f"{film_log_date[1]}-{film_log_date[2]}-{film_log_date[0]}", '%m-%d-%Y').date(),
                        "Release Date": release_date,
                        "Budget": film_tmdb_data.get("budget", 0),
                        "Production Countries":[pycountry.countries.get(alpha_2=country.get("iso_3166_1")).name
                                                if pycountry.countries.get(alpha_2=country.get("iso_3166_1")) is not None
                                                 else None
                                                 for country in film_tmdb_data.get("production_countries", []) ],
                        "Genre(s)": [genre.get("name") for genre in film_tmdb_data.get("genres", [])],
                        "Runtime (Minutes)": film_tmdb_data.get("runtime", 0),
                        "Directors": directors,
                        "Director Gender": director_gender, #List of 0s, 1s and 2s; 0 and 1 = Woman, 2 = Man
                        "Writers": writers,
                        "Writer Gender": writer_gender
                    }
                    user_films.append(film_ob)
                await asyncio.sleep(1)
            
            film_df = pl.DataFrame._from_dicts(user_films)
            return film_df
                # else:
                #     print(f"Error: Failed to fetch data from {link}. Status code: {response.status}")
                #     return pl.DataFrame()  # Return an empty dataframe in case of an error

async def crawl_all(username, pages):
    final_df = pl.DataFrame()

    # Create a single session and reuse it
    async with aiohttp.ClientSession() as session:
        # Create a list of tasks, one for each page
        tasks = [crawl(username, i, session) for i in range(1, pages + 1)]

        # Execute tasks concurrently and gather results
        result = await asyncio.gather(*tasks)

        # Concatenate dataframes in the results
        final_df = pl.concat(result)

    return final_df

        
if __name__ == "__main__":
    start_time = time.time()
    # loop = asyncio.get_event_loop()
    # final_df = loop.run_until_complete(crawl('FavourOshio'))
    pl.Config.set_tbl_rows(25)
    user = "kcabs"
    loop = asyncio.get_event_loop()
    final_df = loop.run_until_complete(crawl_all(user, get_total_pages(user)))
    print(final_df)
    print(len(final_df))
    # print(get_total_pages(user))
    print("--- %s seconds ---" % (time.time() - start_time)) #task runtime