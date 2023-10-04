from bs4 import BeautifulSoup
import requests
import pandas as pd
import os

def film_details(id):
    req_link = f"https://api.themoviedb.org/3/movie/{id}"
    head = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.environ.get('TOKEN')}" 
        }
    
    response = requests.get(req_link, headers=head)
    print(response.text)



def crawl(username):
    link = f"https://letterboxd.com/{username}/films/diary/"
    page = requests.get(link)
    soup = BeautifulSoup(page.content, 'html.parser').tbody #Wraps the http request in BS4
    data = {
        "Film Name" : [],
        "ID": [],
        "Release Year": []
    }
    for i in soup.find_all("tr"):
        film_id = i.find("td", {"class": "td-film-details"}).div["data-film-id"]

        data["Film Name"].append(i.find("h3", {"class": "headline-3 prettify"}).text)
        data["ID"].append(film_id)
        data["Release Year"].append("N/A")

    film_df = pd.DataFrame(data)
    
    print(film_df)
    



if __name__ == "__main__":
    # crawl('FavourOshio')
    film_details(27256)