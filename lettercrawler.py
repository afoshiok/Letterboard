from bs4 import BeautifulSoup
import requests
import pandas as pd

def crawl(username):
    link = f"https://letterboxd.com/{username}/films/diary/"
    page = requests.get(link)
    soup = BeautifulSoup(page.content, 'html.parser').tbody #Wraps the http request in BS4
    data = {
        "Film Name" : [],
        "ID": [],
        "Release Year": []
    }
    for i in soup.find_all("td", {"class": "td-film-details"}):
        data["Film Name"].append(i.h3.text)
        data["ID"].append(i.div["data-film-id"])
        data["Release Year"].append(i.span.text)

    film_df = pd.DataFrame(data)
    
    print(film_df)
    



if __name__ == "__main__":
    crawl('FavourOshio')