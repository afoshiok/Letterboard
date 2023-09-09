from bs4 import BeautifulSoup
import requests

def crawl(username):
    link = f"https://letterboxd.com/{username}/films/diary/"
    page = requests.get(link)
    print(page.status_code) 

if __name__ == "__main__":
    crawl('FavourOshio')