from bs4 import BeautifulSoup
import requests

def crawl(username):
    link = f"https://letterboxd.com/{username}/films/diary/"
    page = requests.get(link)
    soup = BeautifulSoup(page.text, 'html.parser') #Wraps the http request in BS4
    print(soup.prettify()) #Makes the page HMTL readable 

if __name__ == "__main__":
    crawl('FavourOshio')