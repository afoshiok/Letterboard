# LetterBoard

## At-A-Glance
Letterboxd is a social media platform where movie lovers can share, review, and discover movies. Originally, I wanted to create a web app that just creates a more complete data frame, with film data instead of just a title and log date, based on your diary data exported from Letterboxd. However, I wanted to create a web app that allows me to go through the full ETL process. So, I decided to create a dashboard with the data generated.

Usually, when I make a web app like this, I would use the FAV (FastApi and Vue) stack, that's where [Streamlit](https://streamlit.io/) comes in. Streamlit enabled me to create the entire app completely in Python.

![Letterboard](https://github.com/afoshiok/Letterboxd-EDA/assets/89757138/9d5f181c-d386-4054-9c0a-a7a802225012)

# Tech Stack

**Frontend and Backend:**
- **Language(s):**

  [![Languages](https://skillicons.dev/icons?i=python)](https://skillicons.dev)

- **Frameworks, Tools and Libraries:** Polars, Asyncio, Aiohttp, Streamlit, Beautiful Soup 4 and Plotly

**Project Management and Documentation:**
- Notion (My second brain)

# Extracting the Data from Letterboxd
Originally I wanted users to drag and drop their exported Letterboxd data csv and add do the transformations on there, but I didn't want users to have to download a csv everytime they wanted to use the app. So I decided to make a webscapers instead. The data extraction step is broken up into 4 asyncronus functions and 1 syncronous function, which can all be found [here](https://github.com/afoshiok/Letterboxd-EDA/blob/main/lettercrawler.py).

First, the app will run the **get_total_pages("_Letterboxd username_")** syncronous function to the total number of pages a user has in their Letterboxd diary (Letterboxd paginate your diary to have 50 fils logged per page). This value is then passed as the second parameter in async **crawl_all()** function, which runs the **crawl()** funtion on all pages, gathers the results, and returns a final dataframe.
```py
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
```

# Letterboxd Diary to Polars Dataframe (Transform)
A majority of dataframe cretation is done in the **crawl()** function. This function creates a [Client Session](https://docs.aiohttp.org/en/stable/client_reference.html) with aiohttp of a diary page, and uses [Beautiful Soup](https://pypi.org/project/beautifulsoup4/) to scrape the data from each row of logged film data and places them into a dictionary with this schema:
```py
{
  "Name": String,
  "Letterboxd Rating": Int,
  "TMDb ID": Int,
  "Log Date": Datetime,
  "Release Date": String, #later converted to Datetime
  "Budget": Int, #This value is never used but good to have
  "Production Countries": [String],
  "Genre(s)": [String],
  "Runtime": Int,
  "Directors": [String],
  "Director Gender": [Int], #Index corresopnds to "Directors" index
  "Writers": [String],
  "Writer Gender": [Int], #Index corresopnds to "Writers" index
}
```
The only items scraped from the diary page are the "Name", "Letterboxd Rating" and "Log Date", the rest are from the TMDb API. The way this works, is while BS4 scrapes each films data from the table rows, it also gets the films "slug". Which will then be used to send a request to the film's Letteboxd page with the **fetch_tmdb_id()** function, to get the film's TMDb ID. That ID is then passed to **fetch_film_details()** function, which sends requests to two TMDb API endpoints and returns both the film and credits data. Each dictionary is then appended to a list which is then converted into a Polars Dataframe.
