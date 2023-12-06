# LetterBoard

## At-A-Glance
Letterboxd is a social media platform where movie lovers can share, review, and discover movies. Originally, I wanted to create a web app that just creates a more complete dataframe, with additional film data instead of just a title and log date, based on your diary data exported from Letterboxd. However, I wanted a project that allowed me to go through the full ETL process without storing the data. So, I decided to create a dashboard with the data generated.

Usually, when I make a web app like this, I use the FAV (FastApi and Vue) stack, that's where [Streamlit](https://streamlit.io/) comes in. Streamlit enabled me to create the entire app completely in Python, and not worry about converting my dataframes into JSON responses.

Try the web app for yourself with either one of these links:
- https://letterboard.streamlit.app/
- https://letterboard.up.railway.app/ (I had Railway credits left over from my Senior year project, so why not.)

![Letterboard](https://github.com/afoshiok/Letterboxd-EDA/assets/89757138/9d5f181c-d386-4054-9c0a-a7a802225012)

# Tech Stack

**Frontend and Backend:**
- **Language(s):**

  [![Languages](https://skillicons.dev/icons?i=python)](https://skillicons.dev)

- **Frameworks, Tools and Libraries:** Polars, Asyncio, Aiohttp, Streamlit, Beautiful Soup 4 and Plotly

**Project Management and Documentation:**
- Notion (My second brain)

# Extracting the Data from Letterboxd
Originally I wanted users to drag and drop their exported Letterboxd data CSV and add the transformations on there, but I didn't want users to have to download a CSV every time they wanted to use the app. So I decided to make a web scraper instead. The data extraction step is broken up into 4 asynchronous functions and 1 synchronous function, which can all be found [here](https://github.com/afoshiok/Letterboxd-EDA/blob/main/lettercrawler.py).

First, the app will run the **get_total_pages("Letterboxd username")** synchronous function to the total number of pages a user has in their Letterboxd diary (Letterboxd paginate your diary to have 50 films logged per page). This value is then passed as the second parameter in the async **crawl_all()** function, which runs the **crawl()** function on all pages, gathers the results, and returns a final dataframe.
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
Polars is a dataframe library written in Rust, with one of its main selling points being effective parallelism. I chose to use Polars over Pandas for this project because it is 50x faster (Their claim), and I want to take advantage of their Lazyframe feature. 

A majority of dataframe creation is done in the **crawl()** function. This function creates a [Client Session](https://docs.aiohttp.org/en/stable/client_reference.html) with aiohttp of a diary page, and uses [Beautiful Soup](https://pypi.org/project/beautifulsoup4/) to scrape the data from each row of logged film data and places them into a dictionary with this schema:
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
The only items scraped from the diary page are the "Name", "Letterboxd Rating" and "Log Date", the rest are from the TMDb API. The way this works is while BS4 scrapes each film's data from the table rows, it also gets the film's "slug". This will then be used to send a request to the film's Letteboxd page with the fetch_tmdb_id() function, to get the film's TMDb ID. That ID is then passed to the fetch_film_details() function, which sends requests to two TMDb API endpoints and returns both the film and credits data. Each dictionary is then appended to a list which is then converted into a Polars Dataframe.

# Streamlit Aggregations + Visulaizations (Load)
All aggregation queries can be found [here](https://github.com/afoshiok/Letterboxd-EDA/blob/main/aggregations.py), so I'm just going to walk through a couple. With the aggregation used to create the pie chart for the directors' and writers' genders, I needed to count the instances of 0s, 1s, 2s, and 3s in the lists, as well as give them their proper labels. Here's how I achieved that:
```py
director_gender = df.select(
                            pl.col("Director Gender").list.count_matches(2).alias("Male Directors"),
                            pl.col("Director Gender").list.count_matches(1).alias("Female Directors"),
                            pl.col("Director Gender").list.count_matches(3).alias("Non-Binary Directors"),
                            pl.col("Director Gender").list.count_matches(0).alias("Director's Gender Not Specified")
                            )
```
This counted the genders of the directors for each film resulting in a dataframe that looked something like this:

| Male Directors | Female Directors | Non-Binary Directors | Director's Gender Not Specified |
|----------------|------------------|----------------------|---------------------------------|
| 1              | 0                | 0                    | 0                               |
| 2              | 0                | 0                    | 0                               |
| 0              | 1                | 0                    | 0                               |
| 1              | 0                | 0                    | 0                               |

All I have to do now is just sum the values of each column and transpose the dataframe for it to be readable by Plotly:
```py
director_gender_sum = director_gender.select(
    pl.col("Male Directors").sum(),
    pl.col("Female Directors").sum(),
    pl.col("Non-Binary Directors").sum(),
    pl.col("Director's Gender Not Specified").sum()
).transpose(include_header=True,header_name="categories")
```
Finally, this results in a dataframe that looks like this:

| categories                      | count |
|---------------------------------|-------|
| Male Directors                  | 4     |
| Female Directors                | 1     |
| Non-Binary Directors            | 0     |
| Director's Gender Not Specified | 0     |

This is what the graphs looks like on the website:

![image](https://github.com/afoshiok/Letterboxd-EDA/assets/89757138/745dbe24-5bb3-4e6a-abca-87a7435af499)


One aggregation I found particularly challenging was the one needed for the "Countries Distribution" Tree Map. Originally I wanted to visualize the country data as a Mapbox map, this meant I had to convert the values in the "Production Countries" field from ISO 3166-2 format (ex: Russian = RU, United States = US, etc.) to ISO 3166-3 format to match the values in my geojson. I used a Python package called py country to make the conversion in the extraction phase:
```py
    "Production Countries":[pycountry.countries.get(alpha_2=country.get("iso_3166_1")).alpha_3
                            if pycountry.countries.get(alpha_2=country.get("iso_3166_1")) is not None
                             else None
                             for country in film_tmdb_data.get("production_countries", []) ],
```
Creating a dataframe for this visualization was fairly simple:
```py
countries_df = df.select(pl.col("Production Countries").list.explode())
countries_df_count = countries_df.group_by("Production Countries").count()
```
I opted to using a tree map instead after running into issue with displaying my map, so I switch the countries format from ISO 3166-3 to just display the country name. Using the same aggregation script and changing the pycountry value from alpha_3 to name I got this resulting dataframe:

| Production Countries     | count |
|--------------------------|-------|
| United States of America | 150   |
| United Kingdom           | 24    |
| Republic of Korea        | 7     |

This is what the tree map looks like on the website:

![image](https://github.com/afoshiok/Letterboxd-EDA/assets/89757138/d1118080-7b49-4d2a-87bb-b38e7cf0473e)
