import streamlit as st
import polars as pl
import time
import asyncio
import aiohttp
import pycountry
from bs4 import BeautifulSoup
import requests
import asyncio
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from dotenv import load_dotenv
import os

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
        "Authorization": f"Bearer {os.environ.get('api')}" 
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

#Streamlit Visualizations
st.set_page_config(page_title="LetterBoard", layout='wide')
col_top1, col_top2 = st.columns((8,.45))
with col_top1:
    st.title("Letterboxd Data Analysis")
with col_top2:
    if st.button("Clear cache"):
        # Clear values from *all* all in-memory and on-disk data caches:
        # i.e. clear values from both square and cube
        st.cache_data.clear()
st.markdown("""Letterboxd is a social media platform for film lovers to rate, discuss, and discover movies.
            This app scrapes data from your Letterboxd diary, and maps movie data to corresponding data in the TMDb API (The more movies you have logged, the longer this will take).
            _No affiliation with Letterboxd itself._
            """)
st.markdown("Made by [Favour O.](https://www.linkedin.com/in/favour-oshio/), inspired by [Tyler Richards' Goodreads App](https://goodreads.streamlit.app/).")


@st.cache_data()
def load_user_data(username):
    try:
        pages = get_total_pages(username)
        print(f"{username} has {pages}")
        loop = asyncio.new_event_loop()
        df = loop.run_until_complete(crawl_all(username, pages))  # noqa: F405
        return df
    except Exception as e:
        st.error(f"Error loading user data: {str(e)}")
        return None
col_top1, col_top2 = st.columns((8,2))
username = st.text_input('Username', placeholder="Tip: To avoid errors don't copy + paste the examples.")
st.markdown("""_If you don't have a Letterboxd try some of my favorite accounts: :green[fumilayo] (Ayo Edebiri), :green[jaredgilman] (Jared Gilman), :green[girlactress] (Rachel Sennott)_ """)
submitted = st.button("Submit")
df = None
year_count = None
release_count = None

if submitted:
    tab1, tab2, tab3 = st.tabs(["Films Logged", "Film Release Year", "Film Runtimes"])
    df = load_user_data(username).drop_nulls()
    print(df)

if df is not None:

    

    with st.container():
        with tab1:
            log_years = df.with_columns(pl.col("Log Date").dt.year().cast(pl.Utf8).to_physical().alias("Log Year"))
            log_years_script = (
                log_years.lazy()
                .group_by("Log Year")
                .agg(pl.count("Log Date").alias("Films Logged"))
                .sort("Log Year")
            )
            log_years_count = log_years_script.collect()
            log_years_graph = px.bar(log_years_count, x="Log Year", y="Films Logged", title="Films Logged by Year")
            log_years_graph.update_xaxes(type='category')
            max_log_years_df = log_years_script.filter(pl.col("Films Logged") == pl.col("Films Logged").max()).collect()
            max_log_year = max_log_years_df.to_dicts()
            years_format = 'years' if len(log_years_count) > 1 else 'year'
            st.markdown(f"""You logged a total of :green[**{len(df)}***] films in your diary over the course of {len(log_years_count)} {years_format}, with :green[**{max_log_year[0]["Log Year"]}**] having the most logged films!""")
            st.plotly_chart(log_years_graph, use_container_width=True)
            st.markdown("\*  = Only films logged with a date will be counted towards your diary.")
        
        with tab2:
            release_years = df.with_columns(pl.col("Release Date").dt.year().cast(pl.Utf8).to_physical().alias("Release Year"))
            release_years_script = (
            release_years.lazy()
                .group_by("Release Year")
                .agg(pl.count().alias("Films Logged"))
                .drop_nulls()
                .sort("Release Year")
            )
            re_count = release_years_script.collect()
            re_years_graph = px.bar(re_count, x="Release Year", y="Films Logged", title="Films Logged by Release Year")
            re_years_graph.update_xaxes(type='category')
            max_re_count = release_years_script.filter(pl.col("Films Logged") == pl.col("Films Logged").max()).collect()
            max_release_year = max_re_count.to_dicts()
            oldest_film_df = release_years.filter(pl.col("Release Year") == pl.col("Release Year").min()).bottom_k(1, by=["Log Date"])
            oldest_film = oldest_film_df.to_dicts()
            st.markdown(f"""You logged a total of :green[**{len(df)}***] films in your diary, with most of your films logged being released in :green[**{max_release_year[0]["Release Year"]}**]!
                        Your oldest film logged was :green[_**"{oldest_film[0]["Name"]}"**_], released on {oldest_film[0]["Release Date"].strftime("%B %d, %Y")}. """)
            st.plotly_chart(re_years_graph, use_container_width=True)
            st.markdown("\*  = Only films logged with a date will be counted towards your diary.")

        with tab3:
            def categorize_runtime(runtime):
                if runtime <= 60:
                    return 'Short (Under 60mins)'
                elif 60 <= runtime <= 90:
                    return 'Medium (60 - 90mins)'
                elif 90 < runtime:
                    return 'Long (90mins <)'
            film_runtimes = df.with_columns(pl.col("Runtime (Minutes)").map_elements(categorize_runtime, pl.Utf8).alias("Runtime Category"))
            runtime_category_counts = film_runtimes.group_by('Runtime Category').agg(pl.col('Runtime Category').count().alias('Films Logged')).sort("Runtime Category", descending=True)
            runtime_graph = px.bar(runtime_category_counts, x="Runtime Category", y="Films Logged", title="Films Logged by Runtime")
            top_runtime = runtime_category_counts.filter(pl.col("Films Logged") == pl.col("Films Logged").max()).to_dicts()
            shortest_film = film_runtimes.filter(pl.col("Runtime (Minutes)") != 0).bottom_k(1, by="Runtime (Minutes)").to_dicts()
            longest_film = film_runtimes.filter(pl.col("Runtime (Minutes)") != 0).top_k(1, by="Runtime (Minutes)").to_dicts()
            st.markdown(f"""You watched films with a {top_runtime[0]["Runtime Category"].lower()} runtime the most. The shortest film you watched was :green[_**"{shortest_film[0]["Name"]}"**_]
                        with a runtime of {shortest_film[0]["Runtime (Minutes)"]} minutes. The longest film you watch was :green[_**"{longest_film[0]["Name"]}"**_]  with a runtime of {longest_film[0]["Runtime (Minutes)"]} minutes.""")
            st.plotly_chart(runtime_graph, use_container_width=True)

    st.divider()
                
    col_01, col_center, col_02 = st.columns((0.5,8,0.5))
    
    with col_center:
        with st.container():
            st.header("Writer/Director Demographics")
            st.markdown("Explore the diverse landscape of film makers with insightful pie graphs depicting the gender distribution among both directors and writers.")
            pie_color = ['#ff8000', '#00e054', '#ffffff', '#40bcf4' ]
            column_1, column_2 = st.columns(2)
            with column_1:
                director_gender = df.select(
                                    pl.col("Director Gender").list.count_matches(2).alias("Male Directors"),
                                    pl.col("Director Gender").list.count_matches(1).alias("Female Directors"),
                                    pl.col("Director Gender").list.count_matches(3).alias("Non-Binary Directors"),
                                    pl.col("Director Gender").list.count_matches(0).alias("Not Specified")
                                    )
                director_gender_sum = director_gender.select(
                    pl.col("Male Directors").sum(),
                    pl.col("Female Directors").sum(),
                    pl.col("Non-Binary Directors").sum(),
                    pl.col("Not Specified").sum()
                ).transpose(include_header=True,header_name="categories")

                director_explode = df.select(pl.col("Directors").list.explode())
                director_df = director_explode.group_by("Directors").count()
                top_directors_dict = director_df.filter(pl.col("Directors") != None).top_k(5, by="count").to_dicts()
                top_directors = [i["Directors"] for i in top_directors_dict]
                top_directors_string = f"Your top 5 directors are :green[**{', '.join(top_directors[:-1]) + ' and ' + top_directors[-1]}**]." if len(top_directors) > 1 else "You don't have enough directors logged to generate a top 5." #I know this is long, I'm really tired bro
                st.subheader("Director Demographics")
                st.markdown(f"""You logged :green[**{len(director_df)} directors**]. {top_directors_string}""")
                director_donut = go.Figure(data=[go.Pie(labels=director_gender_sum.to_series(0), values=director_gender_sum.to_series(1), hole=.4, marker=dict(colors=pie_color))])
                st.plotly_chart(director_donut)
            with column_2:
                writer_gender = df.select( 
                                        pl.col("Writer Gender").list.count_matches(2).alias("Male Writers"),
                                        pl.col("Writer Gender").list.count_matches(1).alias("Female Writers"),
                                        pl.col("Writer Gender").list.count_matches(3).alias("Non-Binary Writers"),
                                        pl.col("Writer Gender").list.count_matches(0).alias("Not Specified")
                                        )
                writer_gender_sum = writer_gender.select(
                    pl.col("Male Writers").sum(),
                    pl.col("Female Writers").sum(),
                    pl.col("Non-Binary Writers").sum(),
                    pl.col("Not Specified").sum()
                ).transpose(include_header=True,header_name="categories")

                writers_explode = df.select(pl.col("Writers").list.explode())
                writers_df = writers_explode.group_by("Writers").count()
                top_writers_dict = writers_df.filter(pl.col("Writers") != None).top_k(5, by="count").to_dicts()
                top_writers = [i["Writers"] for i in top_writers_dict]
                top_writers_string = f"Your top 5 writers are :green[**{', '.join(top_writers[:-1]) + ' and ' + top_writers[-1]}**]." if len(top_writers) > 1 else "You don't have enough writers logged to generate a top 5."
                st.subheader("Writer Demographics")
                st.markdown(f"""You logged :green[**{len(writers_df)} writers**]. {top_writers_string}""")
                writer_donut = go.Figure(data=[go.Pie(labels=writer_gender_sum.to_series(0), values=writer_gender_sum.to_series(1), hole=.4, marker=dict(colors=pie_color))])
                st.plotly_chart(writer_donut)

    st.divider()
    
    with st.container():
        st.subheader("Countries + Genres Distribution")
        countries_df = df.select(pl.col("Production Countries").list.explode())
        countries_df_count = countries_df.group_by("Production Countries").count()
        parents = ['Countries Logged']*len(countries_df_count['Production Countries'])
        count_map = go.Figure(go.Treemap(
            labels=countries_df_count["Production Countries"], 
            values=countries_df_count["count"],
            parents=parents,
            textinfo="label+value+percent root",
            marker_colorscale = ['#ebf8fd', '#40bcf4' ]
            )
            )
        count_map.update_traces(marker=dict(cornerradius=5))
        count_map.update_layout(margin = dict(t=50, l=0, r=0, b=0))
        top_country_filter = countries_df_count.top_k(1, by="count")
        top_country = top_country_filter.to_dicts()
        countries_check = ["United States", "United Arab Emirates", "United Kingdom", "Netherlands"]
        count_string_fmt = '' if top_country[0]["Production Countries"] not in countries_check else 'the'
        st.plotly_chart(count_map, use_container_width=True)
        st.markdown(f"""You logged films produced across :green[**{len(countries_df_count)}**] countries! A majority of films you logged were produced in {count_string_fmt} :green[**{top_country[0]["Production Countries"]}**].
                    _Keep in mind: Films can be produced in more than on country._""")

    with st.container():
        genres_df = df.select(pl.col("Genre(s)").list.explode())
        genres_df_count = genres_df.group_by("Genre(s)").count()
        genres_parent = ['Genres']*len(genres_df_count['Genre(s)'])
        genres_map = go.Figure(go.Treemap(
            labels=genres_df_count["Genre(s)"], 
            values=genres_df_count["count"],
            parents=genres_parent,
            textinfo="label+value+percent root",
            marker_colorscale = ['#ebf8fd', '#40bcf4' ]
            )
            )
        genres_map.update_traces(marker=dict(cornerradius=5))
        genres_map.update_layout(margin = dict(t=50, l=0, r=0, b=0))
        top_genre_filter = genres_df_count.top_k(1, by="count")
        top_genre = top_genre_filter.to_dicts()
        st.plotly_chart(genres_map, use_container_width=True)
        st.markdown(f"""You logged films from :green[**{len(genres_df_count)}**] different genres, with your top genre being :green[**{top_genre[0]["Genre(s)"]}**]! """)