import streamlit as st
import polars as pl
from lettercrawler import *
import asyncio
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(page_title="LetterBoard", layout='wide')
st.title("Letterboxd Data Analysis")
st.markdown("""Letterboxd is a social media platform for film lovers to rate, discuss, and discover movies.
            This app scrapes data from your Letterboxd diary, and maps movie data to corresponding data in the TMDb API (The more movies you have logged, the longer this will take).
            _No affiliation with Letterboxd itself._
            """)
st.markdown("Made by [Favour O.](https://www.linkedin.com/in/favour-oshio/), inspired by [Tyler Richards' Goodreads App](https://goodreads.streamlit.app/).")


@st.cache_data()
def load_user_data(username):
    try:
        pages = get_total_pages(username)
        loop = asyncio.new_event_loop()
        df = loop.run_until_complete(crawl_all(username, pages))  # noqa: F405
        return df
    except Exception as e:
        st.error(f"Error loading user data: {str(e)}")
        return None

username = st.text_input('Username')
st.markdown("""_If you don't have a Letterboxd try some of my favorite accounts: :green[fumilayo] (Ayo Edebiri), :green[jaredgilman] (Jared Gilman), :green[girlactress] (Rachel Sennott)_ """)
submitted = st.button("Submit")
df = None
year_count = None
release_count = None

if submitted:
    tab1, tab2, tab3 = st.tabs(["Films Logged", "Film Release Year", "Film Runtimes"])
    df = load_user_data(username)

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