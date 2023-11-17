import streamlit as st
import polars as pl
from lettercrawler import *
import asyncio
import plotly.express as px

st.set_page_config(page_title="LetterBoard", layout='wide')
st.title("Letterboxd Data Analysis")
st.markdown("""Letterboxd is a social media platform for film lovers to rate, discuss, and discover movies.
            
            """)


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
submitted = st.button("Submit")
df = None
year_count = None
release_count = None

if submitted:
    tab1, tab2 = st.tabs(["Films Logged", "Film Release Year"])
    row1, row2 = st.columns(2)
    df = load_user_data(username)

if df is not None:
    with row1:
        
        with tab1:
            st.markdown(f"""### You've logged a total of {len(df)}* films on Letterboxd!""")
            log_years = df.with_columns(pl.col("Log Date").dt.year().cast(pl.Utf8).to_physical().alias("Log_Year"))
            log_years_script = (
                log_years.lazy()
                .group_by("Log_Year")
                .agg(pl.count("Log Date").alias("Films Logged"))
                .sort("Log_Year")
            )
            log_years_count = log_years_script.collect()
            log_years_graph = px.bar(log_years_count, x="Log_Year", y="Films Logged", title="Films Logged by Year")
            log_years_graph.update_xaxes(type='category')
            st.plotly_chart(log_years_graph, use_container_width=True)
        
        with tab2:
            release_years = df.with_columns(pl.col("Release Date").dt.year().cast(pl.Utf8).to_physical().alias("Release Year"))
            release_years_script = (
            release_years.lazy()
                .group_by("Release Year")
                .agg(pl.count())
                .drop_nulls()
                .sort("Release Year")
            )
            re_count = release_years_script.collect()
            re_years_graph = px.bar(re_count, x="Release Year", y="count")
            re_years_graph.update_xaxes(type='category')
            st.plotly_chart(re_years_graph, use_container_width=True)
    

