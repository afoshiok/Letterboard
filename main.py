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

if submitted:
    tab1, tab2, tab3 = st.tabs(["Films Logged", "Dog", "Owl"])
    df = load_user_data(username)

if df is not None:
    # Display your DataFrame in Streamlit
    with tab1:
        st.write(df)

