import streamlit as st
import polars as pl
from lettercrawler import *
import asyncio
import aiohttp

st.set_page_config(page_title="LetterBoard", layout='wide')
st.title("Letterboxd Data Analysis")
st.markdown("")

df = None

with st.form(key='username'):
    username = st.text_input('Username')
    submitted = st.form_submit_button("Submit")
    if submitted:
        loop = asyncio.new_event_loop()
        df = loop.run_until_complete(crawl_all(username,get_total_pages(username)))  # noqa: F405

if df is not None:
    # Display your DataFrame in Streamlit
    st.write(df)