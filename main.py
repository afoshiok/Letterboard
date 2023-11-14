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
        df = crawl_all(username)

if df is not None:
    # Display your DataFrame in Streamlit
    st.write(df)