import streamlit as st
import polars as pl
from lettercrawler import *
import asyncio
import plotly.express as px
import plotly.graph_objects as go
from streamlit_extras.add_vertical_space import add_vertical_space

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
    df = load_user_data(username)

if df is not None:

    

    with st.container():
        with tab1:
            st.text("Trying to figure out why this isn't working.")
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
            re_years_graph = px.bar(re_count, x="Release Year", y="count", title="Films Logged by Release Year")
            re_years_graph.update_xaxes(type='category')
            st.plotly_chart(re_years_graph, use_container_width=True)

    st.divider()
                
    
    with st.container():
        pie_color = ['#ff8000', '#00e054', '#ffffff', '#40bcf4' ]
        st.header("Writer/Director Demographics")
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
            st.subheader("Director Demographics")
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
            st.subheader("Writer Demographics")
            writer_donut = go.Figure(data=[go.Pie(labels=writer_gender_sum.to_series(0), values=writer_gender_sum.to_series(1), hole=.4, marker=dict(colors=pie_color))])
            st.plotly_chart(writer_donut)

        
        
        
        
            
        
            

    

