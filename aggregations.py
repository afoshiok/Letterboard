from lettercrawler import *
import polars as pl
import pycountry

user = "FavourOshio"
loop = asyncio.get_event_loop()
df = loop.run_until_complete(crawl_all(user, get_total_pages(user)))
# log_years = df.with_columns(pl.col("Log Date").dt.year().alias("log_year"))
# log_years_script = (
#     log_years.lazy()
#     .group_by("log_year")
#     .agg(pl.count("Log Date"))
#     .sort("log_year")
#     )
# log_years_count = log_years_script.collect()
# max_log_years_df = log_years_script.filter(pl.col("Log Date") == pl.col("Log Date").max()).collect()
# max_log_years = max_log_years_df.to_dicts()
# print(max_log_years_df)


print(df.schema)

# release_years = df.with_columns(pl.col("Release Date").dt.year().alias("Release Year"))
# release_years_script = (
#     release_years.lazy()
#     .group_by("Release Year")
#     .agg(pl.count())
#     .drop_nulls()
#     .sort("Release Year")
# )
# re_count = release_years_script.collect()

# oldest_film_df = release_years.filter(pl.col("Release Year") == pl.col("Release Year").min()).bottom_k(1, by=["Log Date"])
# oldest_film = oldest_film_df.to_dicts()
# print(oldest_film)

# max_re_count = release_years_script.filter(pl.col("count") == pl.col("count").max()).collect()
# print(max_re_count)

# director_gender = df.select(
#                             pl.col("Director Gender").list.count_matches(2).alias("Male Directors"),
#                             pl.col("Director Gender").list.count_matches(1).alias("Female Directors"),
#                             pl.col("Director Gender").list.count_matches(3).alias("Non-Binary Directors"),
#                             pl.col("Director Gender").list.count_matches(0).alias("Director's Gender Not Specified")
#                             )
# director_gender_sum = director_gender.select(
#     pl.col("Male Directors").sum(),
#     pl.col("Female Directors").sum(),
#     pl.col("Non-Binary Directors").sum(),
#     pl.col("Director's Gender Not Specified").sum()
# ).transpose(include_header=True,header_name="categories")

# writer_gender = df.select( 
#                         pl.col("Writer Gender").list.count_matches(2).alias("Male Writers"),
#                         pl.col("Writer Gender").list.count_matches(1).alias("Female Writers"),
#                         pl.col("Writer Gender").list.count_matches(3).alias("Non-Binary Writers"),
#                         pl.col("Writer Gender").list.count_matches(0).alias("Writer's Gender Not Specified")
#                         )
# writer_gender_sum = writer_gender.select(
#     pl.col("Male Writers").sum(),
#     pl.col("Female Writers").sum(),
#     pl.col("Non-Binary Writers").sum(),
#     pl.col("Writer's Gender Not Specified").sum()
# ).transpose(include_header=True,header_name="categories")
# with pl.Config(set_tbl_width_chars=175):
#     print(director_gender_sum.to_series(0))
#     print(writer_gender_sum)

    
# countries_df = df.select(pl.col("Production Countries").list.explode())
# countries_df_count = countries_df.group_by("Production Countries").count()

# print(countries_df_count)

# def categorize_runtime(runtime):
#     if runtime <= 60:
#         return 'Short'
#     elif 60 <= runtime <= 90:
#         return 'Medium'
#     elif 90 < runtime:
#         return 'Long'
# film_runtimes = df.with_columns(pl.col("Runtime (Minutes)").map_elements(categorize_runtime, pl.Utf8).alias("Runtime Category")).drop_nulls("Name")
# category_counts = film_runtimes.group_by('Runtime Category').agg(pl.col('Runtime Category').count().alias('Count'))
# shortest_film_df = film_runtimes.filter(pl.col("Runtime (Minutes)") != 0).bottom_k(1, by="Runtime (Minutes)")
# shortest_film = shortest_film_df.select(pl.col("Name"), pl.col("Runtime (Minutes)"))
# print(shortest_film_df)

# director_explode = df.select(pl.col("Directors").list.explode())
# director_df = director_explode.group_by("Directors").count()
# top_directors = director_df.top_k(5, by="count")
# print(top_directors)