from lettercrawler import *
import polars as pl
import pycountry

user = "kcabs"
loop = asyncio.get_event_loop()
df = loop.run_until_complete(crawl_all(user, get_total_pages(user)))
# log_years = final_df.with_columns(pl.col("Log Date").dt.year().alias("log_year"))
# log_years_script = (
#     log_years.lazy()
#     .group_by("log_year")
#     .agg(pl.count("Log Date"))
#     .sort("log_year")
#     )
# log_years_count = log_years_script.collect()
# print(log_years_count)

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
# print(re_count)
# print(release_years.select(['Name', 'Release Year']))

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
def get_country_name(alpha_2):
    try:
        return pycountry.countries.get(alpha_2=alpha_2)
    except AttributeError:
        return None
    
countries_df = df.select(pl.col("Production Countries").list.explode())
countries_df_count = countries_df.group_by("Production Countries").count()

print(countries_df_count)