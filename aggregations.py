from lettercrawler import *
import polars as pl

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

director_gender = df.select(
                            pl.col("Director Gender").list.count_matches(2).alias("Male Directors"),
                            pl.col("Director Gender").list.count_matches(1).alias("Female Directors"),
                            pl.col("Director Gender").list.count_matches(3).alias("Non-Binary Directors"),
                            pl.col("Director Gender").list.count_matches(0).alias("Director's Gender Not Specified")
                            )
director_gender_sum = director_gender.select(
    pl.col("Male Directors").sum(),
    pl.col("Female Directors").sum(),
    pl.col("Non-Binary Directors").sum(),
    pl.col("Director's Gender Not Specified").sum()
)

writer_gender = df.select( 
                        pl.col("Writer Gender").list.count_matches(2).alias("Male Writers"),
                        pl.col("Writer Gender").list.count_matches(1).alias("Female Writers"),
                        pl.col("Writer Gender").list.count_matches(3).alias("Non-Binary Writers"),
                        pl.col("Writer Gender").list.count_matches(0).alias("Writer's Gender Not Specified")
                        )
writer_gender_sum = writer_gender.select(
    pl.col("Male Writers").sum(),
    pl.col("Female Writers").sum(),
    pl.col("Non-Binary Writers").sum(),
    pl.col("Writer's Gender Not Specified").sum()
)
with pl.Config(set_tbl_width_chars=175):
    print(director_gender_sum)
    print(writer_gender_sum)