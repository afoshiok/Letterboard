from lettercrawler import *
import polars as pl

user = "FavourOshio"
loop = asyncio.get_event_loop()
final_df = loop.run_until_complete(crawl_all(user, get_total_pages(user)))
log_years = final_df.with_columns(pl.col("Log Date").dt.year().alias("log_year"))
log_years_script = (
    log_years.lazy()
    .group_by("log_year")
    .agg(pl.count("Log Date"))
    .sort("log_year")
    )
log_years_count = log_years_script.collect()
print(log_years_count)

# print(final_df.schema)
