import dlt
from pyspark.sql.functions import col, when

# ---------------------------
# 1. BRONZE LAYER (RAW INGESTION)
# ---------------------------

@dlt.table(
    name="movies_bronze",
    comment="Raw movies data"
)
def movies_bronze():
    return spark.read.option("header", True).csv("/Volumes/workspace/default/moviesdata/movies.csv")


@dlt.table(
    name="ratings_bronze",
    comment="Raw ratings data"
)
def ratings_bronze():
    return spark.read.option("header", True).csv("/Volumes/workspace/default/moviesdata/ratings.csv")


@dlt.table(
    name="tags_bronze",
    comment="Raw tags data"
)
def tags_bronze():
    return spark.read.option("header", True).csv("/Volumes/workspace/default/moviesdata/tags.csv")


@dlt.table(
    name="links_bronze",
    comment="Raw links data"
)
def links_bronze():
    return spark.read.option("header", True).csv("/Volumes/workspace/default/moviesdata/links.csv")


  