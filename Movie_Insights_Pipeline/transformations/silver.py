import dlt
from pyspark.sql.functions import *

# ---------------------------
# SILVER LAYER (DATA CLEANING)
# ---------------------------

@dlt.table(name="tags_silver")
def tags_silver():
    return dlt.read("tags_bronze") \
        .selectExpr(
            "cast(userId as int) as userId",
            "cast(movieId as int) as movieId",
            "tag",
            "cast(timestamp as long) as timestamp"
        ) \
        .filter(
            col("tag").isNotNull() &
            col("timestamp").isNotNull()
        ) \
        .dropDuplicates(["userId","movieId","tag","timestamp"]) \
        .withColumn("tag_timestamp", from_unixtime(col("timestamp"))) \
        .drop("timestamp")


@dlt.table(name="ratings_silver")
def ratings_silver():
    return dlt.read("ratings_bronze") \
        .selectExpr(
            "cast(userId as int) as userId",
            "cast(movieId as int) as movieId",
            "try_cast(rating as double) as rating",
            "cast(timestamp as long) as timestamp"
        ) \
        .dropDuplicates(["userId","movieId","timestamp"]) \
        .dropna(subset=["rating"]) \
        .filter(col("rating").between(0,5)) \
        .withColumn("rating_timestamp", from_unixtime(col("timestamp"))) \
        .drop("timestamp")


@dlt.table(name="movies_silver")
def movies_silver():
    return dlt.read("movies_bronze") \
        .selectExpr(
            "cast(movieId as int) as movieId",
            "title",
            "genres"
        ) \
        .withColumn("title", trim(col("title"))) \
        .withColumn("genres", trim(col("genres"))) \
        .filter(
            col("title").isNotNull() &
            col("genres").isNotNull()
        ) \
        .filter(
            (col("title") != "null") &
            (col("genres") != "null")
        ) \
        .dropDuplicates()


@dlt.table(name="links_silver")
def links_silver():
    return dlt.read("links_bronze") \
        .withColumn(
            "imdbId",
            when(trim(col("imdbId")) == "unknown", None)
            .otherwise(col("imdbId"))
        ) \
        .selectExpr(
            "cast(movieId as int) as movieId",
            "try_cast(imdbId as int) as imdbId",
            "try_cast(tmdbId as int) as tmdbId"
        ) \
        .filter(
            col("movieId").isNotNull() &
            col("imdbId").isNotNull() &
            col("tmdbId").isNotNull()
        ) \
        .dropDuplicates(["movieId"])

@dlt.table(name="movie_genre")
def movie_genre():
    return dlt.read("movies_silver") \
        .withColumn("genre", explode(split("genres", "\\|"))) \
        .select("movieId", "genre")


@dlt.table(name="dim_movies")
def dim_movies():
    return dlt.read("movies_silver").select("movieId", "title")


@dlt.table(name="dim_genre")
def dim_genre():
    return dlt.read("movie_genre").select("genre").distinct()


@dlt.table(name="fact_ratings")
def fact_ratings():
    return dlt.read("ratings_silver")