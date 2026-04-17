import dlt
from pyspark.sql.functions import *

# ---------------------------
# KPI 1: Average Rating per Movie
# ---------------------------
@dlt.table(name="kpi_avg_rating_per_movie")
def kpi_avg_rating_per_movie():
    return dlt.read("ratings_silver") \
        .groupBy("movieId") \
        .agg(avg("rating").alias("avg_rating"))


# ---------------------------
# KPI 2: Total Ratings per Movie
# ---------------------------
@dlt.table(name="kpi_total_ratings_per_movie")
def kpi_total_ratings_per_movie():
    return dlt.read("ratings_silver") \
        .groupBy("movieId") \
        .count() \
        .withColumnRenamed("count", "total_ratings")


# ---------------------------
# KPI 3: Top Rated Movies (avg >= 4)
# ---------------------------
@dlt.table(name="kpi_top_rated_movies")
def kpi_top_rated_movies():
    return dlt.read("ratings_silver") \
        .groupBy("movieId") \
        .agg(avg("rating").alias("avg_rating")) \
        .filter(col("avg_rating") >= 4.0)


# ---------------------------
# KPI 4: Average Rating per Genre
# ---------------------------
@dlt.table(name="kpi_avg_rating_per_genre")
def kpi_avg_rating_per_genre():
    ratings = dlt.read("ratings_silver")
    genres = dlt.read("movies_silver") \
        .withColumn("genre", explode(split("genres", "\\|"))) \
        .select("movieId", "genre")

    return ratings.join(genres, "movieId") \
        .groupBy("genre") \
        .agg(avg("rating").alias("avg_rating"))

@dlt.table(name="kpi_rating_trend_over_time")
def kpi_rating_trend_over_time():
    return dlt.read("ratings_silver") \
        .withColumn("year", year(col("rating_timestamp"))) \
        .groupBy("year") \
        .agg(avg("rating").alias("avg_rating")) \
        .orderBy("year")


# ---------------------------
# KPI 5: Early vs Late Rating Bias
# ---------------------------
@dlt.table(name="kpi_early_vs_late_ratings")
def kpi_early_vs_late_ratings():
    df = dlt.read("ratings_silver")

    return df.withColumn(
        "rating_phase",
        when(year(col("rating_timestamp")) < 2010, "early")
        .otherwise("late")
    ).groupBy("rating_phase") \
     .agg(avg("rating").alias("avg_rating"))


# ---------------------------
# KPI 6: User Loyalty (genre preference)
# ---------------------------
@dlt.table(name="kpi_user_loyalty")
def kpi_user_loyalty():
    ratings = dlt.read("ratings_silver")
    genres = dlt.read("movies_silver") \
        .withColumn("genre", explode(split("genres", "\\|"))) \
        .select("movieId", "genre")

    return ratings.join(genres, "movieId") \
        .groupBy("userId", "genre") \
        .count() \
        .withColumnRenamed("count", "genre_watch_count") \
        .filter(col("genre_watch_count") > 20)


# ---------------------------
# KPI 7: Controversial Movies (high variance)
# ---------------------------
@dlt.table(name="kpi_controversial_movies")
def kpi_controversial_movies():
    return dlt.read("ratings_silver") \
        .groupBy("movieId") \
        .agg(
            avg("rating").alias("avg_rating"),
            stddev("rating").alias("rating_variance")
        ) \
        .filter(col("rating_variance") > 1.5)


# ---------------------------
# KPI 8: Most Popular Genres (by rating count)
# ---------------------------
@dlt.table(name="kpi_popular_genres")
def kpi_popular_genres():
    ratings = dlt.read("ratings_silver")
    genres = dlt.read("movies_silver") \
        .withColumn("genre", explode(split("genres", "\\|"))) \
        .select("movieId", "genre")

    return ratings.join(genres, "movieId") \
        .groupBy("genre") \
        .count() \
        .withColumnRenamed("count", "total_ratings")


# ---------------------------
# KPI 9: Active Users (users who rated many movies)
# ---------------------------
@dlt.table(name="kpi_active_users")
def kpi_active_users():
    return dlt.read("ratings_silver") \
        .groupBy("userId") \
        .count() \
        .withColumnRenamed("count", "total_ratings") \
        .filter(col("total_ratings") > 50)


# ---------------------------
# KPI 10: Rating Distribution (how ratings are spread)
# ---------------------------
@dlt.table(name="kpi_rating_distribution")
def kpi_rating_distribution():
    return dlt.read("ratings_silver") \
        .groupBy("rating") \
        .count() \
        .orderBy("rating")