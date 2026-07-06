from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    split
)
import os
from dotenv import load_dotenv

load_dotenv()

RAW_PATH = os.environ["RAW_PATH"]
OUTPUT_PATH=os.environ["LAKE_PATH"]

spark = (
    SparkSession.builder
    .appName("IMDbLakehouseETL")
    .getOrCreate()
)



basics = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .csv(f"{RAW_PATH}/title.basics.tsv")
)


ratings = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .csv(f"{RAW_PATH}/title.ratings.tsv")
)


basics_clean = (
    basics
    .withColumn(
        "startYear",
        when(col("startYear") == "\\N", None)
        .otherwise(col("startYear").cast("int"))
    )
    .withColumn(
        "runtimeMinutes",
        when(col("runtimeMinutes") == "\\N", None)
        .otherwise(col("runtimeMinutes").cast("int"))
    )
    .withColumn(
        "genres",
        when(col("genres") == "\\N", None)
        .otherwise(split(col("genres"), ","))
    )
)

ratings_clean = (
    ratings
    .select(
        col("tconst"),
        col("averageRating")
            .cast("double")
            .alias("average_rating"),
        col("numVotes")
            .cast("long")
            .alias("num_votes")
    )
)

parent_titles = (
    basics_clean
    .select(
        col("tconst").alias("parent_lookup_tconst"),
        col("primaryTitle").alias("series_title")
    )
)

final_df = (
    basics_clean
    .join(ratings_clean, "tconst", "left")
    .join(
        parent_titles,
        col("tconst") ==
        col("parent_lookup_tconst"),
        "left"
    )
    .select(
        col("tconst"),

        col("titleType")
            .alias("title_type"),

        col("primaryTitle")
            .alias("primary_title"),

        col("originalTitle")
            .alias("original_title"),

        col("startYear")
            .alias("start_year"),

        col("runtimeMinutes")
            .alias("runtime_minutes"),

        col("genres"),

        col("average_rating"),
        col("num_votes"),

        col("series_title")
    )
)

final_df.printSchema()

print("Final rows:", final_df.count())

print(
    "Distinct titles:",
    final_df.select("tconst").distinct().count()
)

(
    final_df
    .repartition(
        "title_type",
        "start_year"
    )
    .write
    .mode("overwrite")
    .partitionBy(
        "title_type",
        "start_year"
    )
    .option("compression", "snappy")
    .parquet(OUTPUT_PATH)
)

