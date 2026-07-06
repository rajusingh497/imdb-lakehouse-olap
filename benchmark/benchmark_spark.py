import csv
import statistics
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    explode,
    sum as spark_sum
)


PARQUET_PATH = "/data/lake/imdb_curated"

RESULT_PATH = (
    "/opt/spark/benchmark/results/"
    "spark_results.csv"
)

WARMUP_RUNS = 1
MEASURED_RUNS = 5


spark = (
    SparkSession.builder
    .appName("IMDbSparkBenchmark")
    .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("SPARK BENCHMARK")
print("=" * 70)


# ------------------------------------------------------------
# Read curated Parquet
# ------------------------------------------------------------

titles_df = (
    spark.read
    .parquet(PARQUET_PATH)
)


print("\nSchema:")

titles_df.printSchema()


# ------------------------------------------------------------
# Query 1
#
# Top-rated popular movies
#
# Workload:
# Filter + Sort + Limit
# ------------------------------------------------------------

def query_1():

    return (
        titles_df
        .filter(
            (col("title_type") == "movie") &
            (col("num_votes") >= 100000)
        )
        .select(
            "primary_title",
            "start_year",
            "average_rating",
            "num_votes"
        )
        .orderBy(
            col("average_rating").desc(),
            col("num_votes").desc()
        )
        .limit(20)
    )


# ------------------------------------------------------------
# Query 2
#
# Rating trend by year
#
# Workload:
# Filter + Group By + Aggregation + Sort
# ------------------------------------------------------------

def query_2():

    return (
        titles_df
        .filter(
            (col("title_type") == "movie") &
            col("start_year").isNotNull() &
            col("average_rating").isNotNull()
        )
        .groupBy("start_year")
        .agg(
            avg("average_rating")
            .alias("avg_rating"),

            count("*")
            .alias("movie_count"),

            spark_sum("num_votes")
            .alias("total_votes")
        )
        .orderBy("start_year")
    )


# ------------------------------------------------------------
# Query 3
#
# Genre analytics
#
# Workload:
# Array explode + Group By + Aggregation
# ------------------------------------------------------------

def query_3():

    genre_df = (
        titles_df
        .filter(
            col("average_rating").isNotNull()
        )
        .withColumn(
            "genre",
            explode(col("genres"))
        )
    )


    return (
        genre_df
        .groupBy("genre")
        .agg(
            count("*")
            .alias("title_count"),

            avg("average_rating")
            .alias("avg_rating")
        )
        .orderBy(
            col("title_count").desc()
        )
    )





QUERIES = {

    "Q1_TOP_MOVIES": query_1,

    "Q2_YEARLY_TREND": query_2,

    "Q3_GENRE_ANALYSIS": query_3
}


# ------------------------------------------------------------
# Benchmark function
# ------------------------------------------------------------

def benchmark_query(
    query_name,
    query_function
):

    print("\n" + "=" * 70)

    print(f"Running: {query_name}")

    print("=" * 70)


    # --------------------------------------------------------
    # Warm-up
    # --------------------------------------------------------

    for warmup_run in range(
        1,
        WARMUP_RUNS + 1
    ):

        print(
            f"Warm-up run {warmup_run}"
        )


        dataframe = query_function()


        #
        # collect() is necessary because Spark is lazy.
        #
        # Without an action, Spark would only construct
        # the execution plan and would not actually execute
        # the query.
        #

        dataframe.collect()


    # --------------------------------------------------------
    # Measured runs
    # --------------------------------------------------------

    execution_times = []


    for run_number in range(
        1,
        MEASURED_RUNS + 1
    ):

        start_time = time.perf_counter()


        dataframe = query_function()


        result = dataframe.collect()


        elapsed_time = (
            time.perf_counter()
            -
            start_time
        )


        execution_times.append(
            elapsed_time
        )


        print(
            f"Run {run_number}: "
            f"{elapsed_time:.4f} seconds "
            f"| Result rows: {len(result)}"
        )


    median_time = statistics.median(
        execution_times
    )


    mean_time = statistics.mean(
        execution_times
    )


    min_time = min(
        execution_times
    )


    max_time = max(
        execution_times
    )


    print(
        f"\nMedian: {median_time:.4f}s"
    )

    print(
        f"Mean  : {mean_time:.4f}s"
    )


    return {

        "engine": "Spark",

        "query": query_name,

        "median_seconds": round(
            median_time,
            4
        ),

        "mean_seconds": round(
            mean_time,
            4
        ),

        "min_seconds": round(
            min_time,
            4
        ),

        "max_seconds": round(
            max_time,
            4
        )
    }


# ------------------------------------------------------------
# Execute benchmark
# ------------------------------------------------------------

benchmark_results = []


for query_name, query_function in QUERIES.items():

    benchmark_result = benchmark_query(
        query_name,
        query_function
    )


    benchmark_results.append(
        benchmark_result
    )


# ------------------------------------------------------------
# Write CSV
# ------------------------------------------------------------

with open(
    RESULT_PATH,
    "w",
    newline=""
) as csv_file:

    writer = csv.DictWriter(

        csv_file,

        fieldnames=[
            "engine",
            "query",
            "median_seconds",
            "mean_seconds",
            "min_seconds",
            "max_seconds"
        ]
    )


    writer.writeheader()


    writer.writerows(
        benchmark_results
    )


print("\n" + "=" * 70)

print(
    f"Spark benchmark written to:"
    f"\n{RESULT_PATH}"
)

print("=" * 70)


spark.stop()
