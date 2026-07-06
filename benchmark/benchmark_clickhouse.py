import csv
import statistics
import time

import clickhouse_connect


CLICKHOUSE_HOST = "localhost"

CLICKHOUSE_PORT = 8123

RESULT_PATH = (
    "benchmark/results/"
    "clickhouse_results.csv"
)

WARMUP_RUNS = 1

MEASURED_RUNS = 5


# ------------------------------------------------------------
# Connect to ClickHouse
# ------------------------------------------------------------

client = clickhouse_connect.get_client(

    host=CLICKHOUSE_HOST,

    port=CLICKHOUSE_PORT,

    username="default",

    password="my_secret_password",
)


# ------------------------------------------------------------
# Queries
# ------------------------------------------------------------

QUERIES = {


    "Q1_TOP_MOVIES":

    """
    SELECT
        primary_title,
        start_year,
        average_rating,
        num_votes

    FROM imdb.title_analytics

    WHERE title_type = 'movie'

      AND num_votes >= 100000

    ORDER BY
        average_rating DESC,
        num_votes DESC

    LIMIT 20
    """,


    "Q2_YEARLY_TREND":

    """
    SELECT
        start_year,

        avg(average_rating)
            AS avg_rating,

        count()
            AS movie_count,

        sum(num_votes)
            AS total_votes

    FROM imdb.title_analytics

    WHERE title_type = 'movie'

      AND start_year IS NOT NULL

      AND average_rating IS NOT NULL

    GROUP BY start_year

    ORDER BY start_year
    """,


    "Q3_GENRE_ANALYSIS":

    """
    SELECT
        genres as genre,

        count()
            AS title_count,

        avg(average_rating)
            AS avg_rating

    FROM imdb.title_analytics

    WHERE average_rating IS NOT NULL

    GROUP BY genre

    ORDER BY title_count DESC
    """

}


# ------------------------------------------------------------
# Benchmark function
# ------------------------------------------------------------

def benchmark_query(
    query_name,
    query
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


        client.query(query)


    # --------------------------------------------------------
    # Measured runs
    # --------------------------------------------------------

    execution_times = []


    for run_number in range(
        1,
        MEASURED_RUNS + 1
    ):

        start_time = time.perf_counter()


        result = client.query(
            query
        )


        elapsed_time = (
            time.perf_counter()
            -
            start_time
        )


        execution_times.append(
            elapsed_time
        )


        result_rows = len(
            result.result_rows
        )


        print(
            f"Run {run_number}: "
            f"{elapsed_time:.4f} seconds "
            f"| Result rows: {result_rows}"
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

        "engine": "ClickHouse",

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
# Run benchmark
# ------------------------------------------------------------

benchmark_results = []


for query_name, query in QUERIES.items():

    benchmark_result = benchmark_query(
        query_name,
        query
    )


    benchmark_results.append(
        benchmark_result
    )


# ------------------------------------------------------------
# Write results
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
    f"ClickHouse benchmark written to:"
    f"\n{RESULT_PATH}"
)

print("=" * 70)
