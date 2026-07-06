import csv


SPARK_FILE = (
    "benchmark/results/"
    "spark_results.csv"
)

CLICKHOUSE_FILE = (
    "benchmark/results/"
    "clickhouse_results.csv"
)

OUTPUT_FILE = (
    "benchmark/results/"
    "benchmark_comparison.csv"
)


def read_results(
    file_path
):

    results = {}


    with open(
        file_path,
        "r"
    ) as csv_file:

        reader = csv.DictReader(
            csv_file
        )


        for row in reader:

            results[
                row["query"]
            ] = float(
                row["median_seconds"]
            )


    return results


spark_results = read_results(
    SPARK_FILE
)


clickhouse_results = read_results(
    CLICKHOUSE_FILE
)


comparison = []


for query_name in spark_results:

    spark_time = (
        spark_results[query_name]
    )


    clickhouse_time = (
        clickhouse_results[query_name]
    )


    speedup = (
        spark_time
        /
        clickhouse_time
    )


    comparison.append({

        "query": query_name,

        "spark_seconds": round(
            spark_time,
            4
        ),

        "clickhouse_seconds": round(
            clickhouse_time,
            4
        ),

        "speedup": round(
            speedup,
            2
        )
    })


# ------------------------------------------------------------
# Print table
# ------------------------------------------------------------

print("\n")

print(
    f"{'Query':30}"
    f"{'Spark':>12}"
    f"{'ClickHouse':>15}"
    f"{'Speedup':>12}"
)


print("-" * 69)


for row in comparison:

    print(

        f"{row['query']:30}"

        f"{row['spark_seconds']:>12.4f}"

        f"{row['clickhouse_seconds']:>15.4f}"

        f"{row['speedup']:>11.2f}x"
    )


# ------------------------------------------------------------
# Write comparison CSV
# ------------------------------------------------------------

with open(
    OUTPUT_FILE,
    "w",
    newline=""
) as csv_file:

    writer = csv.DictWriter(

        csv_file,

        fieldnames=[
            "query",
            "spark_seconds",
            "clickhouse_seconds",
            "speedup"
        ]
    )


    writer.writeheader()

    writer.writerows(
        comparison
    )


print(
    f"\nComparison written to: "
    f"{OUTPUT_FILE}"
)
