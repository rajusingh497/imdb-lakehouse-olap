from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import os
from dotenv import load_dotenv

load_dotenv()

RAW_PATH = os.environ["RAW_PATH"]
OUTPUT_PATH=os.environ["LAKE_PATH"]


# --- Config (adjust to your setup) ---
CLICKHOUSE_HOST = os.environ["CLICKHOUSE_HOST"]
CLICKHOUSE_HTTP_PORT = os.environ["CLICKHOUSE_HTTP_PORT"]
CLICKHOUSE_USER = os.environ["CLICKHOUSE_USER"]
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
CLICKHOUSE_DB = os.environ["CLICKHOUSE_DB"]
TARGET_TABLE = os.environ["CLICKHOUSE_TABLE"]
PARQUET_PATH = os.environ["LAKE_PATH"]


def main():
    spark = (
        SparkSession.builder
        .appName("LoadParquetToClickHouse")
        .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
        .config("spark.sql.catalog.clickhouse.host", CLICKHOUSE_HOST)
        .config("spark.sql.catalog.clickhouse.protocol", "http")
        .config("spark.sql.catalog.clickhouse.http_port", CLICKHOUSE_HTTP_PORT)
        .config("spark.sql.catalog.clickhouse.user", CLICKHOUSE_USER)
        .config("spark.sql.catalog.clickhouse.password", CLICKHOUSE_PASSWORD)
        .config("spark.sql.catalog.clickhouse.database", CLICKHOUSE_DB)
        .config("spark.sql.catalog.clickhouse.option.http_connection_provider", "HTTP_URL_CONNECTION")
        .config("spark.sql.catalog.clickhouse.option.decompress", "false")
        .config("spark.sql.catalog.clickhouse.option.compress", "false")
        .config("spark.clickhouse.read.compression.codec", "none")
        .config("spark.clickhouse.write.compression.codec", "none")
        .getOrCreate()
    )

    # 1. Read parquet
    df = spark.read.parquet(PARQUET_PATH)
    print(f"Read {df.count()} rows from {PARQUET_PATH}")
    df.printSchema()
    
    df = df.select(

        col("tconst")
            .cast("string"),

        col("title_type")
            .cast("string"),

        col("primary_title")
            .cast("string"),

        col("original_title")
            .cast("string"),

        col("start_year")
            .cast("int"),

        col("runtime_minutes")
            .cast("int"),

        col("genres"),

        col("average_rating")
            .cast("float"),

        col("num_votes")
            .cast("long"),

        col("series_title")
            .cast("string")
    )


    # Assuming the target table already exists
    df.writeTo(f"clickhouse.{CLICKHOUSE_DB}.{TARGET_TABLE}").overwrite(lit(True))

    print("Load complete.")
    spark.stop()


if __name__ == "__main__":
    main()
