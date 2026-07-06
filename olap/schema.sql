CREATE DATABASE IF NOT EXISTS imdb;

CREATE TABLE IF NOT EXISTS imdb.title_analytics
(
    tconst          String,
    title_type      LowCardinality(String),
    primary_title   String,
    original_title  String,
    start_year      Nullable(UInt16),
    runtime_minutes Nullable(UInt16),
    genres          Array(String),
    average_rating  Nullable(Float32),
    num_votes       Nullable(UInt64),
    series_title    Nullable(String),

    -- ---- Data-skipping (secondary) indexes ----
    -- Q1: WHERE num_votes >= 100000  -> skip granules outside the min/max range
    INDEX idx_num_votes     num_votes      TYPE minmax               GRANULARITY 4,
    -- Q1/analytics: filter/sort on rating
    INDEX idx_avg_rating    average_rating TYPE minmax               GRANULARITY 4,
    -- Q3: ARRAY JOIN genres + genre filters -> membership test on array
    INDEX idx_genres        genres         TYPE bloom_filter(0.01)   GRANULARITY 4,
    -- optional: substring/title search
    INDEX idx_primary_title primary_title  TYPE tokenbf_v1(4096, 3, 0) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY ifNull(intDiv(start_year, 10), 0)          -- decade partitions (few, balanced)
PRIMARY KEY (title_type, ifNull(start_year, 0))         -- sparse index prefix (small, in RAM)
ORDER BY   (title_type, ifNull(start_year, 0), tconst); -- full sort key (adds tconst locality)
