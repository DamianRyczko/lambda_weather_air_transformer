import logging

import duckdb

from lambda_weather_air_transformer.config import (
    GIOS_BRONZE,
    GIOS_BRONZE_ARCHIVE,
    GIOS_INDEX_SILVER_ALL,
    GIOS_INDEX_SILVER_LIVE,
    GIOS_SILVER_ALL,
    GIOS_SILVER_ARCHIVE,
    GIOS_SILVER_LIVE,
)

SELECTED_STATION = "DsWrocWybCon"

logger = logging.getLogger(__name__)

def transform_gios_archive(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming GIOS archive")
    gios = s3_con.sql(f"""
        WITH vertical AS (
            SELECT
                data - INTERVAL 1 HOUR AS measured_at,
                kod_stacji             AS station_code,
                wskaznik               AS indicator_code,
                wartosc                AS value
            FROM read_csv(
                '{GIOS_BRONZE_ARCHIVE}',
                hive_partitioning = true
            )
            WHERE station_code LIKE '{SELECTED_STATION}'
        )
        PIVOT vertical
        ON indicator_code
        USING first(value)
        GROUP BY measured_at, station_code
        ORDER BY measured_at
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = gios.count("*").fetchall()
        logger.debug("Fetched %s unique gios archive rows", row[0][0])
    return gios


def transform_gios_live(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming GIOS live")
    gios = s3_con.sql(f"""
        WITH archive_end AS (
            SELECT max(measured_at) AS cutoff
            FROM read_parquet('{GIOS_SILVER_ARCHIVE}')
        ),
        vertical AS (
            SELECT
                timezone('UTC', timezone('Europe/Warsaw', r.date)) AS measured_at,
                s.station_code,
                sen.indicator_code,
                r.value
            FROM read_json(
                '{GIOS_BRONZE}',
                hive_partitioning = true
            ) AS h,
                UNNEST(h.stations)   AS t1(s),
                UNNEST(s.sensors)    AS t2(sen),
                UNNEST(sen.readings) AS t3(r)
            WHERE s.station_code LIKE '{SELECTED_STATION}'
        ),
        live_only AS (
            SELECT *
            FROM vertical
            WHERE measured_at > (SELECT cutoff FROM archive_end)
        )
        PIVOT live_only
        ON indicator_code
        USING first(value)
        GROUP BY measured_at, station_code
        ORDER BY measured_at
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = gios.count("*").fetchall()
        logger.debug("Fetched %s unique gios live rows", row[0][0])
    return gios


def transform_gios_index_live(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming GIOS air index live")
    gios_index = s3_con.sql(f"""
        WITH stations AS (
            SELECT
                h.fetched_at,
                s.station_code,
                s.air_indexes AS idx
            FROM read_json(
                '{GIOS_BRONZE}',
                hive_partitioning = true
            ) AS h,
                UNNEST(h.stations) AS t1(s)
        ),
        long AS (
            SELECT fetched_at, station_code, 'PM10'  AS pollutant, idx.pm_10_index_reading AS value, idx.pm_10_index_reading_date AS source_date FROM stations
            UNION ALL
            SELECT fetched_at, station_code, 'PM2.5' AS pollutant, idx.pm_25_index_reading AS value, idx.pm_25_index_reading_date AS source_date FROM stations
            UNION ALL
            SELECT fetched_at, station_code, 'NO2'   AS pollutant, idx.NO2_index_reading   AS value, idx.NO2_index_reading_date   AS source_date FROM stations
            UNION ALL
            SELECT fetched_at, station_code, 'O3'    AS pollutant, idx.O3_index_reading    AS value, idx.O3_index_reading_date    AS source_date FROM stations
            UNION ALL
            SELECT fetched_at, station_code, 'SO2'   AS pollutant, idx.SO2_index_reading   AS value, idx.SO2_index_reading_date   AS source_date FROM stations
        ),
        hourly AS (
            SELECT
                timezone('UTC', timezone('Europe/Warsaw', source_date::TIMESTAMP)) AS measured_at,
                concat('gios_index_', replace(station_code, 'DsWroc', ''), '_', pollutant) AS index_name,
                value
            FROM long
            WHERE source_date IS NOT NULL
            QUALIFY row_number() OVER (PARTITION BY station_code, pollutant, source_date ORDER BY fetched_at DESC) = 1
        )
        PIVOT hourly
        ON index_name
        USING first(value)
        GROUP BY measured_at
        ORDER BY measured_at
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = gios_index.count("*").fetchall()
        logger.debug("Fetched %s unique gios index rows", row[0][0])
    return gios_index


def save_gios_archive_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    gios = transform_gios_archive(s3_con=s3_con)
    gios.write_parquet(GIOS_SILVER_ARCHIVE, compression="zstd")
    logger.info("Saved GIOS archive partition to %s", GIOS_SILVER_ARCHIVE)


def save_gios_live_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    gios = transform_gios_live(s3_con=s3_con)
    gios.write_parquet(GIOS_SILVER_LIVE, compression="zstd")
    logger.info("Saved GIOS live partition to %s", GIOS_SILVER_LIVE)


def save_gios_index_live_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    gios_index = transform_gios_index_live(s3_con=s3_con)
    gios_index.write_parquet(GIOS_INDEX_SILVER_LIVE, compression="zstd")
    logger.info("Saved GIOS air index live partition to %s", GIOS_INDEX_SILVER_LIVE)


def read_gios_silver(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    return s3_con.sql(f"""
        SELECT *
        FROM read_parquet(
            '{GIOS_SILVER_ALL}',
            hive_partitioning = true,
            union_by_name     = true
        )
    """)


def read_gios_index_silver(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    return s3_con.sql(f"""
        SELECT *
        FROM read_parquet(
            '{GIOS_INDEX_SILVER_ALL}',
            hive_partitioning = true,
            union_by_name     = true
        )
    """)


def read_gios_recent(s3_con: duckdb.DuckDBPyConnection, hours: int = 48) -> duckdb.DuckDBPyRelation:
    return s3_con.sql(f"""
        SELECT *
        FROM read_parquet(
            '{GIOS_SILVER_ALL}',
            hive_partitioning = true,
            union_by_name     = true
        )
        WHERE source = 'live'
          AND measured_at >= timezone('UTC', now()) - to_hours($hours)
    """, params={"hours": hours})
