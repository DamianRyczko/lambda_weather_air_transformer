import logging

import duckdb

from lambda_weather_air_transformer.config import (
    OPENMETEO_AIR_SILVER_ALL,
    OPENMETEO_AIR_SILVER_ARCHIVE,
    OPENMETEO_AIR_SILVER_LIVE,
    OPENMETEO_BRONZE,
    OPENMETEO_BRONZE_ARCHIVE,
    OPENMETEO_CURRENT_SILVER_ALL,
    OPENMETEO_CURRENT_SILVER_LIVE,
    OPENMETEO_WEATHER_SILVER_ALL,
    OPENMETEO_WEATHER_SILVER_ARCHIVE,
    OPENMETEO_WEATHER_SILVER_LIVE,
)

logger = logging.getLogger(__name__)


def transform_openmeteo_weather_archive(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming OpenMeteo weather archive")
    open_meteo_weather = s3_con.sql(f"""
        SELECT DISTINCT
            time                      AS measured_at,
            temperature_2m            AS temperature,
            relative_humidity_2m      AS relative_humidity,
            pressure_msl              AS atmospheric_pressure,
            precipitation,
            wind_speed_10m            AS wind_speed,
            wind_direction_10m        AS wind_direction,
            wind_gusts_10m            AS wind_gusts,
            soil_temperature_0_to_7cm AS soil_temperature,
            cloud_cover,
            boundary_layer_height
        FROM read_csv(
            '{OPENMETEO_BRONZE_ARCHIVE}',
            hive_partitioning = true,
            union_by_name     = true  -- a month the backfill skipped keeps an older csv with fewer columns
        )
        ORDER BY measured_at
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = open_meteo_weather.count("*").fetchall()
        logger.debug("Fetched %s unique OpenMeteo weather archive rows", row[0][0])
    return open_meteo_weather


def transform_openmeteo_weather_live(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming OpenMeteo weather live")
    open_meteo_weather = s3_con.sql(f"""
        WITH archive_end AS (
            SELECT max(measured_at) AS cutoff
            FROM read_parquet('{OPENMETEO_WEATHER_SILVER_ARCHIVE}')
        ),
        live AS (
            SELECT
                fetched_at,
                UNNEST(weather.hourly_forecast.time)                 AS time,
                UNNEST(weather.hourly_forecast.temperature)          AS temperature,
                UNNEST(weather.hourly_forecast.relative_humidity)    AS relative_humidity,
                UNNEST(weather.hourly_forecast.atmospheric_pressure) AS atmospheric_pressure,
                UNNEST(weather.hourly_forecast.precipitation)        AS precipitation,
                UNNEST(weather.hourly_forecast.wind_speed)           AS wind_speed,
                UNNEST(weather.hourly_forecast.wind_direction)       AS wind_direction,
                UNNEST(weather.hourly_forecast.wind_gusts)           AS wind_gusts,
                UNNEST(weather.hourly_forecast.soil_temperature)     AS soil_temperature,
                UNNEST(weather.hourly_forecast.cloud_cover)          AS cloud_cover,
                UNNEST(weather.hourly_forecast.boundary_layer_height) AS boundary_layer_height
            FROM read_json(
                '{OPENMETEO_BRONZE}',
                hive_partitioning = true,
                columns = {{  -- fixed schema: older snapshots have no boundary_layer_height (-> NULL); detecting it over every file runs out of memory
                    fetched_at: 'TIMESTAMPTZ',
                    weather: 'STRUCT(hourly_forecast STRUCT(
                        "time" TIMESTAMPTZ[], temperature DOUBLE[], relative_humidity DOUBLE[], atmospheric_pressure DOUBLE[],
                        precipitation DOUBLE[], wind_speed DOUBLE[], wind_direction DOUBLE[], wind_gusts DOUBLE[],
                        soil_temperature DOUBLE[], cloud_cover DOUBLE[], boundary_layer_height DOUBLE[]))'
                }}
            )
        )
        SELECT
            timezone('UTC', time) AS measured_at,  -- live times carry a time zone, historic ones are plain UTC
            temperature,
            relative_humidity,
            atmospheric_pressure,
            precipitation,
            wind_speed,
            wind_direction,
            wind_gusts,
            soil_temperature,
            cloud_cover,
            boundary_layer_height
        FROM live
        WHERE measured_at > (SELECT cutoff FROM archive_end)
        QUALIFY row_number() OVER (PARTITION BY time ORDER BY fetched_at DESC) = 1  -- per hour keep the newest snapshot
        ORDER BY measured_at
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = open_meteo_weather.count("*").fetchall()
        logger.debug("Fetched %s unique OpenMeteo weather live rows", row[0][0])
    return open_meteo_weather


def transform_openmeteo_air_archive(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming OpenMeteo air archive")
    open_meteo_air = s3_con.sql(f"""
        SELECT DISTINCT
            time             AS measured_at,
            pm10             AS pm_10,
            pm2_5            AS pm_25,
            nitrogen_dioxide AS NO2,
            ozone            AS O3,
            sulphur_dioxide  AS SO2
        FROM read_csv(
            '{OPENMETEO_BRONZE_ARCHIVE}',
            hive_partitioning = true,
            union_by_name     = true
        )
        ORDER BY measured_at
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = open_meteo_air.count("*").fetchall()
        logger.debug("Fetched %s unique OpenMeteo air archive rows", row[0][0])
    return open_meteo_air


def transform_openmeteo_air_live(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming OpenMeteo air live")
    open_meteo_air = s3_con.sql(f"""
        WITH archive_end AS (
            SELECT max(measured_at) AS cutoff
            FROM read_parquet('{OPENMETEO_AIR_SILVER_ARCHIVE}')
        ),
        live AS (
            SELECT
                fetched_at,
                UNNEST(air_quality.hourly_air.time)  AS time,
                UNNEST(air_quality.hourly_air.pm_10) AS pm_10,
                UNNEST(air_quality.hourly_air.pm_25) AS pm_25,
                UNNEST(air_quality.hourly_air.NO2)   AS NO2,
                UNNEST(air_quality.hourly_air.O3)    AS O3,
                UNNEST(air_quality.hourly_air.SO2)   AS SO2
            FROM read_json(
                '{OPENMETEO_BRONZE}',
                hive_partitioning = true
            )
        )
        SELECT
            timezone('UTC', time) AS measured_at,
            pm_10,
            pm_25,
            NO2,
            O3,
            SO2
        FROM live
        WHERE measured_at > (SELECT cutoff FROM archive_end)
        QUALIFY row_number() OVER (PARTITION BY time ORDER BY fetched_at DESC) = 1
        ORDER BY measured_at
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = open_meteo_air.count("*").fetchall()
        logger.debug("Fetched %s unique OpenMeteo air live rows", row[0][0])
    return open_meteo_air


def transform_openmeteo_current_live(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming OpenMeteo current weather live")
    open_meteo_current = s3_con.sql(f"""
        WITH bucketed AS (
            SELECT
                date_trunc('hour', timezone('UTC', weather.weather_current.time) - INTERVAL 1 MICROSECOND) + INTERVAL 1 HOUR AS measured_at,  -- 15-min slot in (t-1h, t] belongs to hour t
                timezone('UTC', weather.weather_current.time) AS current_at,
                fetched_at,
                weather.weather_current AS reading
            FROM read_json(
                '{OPENMETEO_BRONZE}',
                hive_partitioning = true,
                columns = {{  -- fixed schema: older snapshots have no boundary_layer_height (-> NULL); detecting it over every file runs out of memory
                    fetched_at: 'TIMESTAMPTZ',
                    weather: 'STRUCT(weather_current STRUCT(
                        "time" TIMESTAMPTZ, temperature DOUBLE, relative_humidity DOUBLE, atmospheric_pressure DOUBLE,
                        precipitation DOUBLE, wind_speed DOUBLE, wind_direction DOUBLE, wind_gusts DOUBLE,
                        cloud_cover DOUBLE, boundary_layer_height DOUBLE))'
                }}
            )
            WHERE weather IS NOT NULL
        )
        SELECT
            measured_at,
            reading.temperature,
            reading.relative_humidity,
            reading.atmospheric_pressure,
            reading.precipitation,
            reading.wind_speed,
            reading.wind_direction,
            reading.wind_gusts,
            reading.cloud_cover,
            reading.boundary_layer_height
        FROM bucketed
        QUALIFY row_number() OVER (PARTITION BY measured_at ORDER BY current_at DESC, fetched_at DESC) = 1  -- per hour keep the newest slot
        ORDER BY measured_at
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = open_meteo_current.count("*").fetchall()
        logger.debug("Fetched %s unique OpenMeteo current weather rows", row[0][0])
    return open_meteo_current


def save_openmeteo_weather_archive_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    open_meteo_weather = transform_openmeteo_weather_archive(s3_con=s3_con)
    open_meteo_weather.write_parquet(OPENMETEO_WEATHER_SILVER_ARCHIVE, compression="zstd")
    logger.info("Saved OpenMeteo weather archive partition to %s", OPENMETEO_WEATHER_SILVER_ARCHIVE)


def save_openmeteo_weather_live_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    open_meteo_weather = transform_openmeteo_weather_live(s3_con=s3_con)
    open_meteo_weather.write_parquet(OPENMETEO_WEATHER_SILVER_LIVE, compression="zstd")
    logger.info("Saved OpenMeteo weather live partition to %s", OPENMETEO_WEATHER_SILVER_LIVE)


def save_openmeteo_air_archive_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    open_meteo_air = transform_openmeteo_air_archive(s3_con=s3_con)
    open_meteo_air.write_parquet(OPENMETEO_AIR_SILVER_ARCHIVE, compression="zstd")
    logger.info("Saved OpenMeteo air archive partition to %s", OPENMETEO_AIR_SILVER_ARCHIVE)


def save_openmeteo_air_live_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    open_meteo_air = transform_openmeteo_air_live(s3_con=s3_con)
    open_meteo_air.write_parquet(OPENMETEO_AIR_SILVER_LIVE, compression="zstd")
    logger.info("Saved OpenMeteo air live partition to %s", OPENMETEO_AIR_SILVER_LIVE)


def save_openmeteo_current_live_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    open_meteo_current = transform_openmeteo_current_live(s3_con=s3_con)
    open_meteo_current.write_parquet(OPENMETEO_CURRENT_SILVER_LIVE, compression="zstd")
    logger.info("Saved OpenMeteo current weather live partition to %s", OPENMETEO_CURRENT_SILVER_LIVE)


def read_openmeteo_weather_silver(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    return s3_con.sql(f"""
        SELECT *
        FROM read_parquet(
            '{OPENMETEO_WEATHER_SILVER_ALL}',
            hive_partitioning = true,
            union_by_name     = true
        )
    """)


def read_openmeteo_air_silver(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    return s3_con.sql(f"""
        SELECT *
        FROM read_parquet(
            '{OPENMETEO_AIR_SILVER_ALL}',
            hive_partitioning = true,
            union_by_name     = true
        )
    """)


def read_openmeteo_current_silver(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    return s3_con.sql(f"""
        SELECT *
        FROM read_parquet(
            '{OPENMETEO_CURRENT_SILVER_ALL}',
            hive_partitioning = true,
            union_by_name     = true
        )
    """)
