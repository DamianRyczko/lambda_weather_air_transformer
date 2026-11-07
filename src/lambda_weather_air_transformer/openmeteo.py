import logging

import duckdb

from lambda_weather_air_transformer.config import (
    OPENMETEO_AIR_SILVER,
    OPENMETEO_BRONZE,
    OPENMETEO_BRONZE_ARCHIVE,
    OPENMETEO_WEATHER_SILVER,
)

logger = logging.getLogger(__name__)



def transform_openemteo_weather(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming OpenMeteo weather data")
    open_meteo_weather = s3_con.sql(f"""
        WITH live AS(
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
                UNNEST(weather.hourly_forecast.cloud_cover)          AS cloud_cover
                FROM read_json(
                    '{OPENMETEO_BRONZE}',
                    hive_partitioning = true
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
            cloud_cover
            FROM live
            QUALIFY row_number() OVER (PARTITION BY time ORDER BY fetched_at DESC) = 1  -- per hour keep the newest snapshot

        UNION

        SELECT
            time as measured_at,
            temperature_2m as temperature,
            relative_humidity_2m as relative_humidity,
            pressure_msl as atmospheric_pressure,
            precipitation,
            wind_speed_10m as wind_speed,
            wind_direction_10m as wind_direction,
            wind_gusts_10m as wind_gusts,
            soil_temperature_0_to_7cm as soil_temperature,
            cloud_cover
            FROM read_csv(
                '{OPENMETEO_BRONZE_ARCHIVE}',
                hive_partitioning = true
            )

        ORDER BY measured_at;
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = open_meteo_weather.count("*").fetchone() # type: ignore[operator]
        logger.debug("Fetched %s unique OpenMeteo weather rows", row[0] if row else 0)
    return open_meteo_weather

def transform_openemteo_air(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming OpenMeteo air data")
    open_meteo_air = s3_con.sql(f"""
        WITH live AS(
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
            QUALIFY row_number() OVER (PARTITION BY time ORDER BY fetched_at DESC) = 1

        UNION

        SELECT
            time as measured_at,
            pm10 as pm_10,
            pm2_5 as pm_25,
            nitrogen_dioxide as NO2,
            ozone as O3,
            sulphur_dioxide as SO2
            FROM read_csv(
                '{OPENMETEO_BRONZE_ARCHIVE}',
                hive_partitioning = true
            )

        ORDER BY measured_at;
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = open_meteo_air.count("*").fetchone() # type: ignore[operator]
        logger.debug("Fetched %s unique OpenMeteo air rows", row[0] if row else 0)
    return open_meteo_air
    
def save_openemteo_air_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    open_meteo_air = transform_openemteo_air(s3_con = s3_con)
    open_meteo_air.write_parquet(
        f"{OPENMETEO_AIR_SILVER}",
        compression="zstd",
    )
    logger.info("Saved OpenMeteo air data to %s", OPENMETEO_AIR_SILVER)


def save_openemteo_weather_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    open_meteo_weather = transform_openemteo_weather(s3_con = s3_con)
    open_meteo_weather.write_parquet(
        f"{OPENMETEO_WEATHER_SILVER}",
        compression="zstd",
    )
    logger.info("Saved OpenMeteo weather data to %s", OPENMETEO_WEATHER_SILVER)
    

