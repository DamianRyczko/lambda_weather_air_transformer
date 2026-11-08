import logging

import duckdb

from lambda_weather_air_transformer.config import (
    GIOS_BRONZE,
    GOLD,
    GOLD_APP,
    GOLD_PREDICTION,
    OPENMETEO_BRONZE,
)
from lambda_weather_air_transformer.gios import read_gios_silver
from lambda_weather_air_transformer.imgw import read_imgw_meteo_silver
from lambda_weather_air_transformer.openmeteo import (
    read_openmeteo_air_silver,
    read_openmeteo_weather_silver,
)

logger = logging.getLogger(__name__)

def transform_silver(s3_con: duckdb.DuckDBPyConnection, hours_ahead: int = 0) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming Silver data")
    read_gios_silver(s3_con=s3_con).create_view("gios")
    read_openmeteo_weather_silver(s3_con=s3_con).create_view("openmeteo")
    train_data = s3_con.sql("""
                    WITH spine AS (
                        SELECT unnest(generate_series(
                            min(measured_at),
                            date_trunc('hour', timezone('UTC', now())) + to_hours($hours_ahead),
                            INTERVAL 1 HOUR
                        )) AS measured_at
                        FROM gios
                    )
                    SELECT
                    spine.measured_at,
                    gios.NO2,
                    gios.O3,
                    gios.PM10,
                    gios."PM2.5",
                    gios.SO2,
                    openmeteo.temperature,
                    openmeteo.relative_humidity,
                    openmeteo.atmospheric_pressure,
                    openmeteo.precipitation,
                    openmeteo.wind_speed,
                    openmeteo.wind_direction,
                    openmeteo.wind_gusts,
                    openmeteo.soil_temperature,
                    openmeteo.cloud_cover,
                    openmeteo.boundary_layer_height
                    FROM spine
                    LEFT JOIN gios USING (measured_at)
                    LEFT JOIN openmeteo USING (measured_at)
                    ORDER BY measured_at
                    """, params={"hours_ahead": hours_ahead})

    if logger.isEnabledFor(logging.DEBUG):
            row = train_data.count("*").fetchone() # type: ignore[operator]
            logger.debug("Fetched %s unique train data rows", row[0] if row else 0)
    return train_data

def save_train_data_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    train_data = transform_silver(s3_con=s3_con)
    train_data.write_parquet(
    f"{GOLD}",
    compression="zstd",
    )
    logger.info("Saved train dataset to %s", GOLD)


def transform_prediction_data(s3_con: duckdb.DuckDBPyConnection, days: int = 7, hours_ahead: int = 24) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming prediction data")
    transform_silver(s3_con=s3_con, hours_ahead=hours_ahead).create_view("train_data")
    return s3_con.sql("""
        SELECT *
        FROM train_data
        WHERE measured_at >= timezone('UTC', now()) - to_days($days)
        ORDER BY measured_at
    """, params={"days": days})


def save_prediction_data_to_parquet(s3_con: duckdb.DuckDBPyConnection, days: int = 7, hours_ahead: int = 24) -> None:
    prediction_data = transform_prediction_data(s3_con=s3_con, days=days, hours_ahead=hours_ahead)
    prediction_data.write_parquet(GOLD_PREDICTION, compression="zstd")
    logger.info("Saved prediction dataset to %s", GOLD_PREDICTION)


def transform_gios_index(s3_con: duckdb.DuckDBPyConnection, days: int = 7) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming GIOS air index")
    s3_con.sql(f"""
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
            WHERE h.date >= timezone('UTC', now())::DATE - to_days($days + 1)
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
        )
        SELECT
            timezone('UTC', timezone('Europe/Warsaw', source_date::TIMESTAMP)) AS measured_at,
            concat('gios_index_', replace(station_code, 'DsWroc', ''), '_', pollutant) AS index_name,
            value
        FROM long
        WHERE source_date IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY station_code, pollutant, source_date ORDER BY fetched_at DESC) = 1
    """, params={"days": days}).create_view("gios_index_hourly")

    return s3_con.sql("""
        PIVOT gios_index_hourly
        ON index_name
        USING first(value)
        GROUP BY measured_at
    """)


def transform_app_data(s3_con: duckdb.DuckDBPyConnection, days: int = 7, hours_ahead: int = 24) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming app data")
    read_imgw_meteo_silver(s3_con=s3_con).create_view("imgwmeteo")
    read_openmeteo_air_silver(s3_con=s3_con).create_view("cams")
    s3_con.register("gios_index", transform_gios_index(s3_con=s3_con, days=days))  # the PIVOT result is held in memory, so register instead of create_view
    return s3_con.sql(f"""
        WITH spine AS (
            SELECT unnest(generate_series(
                date_trunc('hour', timezone('UTC', now()) - to_days($days)),
                date_trunc('hour', timezone('UTC', now())) + to_hours($hours_ahead),
                INTERVAL 1 HOUR
            )) AS measured_at
        ),
        imgw_bucketed AS (
            SELECT
                date_trunc('hour', measured_at - INTERVAL 1 MICROSECOND) + INTERVAL 1 HOUR AS hour,  -- reading in (t-1h, t] belongs to hour t
                *
            FROM imgwmeteo
            WHERE measured_at > timezone('UTC', now()) - to_days($days + 1)
        ),
        imgw_hourly AS (
            SELECT *
            FROM imgw_bucketed
            QUALIFY row_number() OVER (PARTITION BY hour ORDER BY measured_at DESC) = 1
        ),
        openmeteo_bucketed AS (
            SELECT
                date_trunc('hour', timezone('UTC', h.weather.weather_current.time) - INTERVAL 1 MICROSECOND) + INTERVAL 1 HOUR AS hour,  -- 15-min slot in (t-1h, t] belongs to hour t
                timezone('UTC', h.weather.weather_current.time) AS current_at,
                h.fetched_at,
                h.weather.weather_current AS reading
            FROM read_json(
                '{OPENMETEO_BRONZE}',
                hive_partitioning = true,
                union_by_name     = true  -- older snapshots have no boundary_layer_height
            ) AS h
            WHERE h.date >= timezone('UTC', now())::DATE - to_days($days + 1)
              AND h.weather IS NOT NULL
        ),
        openmeteo_hourly AS (
            SELECT *
            FROM openmeteo_bucketed
            QUALIFY row_number() OVER (PARTITION BY hour ORDER BY current_at DESC, fetched_at DESC) = 1
        )
        SELECT
            spine.measured_at,
            imgw_hourly.ground_temperature   AS imgw_ground_temperature,
            imgw_hourly.air_temperature      AS imgw_air_temperature,
            imgw_hourly.relative_humidity    AS imgw_relative_humidity,
            imgw_hourly.precipitation_10min  AS imgw_precipitation_10min,
            imgw_hourly.wind_direction       AS imgw_wind_direction,
            imgw_hourly.wind_avg_speed       AS imgw_wind_avg_speed,
            imgw_hourly.wind_max_speed       AS imgw_wind_max_speed,
            imgw_hourly.wind_max_speed_10min AS imgw_wind_max_speed_10min,
            openmeteo_hourly.reading.temperature           AS openmeteo_temperature,
            openmeteo_hourly.reading.relative_humidity     AS openmeteo_relative_humidity,
            openmeteo_hourly.reading.atmospheric_pressure  AS openmeteo_atmospheric_pressure,
            openmeteo_hourly.reading.precipitation         AS openmeteo_precipitation,
            openmeteo_hourly.reading.wind_speed            AS openmeteo_wind_speed,
            openmeteo_hourly.reading.wind_direction        AS openmeteo_wind_direction,
            openmeteo_hourly.reading.wind_gusts            AS openmeteo_wind_gusts,
            openmeteo_hourly.reading.cloud_cover           AS openmeteo_cloud_cover,
            openmeteo_hourly.reading.boundary_layer_height AS openmeteo_boundary_layer_height,
            cams.pm_10 AS cams_pm_10,
            cams.pm_25 AS cams_pm_25,
            cams.NO2   AS cams_NO2,
            cams.O3    AS cams_O3,
            cams.SO2   AS cams_SO2,
            gios_index.* EXCLUDE (measured_at)
        FROM spine
        LEFT JOIN imgw_hourly ON imgw_hourly.hour = spine.measured_at
        LEFT JOIN openmeteo_hourly ON openmeteo_hourly.hour = spine.measured_at
        LEFT JOIN cams ON cams.measured_at = spine.measured_at
        LEFT JOIN gios_index ON gios_index.measured_at = spine.measured_at
        WHERE spine.measured_at >= timezone('UTC', now()) - to_days($days)
        ORDER BY spine.measured_at
    """, params={"days": days, "hours_ahead": hours_ahead})


def save_app_data_to_parquet(s3_con: duckdb.DuckDBPyConnection, days: int = 7, hours_ahead: int = 24) -> None:
    app_data = transform_app_data(s3_con=s3_con, days=days, hours_ahead=hours_ahead)
    app_data.write_parquet(GOLD_APP, compression="zstd")
    logger.info("Saved app dataset to %s", GOLD_APP)
