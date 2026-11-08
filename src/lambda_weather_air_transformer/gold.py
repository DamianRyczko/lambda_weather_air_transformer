import logging

import duckdb

from lambda_weather_air_transformer.config import (
    GOLD,
    GOLD_APP,
    GOLD_PREDICTION,
)
from lambda_weather_air_transformer.gios import read_gios_index_silver, read_gios_silver
from lambda_weather_air_transformer.imgw import read_imgw_meteo_silver
from lambda_weather_air_transformer.openmeteo import (
    read_openmeteo_air_silver,
    read_openmeteo_current_silver,
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
        row = train_data.count("*").fetchall()
        logger.debug("Fetched %s unique train data rows", row[0][0])
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
    prediction_data = s3_con.sql("""
        SELECT *
        FROM train_data
        WHERE measured_at >= timezone('UTC', now()) - to_days($days)
        ORDER BY measured_at
    """, params={"days": days})
    if logger.isEnabledFor(logging.DEBUG):
        row = prediction_data.count("*").fetchall()
        logger.debug("Fetched %s unique prediction data rows", row[0][0])
    return prediction_data


def save_prediction_data_to_parquet(s3_con: duckdb.DuckDBPyConnection, days: int = 7, hours_ahead: int = 24) -> None:
    prediction_data = transform_prediction_data(s3_con=s3_con, days=days, hours_ahead=hours_ahead)
    prediction_data.write_parquet(GOLD_PREDICTION, compression="zstd")
    logger.info("Saved prediction dataset to %s", GOLD_PREDICTION)


def transform_app_data(s3_con: duckdb.DuckDBPyConnection, days: int = 7, hours_ahead: int = 24) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming app data")
    read_imgw_meteo_silver(s3_con=s3_con).create_view("imgwmeteo")
    read_openmeteo_current_silver(s3_con=s3_con).create_view("openmeteo_current")
    read_openmeteo_air_silver(s3_con=s3_con).create_view("cams")
    read_gios_index_silver(s3_con=s3_con).create_view("gios_index")
    app_data = s3_con.sql("""
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
            openmeteo_current.temperature           AS openmeteo_temperature,
            openmeteo_current.relative_humidity     AS openmeteo_relative_humidity,
            openmeteo_current.atmospheric_pressure  AS openmeteo_atmospheric_pressure,
            openmeteo_current.precipitation         AS openmeteo_precipitation,
            openmeteo_current.wind_speed            AS openmeteo_wind_speed,
            openmeteo_current.wind_direction        AS openmeteo_wind_direction,
            openmeteo_current.wind_gusts            AS openmeteo_wind_gusts,
            openmeteo_current.cloud_cover           AS openmeteo_cloud_cover,
            openmeteo_current.boundary_layer_height AS openmeteo_boundary_layer_height,
            cams.pm_10 AS cams_pm_10,
            cams.pm_25 AS cams_pm_25,
            cams.NO2   AS cams_NO2,
            cams.O3    AS cams_O3,
            cams.SO2   AS cams_SO2,
            gios_index.* EXCLUDE (measured_at, source)
        FROM spine
        LEFT JOIN imgw_hourly ON imgw_hourly.hour = spine.measured_at
        LEFT JOIN openmeteo_current ON openmeteo_current.measured_at = spine.measured_at
        LEFT JOIN cams ON cams.measured_at = spine.measured_at
        LEFT JOIN gios_index ON gios_index.measured_at = spine.measured_at
        WHERE spine.measured_at >= timezone('UTC', now()) - to_days($days)
        ORDER BY spine.measured_at
    """, params={"days": days, "hours_ahead": hours_ahead})
    if logger.isEnabledFor(logging.DEBUG):
        row = app_data.count("*").fetchall()
        logger.debug("Fetched %s unique app data rows", row[0][0])
    return app_data


def save_app_data_to_parquet(s3_con: duckdb.DuckDBPyConnection, days: int = 7, hours_ahead: int = 24) -> None:
    app_data = transform_app_data(s3_con=s3_con, days=days, hours_ahead=hours_ahead)
    app_data.write_parquet(GOLD_APP, compression="zstd")
    logger.info("Saved app dataset to %s", GOLD_APP)
