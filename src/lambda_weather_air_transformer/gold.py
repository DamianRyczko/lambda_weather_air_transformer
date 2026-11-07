import logging

import duckdb

from lambda_weather_air_transformer.config import (
    GIOS_SILVER,
    GOLD,
    IMGW_METEO_SILVER,
    OPENMETEO_WEATHER_SILVER,
)

logger = logging.getLogger(__name__)

def transform_silver(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming Silver data")
    train_data = s3_con.sql(f"""
                    SELECT 
                    gios.measured_at,
                    gios.NO2,
                    gios.O3,
                    gios.PM10,
                    gios."PM2.5",
                    gios.SO2,
                    imgwmeteo.ground_temperature,
                    imgwmeteo.air_temperature,
                    imgwmeteo.relative_humidity,
                    imgwmeteo.precipitation_10min,
                    imgwmeteo.wind_direction,
                    imgwmeteo.wind_avg_speed,
                    imgwmeteo.wind_max_speed,
                    imgwmeteo.wind_max_speed_10min,
                    openmeteo.cloud_cover,
                    openmeteo.atmospheric_pressure
                    FROM read_parquet('{GIOS_SILVER}') gios
                    LEFT JOIN read_parquet('{IMGW_METEO_SILVER}') imgwmeteo USING (measured_at)
                    LEFT JOIN read_parquet('{OPENMETEO_WEATHER_SILVER}') openmeteo USING (measured_at)
                    """)
    
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
    

