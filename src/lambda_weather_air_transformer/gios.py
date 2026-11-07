import logging

import duckdb

from lambda_weather_air_transformer.config import (
    GIOS_BRONZE,
    GIOS_BRONZE_ARCHIVE,
    GIOS_SILVER,
)

SELECTED_STATION = "DsWrocWybCon"

logger = logging.getLogger(__name__)

def transform_gios(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming GIOS data")
    gios = s3_con.sql(f"""
        WITH vertical AS(
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
        
        UNION
        
        SELECT 
            data - INTERVAL 1 HOUR as measured_at,
            kod_stacji as station_code,
            wskaznik as indicator_code,
            wartosc as value
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
        ORDER BY measured_at;
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = gios.count("*").fetchone() # type: ignore[operator]
        logger.debug("Fetched %s unique gios rows", row[0] if row else 0)
    return gios

def save_gios_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    gios = transform_gios(s3_con=s3_con)
    logger.info("Saving GIOS data")
    gios.write_parquet(
        f"{GIOS_SILVER}",
        compression="zstd",
    )
    logger.info("Saved GIOS data to %s", GIOS_SILVER)
    
