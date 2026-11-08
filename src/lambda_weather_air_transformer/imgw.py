import logging

import duckdb

from lambda_weather_air_transformer.config import (
    IMGW_BRONZE,
    IMGW_BRONZE_METEO_ARCHIVE,
    IMGW_BRONZE_SYNOP_ARCHIVE,
    IMGW_METEO_SILVER_ALL,
    IMGW_METEO_SILVER_ARCHIVE,
    IMGW_METEO_SILVER_LIVE,
    IMGW_SYNOP_SILVER_ALL,
    IMGW_SYNOP_SILVER_ARCHIVE,
    IMGW_SYNOP_SILVER_LIVE,
)

SELECTED_METEO_STATION = "351160425"

logger = logging.getLogger(__name__)

def create_imgw_meteo_table(s3_con: duckdb.DuckDBPyConnection) -> None:
    s3_con.sql(f"""
        CREATE OR REPLACE TEMP TABLE imgw_meteo_live AS
        SELECT
        h.fetched_at,
        s.id AS station_id,
        greatest(
            s.reading.ground_temperature_date,
            s.reading.air_temperature_date,
            s.reading.wind_direction_date,
            s.reading.wind_avg_speed_date,
            s.reading.wind_max_speed_date,
            s.reading.wind_max_speed_10min_date,
            s.reading.relative_humidity_date,
            s.reading.precipitation_10min_date
        ) AS measured_at,
        UNNEST(s.reading),  -- every field of reading as its own column: the values and their *_date

            FROM read_json(
                '{IMGW_BRONZE}',
                hive_partitioning = true
            ) as h,
        UNNEST(h.meteo_stations)   AS t1(s)
        """)

def find_imgw_meteo_missmatches(s3_con: duckdb.DuckDBPyConnection) -> None:
    mismatches = s3_con.sql("""
        SELECT
        fetched_at::VARCHAR AS fetched_at,
        station_id,
        measured_at,
        concat_ws(', ',
            CASE WHEN ground_temperature_date   <> measured_at THEN concat('ground_temperature ',   strftime(ground_temperature_date,   '%H:%M')) END,
            CASE WHEN air_temperature_date      <> measured_at THEN concat('air_temperature ',      strftime(air_temperature_date,      '%H:%M')) END,
            CASE WHEN wind_direction_date       <> measured_at THEN concat('wind_direction ',       strftime(wind_direction_date,       '%H:%M')) END,
            CASE WHEN wind_avg_speed_date       <> measured_at THEN concat('wind_avg_speed ',       strftime(wind_avg_speed_date,       '%H:%M')) END,
            CASE WHEN wind_max_speed_date       <> measured_at THEN concat('wind_max_speed ',       strftime(wind_max_speed_date,       '%H:%M')) END,
            CASE WHEN wind_max_speed_10min_date <> measured_at THEN concat('wind_max_speed_10min ', strftime(wind_max_speed_10min_date, '%H:%M')) END,
            CASE WHEN relative_humidity_date    <> measured_at THEN concat('relative_humidity ',    strftime(relative_humidity_date,    '%H:%M')) END,
            CASE WHEN precipitation_10min_date  <> measured_at THEN concat('precipitation_10min ',  strftime(precipitation_10min_date,  '%H:%M')) END
        ) AS other_dates

            FROM imgw_meteo_live
            WHERE other_dates <> ''
            ORDER BY measured_at
        """).fetchall()

    for fetched_at, station_id, measured_at, other_dates in mismatches:
        logger.warning("IMGW meteo station %s, row %s (fetched %s): set to NULL, other date: %s",
            station_id, measured_at, fetched_at, other_dates)


def transform_imgw_meteo_archive(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming IMGW meteo archive")
    imgw_meteo = s3_con.sql(f"""
        WITH vertical AS (
            SELECT *
            FROM read_csv(
                '{IMGW_BRONZE_METEO_ARCHIVE}',
                delim = ';',
                header = true,
                hive_partitioning = true
            )
            WHERE kod_stacji::VARCHAR = '{SELECTED_METEO_STATION}'
        ),
        wide AS (
            PIVOT vertical
            ON parametr IN ('temperatura_gruntu', 'temperatura_powietrza', 'wiatr_kierunek', 'wiatr_srednia_predkosc',
                            'wiatr_predkosc_maksymalna', 'wiatr_poryw_10min', 'wilgotnosc_wzgledna', 'opad_10min')
            USING first(wartosc)
            GROUP BY data, kod_stacji
        )
        SELECT DISTINCT
            data                      AS measured_at,
            temperatura_gruntu        AS ground_temperature,
            temperatura_powietrza     AS air_temperature,
            wiatr_kierunek            AS wind_direction,
            wiatr_srednia_predkosc    AS wind_avg_speed,
            wiatr_predkosc_maksymalna AS wind_max_speed,
            wiatr_poryw_10min         AS wind_max_speed_10min,
            wilgotnosc_wzgledna       AS relative_humidity,
            opad_10min                AS precipitation_10min
        FROM wide
        ORDER BY measured_at
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = imgw_meteo.count("*").fetchall()
        logger.debug("Fetched %s unique imgw meteo archive rows", row[0][0])
    return imgw_meteo


def transform_imgw_meteo_live(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Creating temporary IMGW meteo table")
    create_imgw_meteo_table(s3_con=s3_con)

    logger.info("Transforming IMGW meteo live")
    imgw_meteo = s3_con.sql(f"""
        WITH archive_end AS (
            SELECT max(measured_at) AS cutoff
            FROM read_parquet('{IMGW_METEO_SILVER_ARCHIVE}')
        )
        SELECT DISTINCT
            measured_at,
            CASE WHEN ground_temperature_date   = measured_at THEN ground_temperature   END AS ground_temperature,
            CASE WHEN air_temperature_date      = measured_at THEN air_temperature      END AS air_temperature,
            CASE WHEN wind_direction_date       = measured_at THEN wind_direction       END AS wind_direction,
            CASE WHEN wind_avg_speed_date       = measured_at THEN wind_avg_speed       END AS wind_avg_speed,
            CASE WHEN wind_max_speed_date       = measured_at THEN wind_max_speed       END AS wind_max_speed,
            CASE WHEN wind_max_speed_10min_date = measured_at THEN wind_max_speed_10min END AS wind_max_speed_10min,
            CASE WHEN relative_humidity_date    = measured_at THEN relative_humidity    END AS relative_humidity,
            CASE WHEN precipitation_10min_date  = measured_at THEN precipitation_10min  END AS precipitation_10min
        FROM imgw_meteo_live
        WHERE measured_at > (SELECT cutoff FROM archive_end)
        ORDER BY measured_at
    """)

    find_imgw_meteo_missmatches(s3_con=s3_con)
    if logger.isEnabledFor(logging.DEBUG):
        row = imgw_meteo.count("*").fetchall()
        logger.debug("Fetched %s unique imgw meteo live rows", row[0][0])
    return imgw_meteo


def transform_imgw_synop_archive(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming IMGW synop archive")
    imgw_synop = s3_con.sql(f"""
        SELECT DISTINCT
            data_pomiaru + to_hours(godzina_pomiaru::INT) AS measured_at,
            temperatura         AS temperature,
            predkosc_wiatru     AS wind_speed,
            kierunek_wiatru     AS wind_direction,
            wilgotnosc_wzgledna AS relative_humidity,
            suma_opadu          AS total_precipitation,
            cisnienie           AS atmospheric_pressure
        FROM read_csv(
            '{IMGW_BRONZE_SYNOP_ARCHIVE}',
            delim = ',',
            header = true,
            hive_partitioning = true
        )
        ORDER BY measured_at
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = imgw_synop.count("*").fetchall()
        logger.debug("Fetched %s unique imgw synop archive rows", row[0][0])
    return imgw_synop


def transform_imgw_synop_live(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming IMGW synop live")
    imgw_synop = s3_con.sql(f"""
        WITH archive_end AS (
            SELECT max(measured_at) AS cutoff
            FROM read_parquet('{IMGW_SYNOP_SILVER_ARCHIVE}')
        )
        SELECT DISTINCT
            s.reading.date + to_hours(s.reading.hour::INT) AS measured_at,
            s.reading.temperature          AS temperature,
            s.reading.wind_speed           AS wind_speed,
            s.reading.wind_direction       AS wind_direction,
            s.reading.relative_humidity    AS relative_humidity,
            s.reading.total_precipitation  AS total_precipitation,
            s.reading.atmospheric_pressure AS atmospheric_pressure
        FROM read_json(
            '{IMGW_BRONZE}',
            hive_partitioning = true
        ) AS h,
            UNNEST(h.synop_stations) AS t1(s)
        WHERE measured_at > (SELECT cutoff FROM archive_end)
        ORDER BY measured_at
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = imgw_synop.count("*").fetchall()
        logger.debug("Fetched %s unique imgw synop live rows", row[0][0])
    return imgw_synop


def save_imgw_meteo_archive_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    imgw_meteo = transform_imgw_meteo_archive(s3_con=s3_con)
    imgw_meteo.write_parquet(IMGW_METEO_SILVER_ARCHIVE, compression="zstd")
    logger.info("Saved IMGW meteo archive partition to %s", IMGW_METEO_SILVER_ARCHIVE)


def save_imgw_meteo_live_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    imgw_meteo = transform_imgw_meteo_live(s3_con=s3_con)
    imgw_meteo.write_parquet(IMGW_METEO_SILVER_LIVE, compression="zstd")
    logger.info("Saved IMGW meteo live partition to %s", IMGW_METEO_SILVER_LIVE)


def save_imgw_synop_archive_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    imgw_synop = transform_imgw_synop_archive(s3_con=s3_con)
    imgw_synop.write_parquet(IMGW_SYNOP_SILVER_ARCHIVE, compression="zstd")
    logger.info("Saved IMGW synop archive partition to %s", IMGW_SYNOP_SILVER_ARCHIVE)


def save_imgw_synop_live_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    imgw_synop = transform_imgw_synop_live(s3_con=s3_con)
    imgw_synop.write_parquet(IMGW_SYNOP_SILVER_LIVE, compression="zstd")
    logger.info("Saved IMGW synop live partition to %s", IMGW_SYNOP_SILVER_LIVE)


def read_imgw_meteo_silver(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    return s3_con.sql(f"""
        SELECT *
        FROM read_parquet(
            '{IMGW_METEO_SILVER_ALL}',
            hive_partitioning = true,
            union_by_name     = true
        )
    """)


def read_imgw_synop_silver(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    return s3_con.sql(f"""
        SELECT *
        FROM read_parquet(
            '{IMGW_SYNOP_SILVER_ALL}',
            hive_partitioning = true,
            union_by_name     = true
        )
    """)
