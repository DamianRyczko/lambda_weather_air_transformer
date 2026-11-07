import logging

import duckdb

from lambda_weather_air_transformer.config import (
    IMGW_BRONZE,
    IMGW_BRONZE_METEO_ARCHIVE,
    IMGW_BRONZE_SYNOP_ARCHIVE,
    IMGW_METEO_SILVER,
    IMGW_SYNOP_SILVER,
)

logger = logging.getLogger(__name__)

def create_imgw_meteo_table(s3_con: duckdb.DuckDBPyConnection) -> None:
    s3_con.sql(f"""
        CREATE TEMP TABLE imgw_meteo_live AS
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

def transform_imgw_meteo(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Creating temporary IMGW meteo table")
    create_imgw_meteo_table(s3_con = s3_con)
    
    logger.info("Transforming IMGW meteo data")
    imgw_meteo = s3_con.sql(f"""
    WITH vertical AS(
            SELECT *
                FROM read_csv(
                    '{IMGW_BRONZE_METEO_ARCHIVE}',
                    delim = ';',
                    header = true,
                    hive_partitioning = true
                )
        ),
        wide AS(
            PIVOT vertical
            ON parametr IN ('temperatura_gruntu', 'temperatura_powietrza', 'wiatr_kierunek', 'wiatr_srednia_predkosc',
                            'wiatr_predkosc_maksymalna', 'wiatr_poryw_10min', 'wilgotnosc_wzgledna', 'opad_10min')
            USING first(wartosc)
            GROUP BY data, kod_stacji
        )

    SELECT
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

    UNION

    SELECT
        data as measured_at,
        temperatura_gruntu as ground_temperature,
        temperatura_powietrza as air_temperature,
        wiatr_kierunek as wind_direction,
        wiatr_srednia_predkosc as wind_avg_speed,
        wiatr_predkosc_maksymalna as wind_max_speed,
        wiatr_poryw_10min as wind_max_speed_10min,
        wilgotnosc_wzgledna as relative_humidity,
        opad_10min as precipitation_10min
        FROM wide
        ORDER BY measured_at;
    """)
    
    find_imgw_meteo_missmatches(s3_con = s3_con)
    if logger.isEnabledFor(logging.DEBUG):
        row = imgw_meteo.count("*").fetchone() # type: ignore[operator]
        logger.debug("Fetched %s unique imgw meteo rows", row[0] if row else 0)
    return imgw_meteo
    

    
def transform_imgw_synop(s3_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    logger.info("Transforming IMGW synop data")
    imgw_synop = s3_con.sql(f"""
        SELECT
        s.reading.date + to_hours(s.reading.hour::INT) AS measured_at,
        s.reading.temperature,
        s.reading.wind_speed,
        s.reading.wind_direction,
        s.reading.relative_humidity,
        s.reading.total_precipitation,
        s.reading.atmospheric_pressure,


        FROM read_json(
                '{IMGW_BRONZE}',
                hive_partitioning = true
            ) as h,
            UNNEST(h.synop_stations)   AS t1(s)
            
            
        UNION 
        
        SELECT 
            data_pomiaru + to_hours(godzina_pomiaru::INT) AS measured_at,
            temperatura as temperature,
            predkosc_wiatru as wind_speed,
            kierunek_wiatru as wind_direction,
            wilgotnosc_wzgledna as relative_humidity,
            suma_opadu as total_precipitation,
            cisnienie as atmospheric_pressure,
        
                FROM read_csv(
                '{IMGW_BRONZE_SYNOP_ARCHIVE}',
                    delim = ',',
                    header = true,
                    hive_partitioning = true
                ) 
    """)
    if logger.isEnabledFor(logging.DEBUG):
        row = imgw_synop.count("*").fetchone() # type: ignore[operator]
        logger.debug("Fetched %s unique imgw synop rows", row[0] if row else 0)
    return imgw_synop
   

def save_imgw_meteo_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    imgw_meteo = transform_imgw_meteo(s3_con=s3_con)
    logger.info("Saving IMGW meteo data")
    imgw_meteo.write_parquet(
        f"{IMGW_METEO_SILVER}",
        compression="zstd",
    )
    logger.info("Saved IMGW meteo data to %s", IMGW_METEO_SILVER)


def save_imgw_synop_to_parquet(s3_con: duckdb.DuckDBPyConnection) -> None:
    imgw_synop = transform_imgw_synop(s3_con=s3_con)
    logger.info("Saving IMGW meteo data")
    imgw_synop.write_parquet(
    f"{IMGW_SYNOP_SILVER}",
    compression="zstd",
    )
    logger.info("Saved IMGW synop data to %s", IMGW_SYNOP_SILVER)
        


