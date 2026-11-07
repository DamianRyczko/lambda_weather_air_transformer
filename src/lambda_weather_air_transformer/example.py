"""Example for imgw_meteo: one time per live row (the newest field date), mismatched values set to NULL and logged.

Run: eval "$(aws configure export-credentials --format env)" && uv run python src/lambda_weather_air_transformer/example.py
"""
import logging

import duckdb

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

s3_con = duckdb.connect()
s3_con.sql("SET home_directory='/tmp'")  # Lambda: only /tmp is writable (extensions get cached here)
s3_con.sql("CREATE SECRET (TYPE s3, PROVIDER credential_chain, CHAIN 'env;config', REGION 'eu-north-1')")  # env = Lambda role / exported CLI creds


# 1. temp table: one row per live snapshot, every value with its own date,
#    and measured_at = the newest of those dates
s3_con.sql("""
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
            's3://weather-air-data-885427126159-eu-north-1-an/bronze/imgw/**/*.json',
            hive_partitioning = true
        ) as h,
    UNNEST(h.meteo_stations)   AS t1(s)
""")


# 2. log the rows where a field's date is not measured_at (those fields become NULL in step 3).
#    A field with no date at all (no reading, e.g. gusts at this station) is not logged.
mismatches = s3_con.sql("""
    SELECT
    fetched_at::VARCHAR AS fetched_at,
    station_id,
    measured_at,
    concat_ws(', ',
        CASE WHEN ground_temperature_date   <> measured_at THEN 'ground_temperature '   || strftime(ground_temperature_date,   '%H:%M') END,
        CASE WHEN air_temperature_date      <> measured_at THEN 'air_temperature '      || strftime(air_temperature_date,      '%H:%M') END,
        CASE WHEN wind_direction_date       <> measured_at THEN 'wind_direction '       || strftime(wind_direction_date,       '%H:%M') END,
        CASE WHEN wind_avg_speed_date       <> measured_at THEN 'wind_avg_speed '       || strftime(wind_avg_speed_date,       '%H:%M') END,
        CASE WHEN wind_max_speed_date       <> measured_at THEN 'wind_max_speed '       || strftime(wind_max_speed_date,       '%H:%M') END,
        CASE WHEN wind_max_speed_10min_date <> measured_at THEN 'wind_max_speed_10min ' || strftime(wind_max_speed_10min_date, '%H:%M') END,
        CASE WHEN relative_humidity_date    <> measured_at THEN 'relative_humidity '    || strftime(relative_humidity_date,    '%H:%M') END,
        CASE WHEN precipitation_10min_date  <> measured_at THEN 'precipitation_10min '  || strftime(precipitation_10min_date,  '%H:%M') END
    ) AS other_dates

        FROM imgw_meteo_live
        WHERE other_dates <> ''
        ORDER BY measured_at
""").fetchall()

for fetched_at, station_id, measured_at, other_dates in mismatches:
    logger.warning("IMGW meteo station %s, row %s (fetched %s): set to NULL, other date: %s",
                   station_id, measured_at, fetched_at, other_dates)


# 3. imgw_meteo as in main.py, but the live half comes from the temp table:
#    a value stays only if its own date is measured_at, otherwise NULL
imgw_meteo = s3_con.sql("""
    WITH vertical AS(
            SELECT *
                FROM read_csv(
                    's3://weather-air-data-885427126159-eu-north-1-an/bronze/historic/imgw/meteo/**/*.csv',
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

print(s3_con.sql("SELECT * FROM imgw_meteo ORDER BY measured_at DESC LIMIT 5"))

print(imgw_meteo.limit(5))