import duckdb

s3_con = duckdb.connect()
s3_con.sql("SET home_directory='/tmp'")  # Lambda: only /tmp is writable (extensions get cached here)
s3_con.sql("CREATE SECRET (TYPE s3, PROVIDER credential_chain, CHAIN 'env;config', REGION 'eu-north-1')")  # env = Lambda role / exported CLI creds



gios = s3_con.sql("""
    WITH vertical AS(
    SELECT
        r.date  AS measured_at,
        s.station_code,
        sen.indicator_code,
        r.value
        FROM read_json(
            's3://weather-air-data-885427126159-eu-north-1-an/bronze/gios/**/*.json',
            hive_partitioning = true
        ) AS h,
        UNNEST(h.stations)   AS t1(s),
        UNNEST(s.sensors)    AS t2(sen),
        UNNEST(sen.readings) AS t3(r)
        WHERE s.station_code LIKE 'DsWrocWybCon'
     
     UNION
     
     SELECT 
         data as measured_at,
         kod_stacji as station_code,
         wskaznik as indicator_code,
         wartosc as value
         FROM read_csv(
             's3://weather-air-data-885427126159-eu-north-1-an/bronze/historic/gios/**/*.csv',
             hive_partitioning = true
         ) 
         WHERE station_code LIKE 'DsWrocWybCon'
    )
    PIVOT vertical
    ON indicator_code
    USING first(value)
    GROUP BY measured_at, station_code
    ORDER BY measured_at;
""")

print(gios.limit(5))

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
    s.reading.ground_temperature_date as measured_at,
    s.reading.ground_temperature,
    s.reading.air_temperature,
    s.reading.wind_direction,
    s.reading.wind_avg_speed,
    s.reading.wind_max_speed,
    s.reading.wind_max_speed_10min,
    s.reading.relative_humidity,
    s.reading.precipitation_10min,

        FROM read_json(
            's3://weather-air-data-885427126159-eu-north-1-an/bronze/imgw/**/*.json',
            hive_partitioning = true
        ) as h,
    UNNEST(h.meteo_stations)   AS t1(s)
    
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
imgw_synop = s3_con.sql("""
    SELECT
    s.reading.date,
    s.reading.temperature,
    s.reading.wind_speed,
    s.reading.wind_direction,
    s.reading.relative_humidity,
    s.reading.total_precipitation,
    s.reading.atmospheric_pressure,


    FROM read_json(
            's3://weather-air-data-885427126159-eu-north-1-an/bronze/imgw/**/*.json',
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
                's3://weather-air-data-885427126159-eu-north-1-an/bronze/historic/imgw/synop/**/s_t_*.csv',
                delim = ',',
                header = true,
                hive_partitioning = true
            ) 
""")


print(imgw_meteo.limit(5))
print(imgw_synop.limit(5))

open_meteo_weather = s3_con.sql("""
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
                's3://weather-air-data-885427126159-eu-north-1-an/bronze/openmeteo/**/*.json',
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
            's3://weather-air-data-885427126159-eu-north-1-an/bronze/historic/openmeteo/**/*.csv',
            hive_partitioning = true
        )

    ORDER BY measured_at;
""")

open_meteo_air = s3_con.sql("""
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
                's3://weather-air-data-885427126159-eu-north-1-an/bronze/openmeteo/**/*.json',
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
            's3://weather-air-data-885427126159-eu-north-1-an/bronze/historic/openmeteo/**/*.csv',
            hive_partitioning = true
        )

    ORDER BY measured_at;
""")

print(open_meteo_weather.limit(5))
print(open_meteo_air.limit(5))

# if __name__ == "__main__":

#     main()
