import duckdb

s3_con = duckdb.connect()
s3_con.sql("SET home_directory='/tmp'")  # Lambda: only /tmp is writable 
s3_con.sql("CREATE SECRET (TYPE s3, PROVIDER credential_chain, CHAIN 'env;config', REGION 'eu-north-1')")

S3_URL = "s3://weather-air-data-885427126159-eu-north-1-an"

GIOS_BRONZE = f"{S3_URL}/bronze/gios/**/*.json"
GIOS_BRONZE_ARCHIVE = f"{S3_URL}/bronze/historic/gios/**/*.csv"
GIOS_SILVER = f"{S3_URL}/silver/gios/gios.parquet"

IMGW_BRONZE = f"{S3_URL}/bronze/imgw/**/*.json"
IMGW_BRONZE_METEO_ARCHIVE = f"{S3_URL}/bronze/historic/imgw/meteo/**/*.csv"
IMGW_BRONZE_SYNOP_ARCHIVE = f"{S3_URL}/bronze/historic/imgw/synop/**/s_t_*.csv"
IMGW_METEO_SILVER = f"{S3_URL}/silver/imgw/meteo.parquet"
IMGW_SYNOP_SILVER = f"{S3_URL}/silver/imgw/synop.parquet"

OPENMETEO_BRONZE = f"{S3_URL}/bronze/openmeteo/**/*.json"
OPENMETEO_BRONZE_ARCHIVE = f"{S3_URL}/bronze/historic/openmeteo/**/*.csv"
OPENMETEO_AIR_SILVER = f"{S3_URL}/silver/open_meteo/air.parquet"
OPENMETEO_WEATHER_SILVER = f"{S3_URL}/silver/open_meteo/weather.parquet"

GOLD = f"{S3_URL}/gold/train_dataset.parquet"