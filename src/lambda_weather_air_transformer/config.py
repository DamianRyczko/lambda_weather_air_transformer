import duckdb

s3_con = duckdb.connect()
s3_con.sql("SET home_directory='/tmp'")  # Lambda: only /tmp is writable 
s3_con.sql("CREATE SECRET (TYPE s3, PROVIDER credential_chain, CHAIN 'env;config', REGION 'eu-north-1')")

S3_URL = "s3://weather-air-data-885427126159-eu-north-1-an"

GIOS_BRONZE = f"{S3_URL}/bronze/gios/**/*.json"
GIOS_BRONZE_ARCHIVE = f"{S3_URL}/bronze/historic/gios/**/*.csv"
GIOS_SILVER_ARCHIVE = f"{S3_URL}/silver/gios/source=archive/data.parquet"
GIOS_SILVER_LIVE = f"{S3_URL}/silver/gios/source=live/data.parquet"
GIOS_SILVER_ALL = f"{S3_URL}/silver/gios/*/*.parquet"
GIOS_INDEX_SILVER_LIVE = f"{S3_URL}/silver/gios/index/source=live/data.parquet"
GIOS_INDEX_SILVER_ALL = f"{S3_URL}/silver/gios/index/*/*.parquet"

IMGW_BRONZE = f"{S3_URL}/bronze/imgw/**/*.json"
IMGW_BRONZE_METEO_ARCHIVE = f"{S3_URL}/bronze/historic/imgw/meteo/**/*.csv"
IMGW_BRONZE_SYNOP_ARCHIVE = f"{S3_URL}/bronze/historic/imgw/synop/**/s_t_*.csv"
IMGW_METEO_SILVER_ARCHIVE = f"{S3_URL}/silver/imgw/meteo/source=archive/data.parquet"
IMGW_METEO_SILVER_LIVE = f"{S3_URL}/silver/imgw/meteo/source=live/data.parquet"
IMGW_METEO_SILVER_ALL = f"{S3_URL}/silver/imgw/meteo/*/*.parquet"
IMGW_SYNOP_SILVER_ARCHIVE = f"{S3_URL}/silver/imgw/synop/source=archive/data.parquet"
IMGW_SYNOP_SILVER_LIVE = f"{S3_URL}/silver/imgw/synop/source=live/data.parquet"
IMGW_SYNOP_SILVER_ALL = f"{S3_URL}/silver/imgw/synop/*/*.parquet"

OPENMETEO_BRONZE = f"{S3_URL}/bronze/openmeteo/**/*.json"
OPENMETEO_BRONZE_ARCHIVE = f"{S3_URL}/bronze/historic/openmeteo/**/*.csv"
OPENMETEO_AIR_SILVER_ARCHIVE = f"{S3_URL}/silver/open_meteo/air/source=archive/data.parquet"
OPENMETEO_AIR_SILVER_LIVE = f"{S3_URL}/silver/open_meteo/air/source=live/data.parquet"
OPENMETEO_AIR_SILVER_ALL = f"{S3_URL}/silver/open_meteo/air/*/*.parquet"
OPENMETEO_WEATHER_SILVER_ARCHIVE = f"{S3_URL}/silver/open_meteo/weather/source=archive/data.parquet"
OPENMETEO_WEATHER_SILVER_LIVE = f"{S3_URL}/silver/open_meteo/weather/source=live/data.parquet"
OPENMETEO_WEATHER_SILVER_ALL = f"{S3_URL}/silver/open_meteo/weather/*/*.parquet"
OPENMETEO_CURRENT_SILVER_LIVE = f"{S3_URL}/silver/open_meteo/current/source=live/data.parquet"
OPENMETEO_CURRENT_SILVER_ALL = f"{S3_URL}/silver/open_meteo/current/*/*.parquet"

GOLD = f"{S3_URL}/gold/train_dataset.parquet"
GOLD_PREDICTION = f"{S3_URL}/gold/prediction_data.parquet"
GOLD_APP = f"{S3_URL}/gold/app_data.parquet"