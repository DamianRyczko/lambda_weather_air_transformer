import argparse
import logging

from lambda_weather_air_transformer.config import s3_con
from lambda_weather_air_transformer.gios import (
    save_gios_archive_to_parquet,
    save_gios_index_live_to_parquet,
    save_gios_live_to_parquet,
)
from lambda_weather_air_transformer.gold import (
    save_app_data_to_parquet,
    save_prediction_data_to_parquet,
    save_train_data_to_parquet,
)
from lambda_weather_air_transformer.imgw import (
    save_imgw_meteo_archive_to_parquet,
    save_imgw_meteo_live_to_parquet,
    save_imgw_synop_archive_to_parquet,
    save_imgw_synop_live_to_parquet,
)
from lambda_weather_air_transformer.openmeteo import (
    save_openmeteo_air_archive_to_parquet,
    save_openmeteo_air_live_to_parquet,
    save_openmeteo_current_live_to_parquet,
    save_openmeteo_weather_archive_to_parquet,
    save_openmeteo_weather_live_to_parquet,
)

logger = logging.getLogger(__name__)
logging.getLogger("lambda_weather_air_archiver").setLevel(logging.INFO)

def generate_silver_archive():
    try:
        save_gios_archive_to_parquet(s3_con)
        save_imgw_meteo_archive_to_parquet(s3_con)
        save_imgw_synop_archive_to_parquet(s3_con)
        save_openmeteo_weather_archive_to_parquet(s3_con)
        save_openmeteo_air_archive_to_parquet(s3_con)

    except Exception:
        logger.critical("Generating Silver archive failed", exc_info=True)

def generate_silver_live():
    try:
        save_gios_live_to_parquet(s3_con)
        save_imgw_meteo_live_to_parquet(s3_con)
        save_imgw_synop_live_to_parquet(s3_con)
        save_openmeteo_weather_live_to_parquet(s3_con)
        save_openmeteo_air_live_to_parquet(s3_con)
        save_gios_index_live_to_parquet(s3_con)
        save_openmeteo_current_live_to_parquet(s3_con)

    except Exception:
        logger.critical("Generating Silver live failed", exc_info=True)

def generate_silver():
    generate_silver_archive()
    generate_silver_live()

def generate_gold_dataset():
    try:
        save_train_data_to_parquet(s3_con)
    except Exception:
        logger.critical("Generating Gold dataset failed", exc_info=True)

def generate_gold_current_data():
    try:
        save_prediction_data_to_parquet(s3_con)
    except Exception:
        logger.critical("Generating Gold prediction data failed", exc_info=True)
    try:
        save_app_data_to_parquet(s3_con)
    except Exception:
        logger.critical("Generating Gold app data failed", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description="Get data from wether or air quality stations and upload them to S3")

    parser.add_argument("--just_silver", action="store_true", help="Regenerate just silver stage")
    parser.add_argument("--just_gold", action="store_true", help="Regenerate just gold stage")
    parser.add_argument("--current_data", action="store_true", help="Regenerate just prediction and app data")

    args = parser.parse_args()

    if args.just_silver and not args.just_gold:
        generate_silver()

    if args.just_gold and not args.just_silver:
        generate_gold_dataset()
        generate_gold_current_data()
        
    if args.current_data:
        generate_silver_live()
        generate_gold_current_data()

    if not args.just_gold and not args.just_silver and not args.current_data:
        generate_silver()
        generate_gold_dataset()
        generate_gold_current_data()

    if args.just_silver and args.just_gold:
        logger.error("Specifing just silver or just gold blocks the other one. To regenrate both silver and gold so not include any additonal flags. Both is the default")




def lambda_handler(event, context):
    if "silver" in event["targets"]:
        generate_silver_archive()
        generate_silver_live()

    if "gold_dataset" in event["targets"]:
        generate_gold_dataset()

    if "gold_current_data" in event["targets"]:
        generate_silver_live()
        generate_gold_current_data()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
