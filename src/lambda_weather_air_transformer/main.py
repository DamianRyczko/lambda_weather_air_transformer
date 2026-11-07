import argparse
import logging

from lambda_weather_air_transformer.gios import save_gios_to_parquet
from lambda_weather_air_transformer.gold import save_train_data_to_parquet
from lambda_weather_air_transformer.imgw import (
    save_imgw_meteo_to_parquet,
    save_imgw_synop_to_parquet,
)
from lambda_weather_air_transformer.openmeteo import (
    save_openemteo_air_to_parquet,
    save_openemteo_weather_to_parquet,
)

logger = logging.getLogger(__name__)
logging.getLogger("lambda_weather_air_archiver").setLevel(logging.INFO)

def generate_silver():
    try: 
        save_gios_to_parquet()
        save_imgw_meteo_to_parquet()
        save_imgw_synop_to_parquet()
        save_openemteo_weather_to_parquet()
        save_openemteo_air_to_parquet()
        
    except Exception:
        logger.critical("Generating Silver failed", exc_info=True)
        
def generate_gold_dataset():
    try: 
        save_train_data_to_parquet()
    except Exception:
        logger.critical("Generating Gold dataset failed", exc_info=True)

def generate_gold_current_data():
    try: 
        pass
    except Exception:
        logger.critical("Generating Gold current data failed", exc_info=True)
        
def main():
    parser = argparse.ArgumentParser(description="Get data from wether or air quality stations and upload them to S3")
    
    parser.add_argument("--just_silver", action="store_true", help="Regenerate jsut silver stage")
    parser.add_argument("--just_gold", action="store_true", help="Regenerate jsut gold stage")

    args = parser.parse_args()

    if args.just_silver and not args.just_gold:
        generate_silver()
            
    if args.just_gold and not args.just_silver:
        generate_gold_dataset()
            
    if not args.just_gold and not args.just_silver:
        generate_silver()
        generate_gold_dataset()
        
    if args.just_silver and args.just_gold:
        logger.error("Specifing just silver or just gold blocks the other one. To regenrate both silver and gold so not include any additonal flags. Both isthe default")
        
    
        

def lambda_handler(event, context):
    if "silver" in event["targets"]:
        generate_silver()

    if "gold_dataset" in event["targets"]:
        generate_gold_dataset()
        
    if "gold_current_data" in event["targets"]:
        generate_gold_current_data()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)  
    main()