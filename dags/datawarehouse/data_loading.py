import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_data():
    file_path = f"./data/YT_data_{date.today()}.json"
    try:
        with open(file_path, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
            logger.info(f"Successfully loaded data from {file_path}")
            return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from file: {file_path}")
        return None