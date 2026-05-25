import psycopg2
import requests
import psycopg2
import json
import logging
from ingestion.config import CITIES, WEATHER_API_URL, WEATHER_PARAMS, WEATHER_REQUIRED_FIELDS, DB_CONFIG

logger = logging.getLogger(__name__)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def validate_reponse(data:dict, city_code: str) -> bool:
    hourly = data.get("hourly")
    if hourly is None:
        logger.error(f"No hourly data in weather response for {city_code}")
        return False
    for field in WEATHER_REQUIRED_FIELDS:
        if field not in hourly:
            logger.error(f"Missing weather data field {field} for {city_code}")
            return False
    return True

def fetch_weather_data():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        success_cnt = 0
        fail_cnt = 0
        for city_code, city_info in CITIES.items():
            logger.info(f"Fetching data for {city_info['name']} ({city_code})")
            lat = city_info["lat"]
            lon = city_info["lon"]
            try:
                params = {
                    "latitude": lat,
                    "longitude": lon,
                    **WEATHER_PARAMS,
                }
                reponse = requests.get(WEATHER_API_URL, params=params)
                if reponse.status_code != 200:
                    logger.error(f"API request failed with status code {reponse.status_code}")
                    fail_cnt += 1
                    continue
                data = reponse.json()
                if not validate_reponse(data, city_code):
                    fail_cnt += 1
                    continue
                success_cnt += 1
                data["_city_code"] = city_code
                data["_city_name"] = city_info["name"] 
                cursor.execute(
                    """
                    INSERT INTO raw.raw_weather (source, payload)
                    VALUES (%s, %s)
                    """,
                    ("open_meteo", json.dumps(data))
                )
            except Exception as e:
                logger.error(f"Error fetching weather data for {city_code}: {e}")
                fail_cnt += 1
                continue
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return
    finally:
        if conn:
            conn.commit()
            cursor.close()
            conn.close()
        if success_cnt == 0 and fail_cnt > 0:
            raise Exception("All cities failed to fetch weather data")