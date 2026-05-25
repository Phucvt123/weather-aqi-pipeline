import os

# CITIES CONFIG
CITIES = {
    "HCM":  {"name": "Ho Chi Minh",  "lat": 10.8231, "lon": 106.6297},
    "HN":   {"name": "Ha Noi",       "lat": 21.0285, "lon": 105.8542},
    "DN":   {"name": "Da Nang",      "lat": 16.0544, "lon": 108.2022},
    "HP":   {"name": "Hai Phong",    "lat": 20.8449, "lon": 106.6881},
    "CT":   {"name": "Can Tho",      "lat": 10.0452, "lon": 105.7469},
    "HUE":  {"name": "Hue",          "lat": 16.4637, "lon": 107.5909},
    "NT":   {"name": "Nha Trang",    "lat": 12.2388, "lon": 109.1967},
    "DL":   {"name": "Da Lat",       "lat": 11.9404, "lon": 108.4583},
    "VT":   {"name": "Vung Tau",     "lat": 10.3460, "lon": 107.0843},
    "VINH": {"name": "Vinh",         "lat": 18.6796, "lon": 105.6813},
    "QN":   {"name": "Quy Nhon",     "lat": 13.7713, "lon": 109.2307},
}

# API CONFIG
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
AQI_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

WEATHER_PARAMS = {
    "hourly": ",".join([
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "weather_code",
        "surface_pressure",
        "wind_speed_10m",
        "wind_direction_10m",
    ]),
    "timezone": "Asia/Bangkok",
    "past_days": 1,
    "forecast_days": 5,
}

AQI_PARAMS = {
    "hourly": ",".join([
        "pm2_5",
        "pm10",
        "european_aqi",
        "us_aqi",
    ]),
    "timezone": "Asia/Bangkok",
    "past_days": 1,
    "forecast_days": 5,
}

# Fields bắt buộc 
WEATHER_REQUIRED_FIELDS = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"]
AQI_REQUIRED_FIELDS = ["pm2_5", "european_aqi"]

# DATABASE CONFIG
DB_CONFIG = {
    "host": os.getenv("DATA_DB_HOST", "localhost"),
    "port": os.getenv("DATA_DB_PORT", "5433"),
    "dbname": os.getenv("DATA_DB_NAME", "weather_aqi"),
    "user": os.getenv("DATA_DB_USER", "datauser"),
    "password": os.getenv("DATA_DB_PASSWORD", "datapass"),
}
