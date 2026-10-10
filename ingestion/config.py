import os

# ===========================================================================
# 63 TỈNH THÀNH VIỆT NAM — Tọa độ lấy từ trung tâm hành chính
# ===========================================================================
CITIES = {
    "AG":   {"name": "An Giang",          "lat": 10.3899, "lon": 105.4353},
    "BRVT": {"name": "Bà Rịa-Vũng Tàu",  "lat": 10.3460, "lon": 107.0843},
    "BG":   {"name": "Bắc Giang",         "lat": 21.2820, "lon": 106.1975},
    "BK":   {"name": "Bắc Kạn",           "lat": 22.1471, "lon": 105.8348},
    "BL":   {"name": "Bạc Liêu",          "lat":  9.2940, "lon": 105.7216},
    "BN":   {"name": "Bắc Ninh",          "lat": 21.1861, "lon": 106.0763},
    "BTE":  {"name": "Bến Tre",           "lat": 10.2434, "lon": 106.3756},
    "BD":   {"name": "Bình Định",         "lat": 13.7713, "lon": 109.2307},
    "BDG":  {"name": "Bình Dương",        "lat": 11.1671, "lon": 106.6317},
    "BP":   {"name": "Bình Phước",        "lat": 11.7512, "lon": 106.7235},
    "BTH":  {"name": "Bình Thuận",        "lat": 10.9282, "lon": 108.1022},
    "CM":   {"name": "Cà Mau",            "lat":  9.1770, "lon": 105.1524},
    "CT":   {"name": "Cần Thơ",           "lat": 10.0452, "lon": 105.7469},
    "CB":   {"name": "Cao Bằng",          "lat": 22.6666, "lon": 106.2640},
    "DN":   {"name": "Đà Nẵng",           "lat": 16.0544, "lon": 108.2022},
    "DLK":  {"name": "Đắk Lắk",          "lat": 12.7100, "lon": 108.2378},
    "DNO":  {"name": "Đắk Nông",          "lat": 12.2646, "lon": 107.6098},
    "DB":   {"name": "Điện Biên",         "lat": 21.3867, "lon": 103.0168},
    "DNG":  {"name": "Đồng Nai",          "lat": 10.9574, "lon": 106.8426},
    "DT":   {"name": "Đồng Tháp",         "lat": 10.4938, "lon": 105.6882},
    "GL":   {"name": "Gia Lai",           "lat": 13.9832, "lon": 108.0029},
    "HGI":  {"name": "Hà Giang",          "lat": 22.8026, "lon": 104.9784},
    "HNA":  {"name": "Hà Nam",            "lat": 20.5835, "lon": 105.9230},
    "HN":   {"name": "Hà Nội",            "lat": 21.0285, "lon": 105.8542},
    "HT":   {"name": "Hà Tĩnh",          "lat": 18.3560, "lon": 105.8876},
    "HD":   {"name": "Hải Dương",         "lat": 20.9375, "lon": 106.3144},
    "HP":   {"name": "Hải Phòng",         "lat": 20.8449, "lon": 106.6881},
    "HG":   {"name": "Hậu Giang",         "lat":  9.7579, "lon": 105.6413},
    "HB":   {"name": "Hoà Bình",          "lat": 20.8171, "lon": 105.3382},
    "HCM":  {"name": "Hồ Chí Minh",       "lat": 10.8231, "lon": 106.6297},
    "HY":   {"name": "Hưng Yên",          "lat": 20.6468, "lon": 106.0512},
    "KH":   {"name": "Khánh Hoà",         "lat": 12.2388, "lon": 109.1967},
    "KG":   {"name": "Kiên Giang",        "lat": 10.0125, "lon": 105.0809},
    "KT":   {"name": "Kon Tum",           "lat": 14.3545, "lon": 108.0175},
    "LC":   {"name": "Lai Châu",          "lat": 22.3964, "lon": 103.4586},
    "LD":   {"name": "Lâm Đồng",          "lat": 11.9404, "lon": 108.4583},
    "LS":   {"name": "Lạng Sơn",          "lat": 21.8537, "lon": 106.7615},
    "LCA":  {"name": "Lào Cai",           "lat": 22.4854, "lon": 103.9707},
    "LA":   {"name": "Long An",           "lat": 10.5364, "lon": 106.4133},
    "ND":   {"name": "Nam Định",          "lat": 20.4388, "lon": 106.1621},
    "NA":   {"name": "Nghệ An",           "lat": 18.6796, "lon": 105.6813},
    "NB":   {"name": "Ninh Bình",         "lat": 20.2507, "lon": 105.9745},
    "NT":   {"name": "Ninh Thuận",        "lat": 11.5752, "lon": 108.9890},
    "PT":   {"name": "Phú Thọ",           "lat": 21.4220, "lon": 105.2297},
    "PY":   {"name": "Phú Yên",           "lat": 13.0882, "lon": 109.0929},
    "QB":   {"name": "Quảng Bình",        "lat": 17.4690, "lon": 106.6222},
    "QNA":  {"name": "Quảng Nam",          "lat": 15.5394, "lon": 108.0191},
    "QNG":  {"name": "Quảng Ngãi",         "lat": 15.1214, "lon": 108.8044},
    "QNI":  {"name": "Quảng Ninh",         "lat": 21.0064, "lon": 107.2925},
    "QT":   {"name": "Quảng Trị",          "lat": 16.7455, "lon": 107.1856},
    "ST":   {"name": "Sóc Trăng",          "lat":  9.6025, "lon": 105.9739},
    "SL":   {"name": "Sơn La",             "lat": 21.3270, "lon": 103.9144},
    "TNI":  {"name": "Tây Ninh",           "lat": 11.3352, "lon": 106.1099},
    "TB":   {"name": "Thái Bình",          "lat": 20.4464, "lon": 106.3362},
    "TN":   {"name": "Thái Nguyên",        "lat": 21.5928, "lon": 105.8442},
    "TH":   {"name": "Thanh Hoá",          "lat": 19.8067, "lon": 105.7852},
    "HUE":  {"name": "Thừa Thiên Huế",    "lat": 16.4637, "lon": 107.5909},
    "TGI":  {"name": "Tiền Giang",         "lat": 10.3600, "lon": 106.3630},
    "TV":   {"name": "Trà Vinh",           "lat":  9.9513, "lon": 106.3346},
    "TQ":   {"name": "Tuyên Quang",        "lat": 21.7768, "lon": 105.2280},
    "VL":   {"name": "Vĩnh Long",          "lat": 10.2537, "lon": 105.9722},
    "VP":   {"name": "Vĩnh Phúc",          "lat": 21.3609, "lon": 105.5474},
    "YB":   {"name": "Yên Bái",            "lat": 21.7168, "lon": 104.8986},
}

# Lookup nhanh: city_code → city_name
CITY_NAMES = {code: info["name"] for code, info in CITIES.items()}

# ===========================================================================
# API CONFIG
# ===========================================================================
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
AQI_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
HISTORICAL_WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/archive"
HISTORICAL_AQI_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

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

# ===========================================================================
# MinIO / S3 CONFIG (for lakehouse landing zone)
# ===========================================================================
MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    "region_name": "us-east-1",
}

LAKE_BUCKET = "lake"
LANDING_PREFIX = "landing"

# ===========================================================================
# DATABASE CONFIG (legacy v1 — kept for backward compat)
# ===========================================================================
DB_CONFIG = {
    "host": os.getenv("DATA_DB_HOST", "localhost"),
    "port": os.getenv("DATA_DB_PORT", "5433"),
    "dbname": os.getenv("DATA_DB_NAME", "weather_aqi"),
    "user": os.getenv("DATA_DB_USER", "datauser"),
    "password": os.getenv("DATA_DB_PASSWORD", "datapass"),
}
