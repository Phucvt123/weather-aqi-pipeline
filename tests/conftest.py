"""
Shared test fixtures.
pytest tự động load file này trước khi chạy bất kỳ test nào.
"""
import pytest
import json


@pytest.fixture
def sample_weather_response():
    """Mock response từ Open-Meteo Weather API."""
    return {
        "latitude": 10.8231,
        "longitude": 106.6297,
        "timezone": "Asia/Ho_Chi_Minh",
        "hourly": {
            "time": [
                "2025-06-15T00:00",
                "2025-06-15T01:00",
                "2025-06-15T02:00",
                "2025-06-15T03:00",
                "2025-06-15T04:00",
            ],
            "temperature_2m": [28.5, 27.3, 26.8, None, 26.1],
            "relative_humidity_2m": [75, 78, 80, 82, None],
            "wind_speed_10m": [12.4, 10.2, 8.5, None, None],
        },
        "_city_code": "HCM",
        "_city_name": "Ho Chi Minh",
    }


@pytest.fixture
def sample_aqi_response():
    """Mock response từ Open-Meteo AQI API."""
    return {
        "latitude": 10.8231,
        "longitude": 106.6297,
        "timezone": "Asia/Ho_Chi_Minh",
        "hourly": {
            "time": [
                "2025-06-15T00:00",
                "2025-06-15T01:00",
                "2025-06-15T02:00",
                "2025-06-15T03:00",
                "2025-06-15T04:00",
            ],
            "pm2_5": [35.2, 42.1, None, 55.8, 72.3],
            "pm10": [48.0, 55.0, None, 68.0, 89.0],
            "european_aqi": [25, 35, None, 55, 85],
        },
        "_city_code": "HCM",
        "_city_name": "Ho Chi Minh",
    }


@pytest.fixture
def sample_weather_response_missing_field():
    """Response thiếu field bắt buộc — dùng test validation."""
    return {
        "latitude": 10.8231,
        "longitude": 106.6297,
        "hourly": {
            "time": ["2025-06-15T00:00"],
            "temperature_2m": [28.5],
            # thiếu relative_humidity_2m và wind_speed_10m
        },
        "_city_code": "HCM",
    }


@pytest.fixture
def sample_weather_response_all_null():
    """Response có field nhưng toàn null — dùng test validation."""
    return {
        "hourly": {
            "time": ["2025-06-15T00:00", "2025-06-15T01:00"],
            "temperature_2m": [None, None],
            "relative_humidity_2m": [None, None],
            "wind_speed_10m": [None, None],
        },
        "_city_code": "HCM",
    }
