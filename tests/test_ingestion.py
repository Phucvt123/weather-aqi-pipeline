"""
Test ingestion layer:
- Validate response đúng/sai
- API call handling
- DB insert logic
"""
import pytest
from unittest.mock import patch, MagicMock
import json

from ingestion.fetch_weather import validate_reponse as validate_response, fetch_weather_data
from ingestion.fetch_aqi import validate_response as validate_aqi_response, fetch_aqi_data


class TestWeatherValidation:
    """Test validate_response cho weather data."""

    def test_valid_response(self, sample_weather_response):
        """Response đầy đủ field → return True."""
        assert validate_response(sample_weather_response, "HCM") is True

    def test_missing_hourly_key(self):
        """Response không có key 'hourly' → return False."""
        data = {"latitude": 10.8, "error": True}
        assert validate_response(data, "HCM") is False

    def test_missing_required_field(self, sample_weather_response_missing_field):
        """Response thiếu field bắt buộc → return False."""
        assert validate_response(sample_weather_response_missing_field, "HCM") is False

    def test_all_null_values(self, sample_weather_response_all_null):
        """Response có field nhưng toàn null → return True (ingestion chấp nhận raw null)."""
        assert validate_response(sample_weather_response_all_null, "HCM") is True


class TestAQIValidation:
    """Test validate_response cho AQI data."""

    def test_valid_response(self, sample_aqi_response):
        assert validate_aqi_response(sample_aqi_response, "HCM") is True

    def test_missing_pm25(self):
        data = {
            "hourly": {
                "time": ["2025-06-15T00:00"],
                "pm10": [48.0],
                "european_aqi": [25],
                # thiếu pm2_5
            }
        }
        assert validate_aqi_response(data, "HCM") is False


class TestFetchWeatherData:
    """Test fetch_weather_data function."""

    @patch("ingestion.fetch_weather.requests.get")
    @patch("ingestion.fetch_weather.get_db_connection")
    def test_successful_fetch_inserts_data(
        self, mock_db, mock_get, sample_weather_response
    ):
        """API trả OK → insert vào DB."""
        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_weather_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Mock DB
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db.return_value = mock_conn

        # Run
        fetch_weather_data()

        # Assert: cursor.execute được gọi 11 lần (11 cities)
        assert mock_cursor.execute.call_count == 11
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("ingestion.fetch_weather.requests.get")
    @patch("ingestion.fetch_weather.get_db_connection")
    def test_api_failure_does_not_crash(self, mock_db, mock_get):
        """API trả 500 → không crash, log error, tiếp tục city khác."""
        mock_get.side_effect = Exception("API timeout")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db.return_value = mock_conn

        # Phải raise vì TẤT CẢ cities fail
        with pytest.raises(Exception, match="All cities failed"):
            fetch_weather_data()

    @patch("ingestion.fetch_weather.requests.get")
    @patch("ingestion.fetch_weather.get_db_connection")
    def test_partial_failure_still_inserts_successful_cities(
        self, mock_db, mock_get, sample_weather_response
    ):
        """Một số city fail, một số OK → vẫn insert cities OK."""
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 3:  # 3 city đầu fail
                raise Exception("timeout")
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = sample_weather_response
            mock_resp.raise_for_status.return_value = None
            return mock_resp

        mock_get.side_effect = side_effect

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db.return_value = mock_conn

        fetch_weather_data()

        # 11 cities - 3 fail = 8 inserts
        assert mock_cursor.execute.call_count == 8
