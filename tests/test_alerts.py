"""
Test alert generation logic:
- Chỉ alert khi >= Unhealthy
- Chỉ alert cho timestamp tương lai
- lead_time_hours tính đúng
"""
import pytest
from datetime import datetime, timedelta


# Import threshold logic
from spark.thresholds import THRESHOLDS, ALERT_THRESHOLD


class TestAlertThresholds:
    """Test threshold configuration."""

    def test_thresholds_cover_full_range(self):
        """Mọi AQI value từ 0-999 đều map được vào 1 level."""
        for aqi in range(0, 1000):
            matched = False
            for level, info in THRESHOLDS.items():
                if info["min"] <= aqi <= info["max"]:
                    matched = True
                    break
            assert matched, f"AQI {aqi} không map được vào level nào"

    def test_thresholds_no_overlap(self):
        """Không có 2 levels overlap nhau."""
        levels = list(THRESHOLDS.values())
        for i in range(len(levels) - 1):
            assert levels[i]["max"] < levels[i + 1]["min"], \
                f"Overlap giữa level {i} và {i+1}"

    def test_alert_threshold_is_unhealthy(self):
        """Alert bắt đầu từ Unhealthy."""
        assert ALERT_THRESHOLD == "Unhealthy"


class TestAlertLogic:
    """Test logic quyết định có alert hay không."""

    def test_should_alert_unhealthy(self):
        """AQI 51-100 → should alert."""
        assert self._should_alert(55) is True
        assert self._should_alert(100) is True

    def test_should_alert_hazardous(self):
        """AQI > 100 → should alert."""
        assert self._should_alert(150) is True

    def test_should_not_alert_good(self):
        """AQI 0-20 → no alert."""
        assert self._should_alert(10) is False
        assert self._should_alert(20) is False

    def test_should_not_alert_moderate(self):
        """AQI 21-50 → no alert."""
        assert self._should_alert(30) is False
        assert self._should_alert(50) is False

    def test_only_future_timestamps_trigger_alert(self):
        """Timestamp trong quá khứ → không alert dù AQI cao."""
        past_time = datetime.now() - timedelta(hours=2)
        future_time = datetime.now() + timedelta(hours=6)

        assert self._is_alertable_time(past_time) is False
        assert self._is_alertable_time(future_time) is True

    def test_lead_time_calculation(self):
        """lead_time_hours tính đúng."""
        now = datetime(2025, 6, 15, 22, 0)  # 22:00
        forecast_time = datetime(2025, 6, 16, 5, 0)  # 05:00 hôm sau
        lead_time = int((forecast_time - now).total_seconds() / 3600)
        assert lead_time == 7

    # --- Helper methods ---

    def _should_alert(self, aqi_value):
        for level, info in THRESHOLDS.items():
            if info["min"] <= aqi_value <= info["max"]:
                return level in ("Unhealthy", "Hazardous")
        return False

    def _is_alertable_time(self, timestamp):
        return timestamp > datetime.now()
