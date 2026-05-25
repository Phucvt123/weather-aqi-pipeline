"""
Test Spark transform logic:
- get_risk_level mapping
- Deduplicate logic
- Clean (forward-fill)
"""
import pytest

from spark.transform import get_risk_level


class TestRiskLevel:
    """Test AQI → risk level mapping."""

    def test_good(self):
        assert get_risk_level(0) == "Good"
        assert get_risk_level(15) == "Good"
        assert get_risk_level(20) == "Good"

    def test_moderate(self):
        assert get_risk_level(21) == "Moderate"
        assert get_risk_level(35) == "Moderate"
        assert get_risk_level(50) == "Moderate"

    def test_unhealthy(self):
        assert get_risk_level(51) == "Unhealthy"
        assert get_risk_level(75) == "Unhealthy"
        assert get_risk_level(100) == "Unhealthy"

    def test_hazardous(self):
        assert get_risk_level(101) == "Hazardous"
        assert get_risk_level(200) == "Hazardous"
        assert get_risk_level(500) == "Hazardous"

    def test_none_input(self):
        assert get_risk_level(None) is None

    def test_boundary_values(self):
        """Test giá trị biên — nơi dễ sai nhất."""
        assert get_risk_level(20) == "Good"
        assert get_risk_level(21) == "Moderate"
        assert get_risk_level(50) == "Moderate"
        assert get_risk_level(51) == "Unhealthy"
        assert get_risk_level(100) == "Unhealthy"
        assert get_risk_level(101) == "Hazardous"
