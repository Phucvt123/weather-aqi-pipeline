"""
European AQI Thresholds
Source: European Environment Agency
https://www.eea.europa.eu/themes/air/air-quality-index

Index range → Health risk level:
  0-20:   Good
  21-50:  Moderate  
  51-100: Unhealthy
  101+:   Hazardous

Note: Simplified from 6 levels to 4 for this project.
Original levels: Good, Fair, Moderate, Poor, Very Poor, Extremely Poor.
"""

THRESHOLDS = {
    "Good":      {"min": 0,   "max": 20,  "color": "#50C878"},
    "Moderate":  {"min": 21,  "max": 50,  "color": "#FFD700"},
    "Unhealthy": {"min": 51,  "max": 100, "color": "#FF8C00"},
    "Hazardous": {"min": 101, "max": 999, "color": "#DC143C"},
}

ALERT_THRESHOLD = "Unhealthy"  