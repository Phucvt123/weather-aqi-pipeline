"""
landing_writer.py — Ghi JSON thô từ Open-Meteo API lên MinIO landing zone.

Đường dẫn file:
  s3://lake/landing/source=weather/dt=2025-06-15/hour=14/HN.json
  s3://lake/landing/source=aqi/dt=2025-06-15/hour=14/HN.json

Mỗi file chứa nguyên bản JSON gốc + metadata (batch_id, ingested_at, etc).
Bronze layer sẽ đọc từ landing zone này.
"""
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

import boto3
import requests
from botocore.config import Config as BotoConfig

from ingestion.config import (
    CITIES, CITY_NAMES,
    WEATHER_API_URL, AQI_API_URL,
    WEATHER_PARAMS, AQI_PARAMS,
    WEATHER_REQUIRED_FIELDS, AQI_REQUIRED_FIELDS,
    MINIO_CONFIG, LAKE_BUCKET, LANDING_PREFIX,
)

logger = logging.getLogger(__name__)


def _get_s3_client():
    """Tạo boto3 S3 client trỏ vào MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_CONFIG["endpoint_url"],
        aws_access_key_id=MINIO_CONFIG["aws_access_key_id"],
        aws_secret_access_key=MINIO_CONFIG["aws_secret_access_key"],
        region_name=MINIO_CONFIG["region_name"],
        config=BotoConfig(signature_version="s3v4"),
    )


def _validate_response(data: dict, required_fields: list, city_code: str) -> bool:
    """Kiểm tra response API có đủ field bắt buộc không."""
    hourly = data.get("hourly")
    if hourly is None:
        logger.error(f"No hourly data in response for {city_code}")
        return False
    for field in required_fields:
        if field not in hourly:
            logger.error(f"Missing field {field} for {city_code}")
            return False
    return True


def _write_json_to_landing(
    s3_client,
    data: dict,
    source: str,
    city_code: str,
    dt: str,
    hour: str,
    batch_id: str,
    ingested_at: str,
):
    """Ghi 1 file JSON lên MinIO landing zone."""
    # Gắn metadata vào payload
    data["_metadata"] = {
        "city_code": city_code,
        "city_name": CITY_NAMES.get(city_code, city_code),
        "source": source,
        "batch_id": batch_id,
        "ingested_at": ingested_at,
        "request_params": WEATHER_PARAMS if source == "weather" else AQI_PARAMS,
    }

    key = f"{LANDING_PREFIX}/source={source}/dt={dt}/hour={hour}/{city_code}.json"
    s3_client.put_object(
        Bucket=LAKE_BUCKET,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"Wrote s3://{LAKE_BUCKET}/{key}")


def ingest_to_landing(
    execution_time: Optional[str] = None,
    cities: Optional[dict] = None,
):
    """
    Main ingestion function cho lakehouse.
    Gọi cả Weather + AQI API, ghi JSON lên MinIO landing.

    Args:
        execution_time: ISO format string, dùng data_interval_end từ Airflow.
                        Nếu None, dùng UTC now.
        cities: Dict cities để ingest. Nếu None, lấy toàn bộ 63 tỉnh.
    """
    if execution_time:
        exec_dt = datetime.fromisoformat(execution_time)
    else:
        exec_dt = datetime.now(timezone.utc)

    dt_str = exec_dt.strftime("%Y-%m-%d")
    hour_str = exec_dt.strftime("%H")
    ingested_at = exec_dt.isoformat()
    batch_id = f"{dt_str}_{hour_str}_{uuid.uuid4().hex[:8]}"

    if cities is None:
        cities = CITIES

    s3_client = _get_s3_client()
    success_cnt = 0
    fail_cnt = 0

    for city_code, city_info in cities.items():
        lat, lon = city_info["lat"], city_info["lon"]

        # ── Weather ──
        try:
            resp = requests.get(
                WEATHER_API_URL,
                params={"latitude": lat, "longitude": lon, **WEATHER_PARAMS},
                timeout=30,
            )
            resp.raise_for_status()
            weather_data = resp.json()

            if _validate_response(weather_data, WEATHER_REQUIRED_FIELDS, city_code):
                weather_data["_city_code"] = city_code
                _write_json_to_landing(
                    s3_client, weather_data, "weather",
                    city_code, dt_str, hour_str, batch_id, ingested_at,
                )
            else:
                fail_cnt += 1
                continue
        except Exception as e:
            logger.error(f"Weather fetch failed for {city_code}: {e}")
            fail_cnt += 1
            continue

        # ── AQI ──
        try:
            resp = requests.get(
                AQI_API_URL,
                params={"latitude": lat, "longitude": lon, **AQI_PARAMS},
                timeout=30,
            )
            resp.raise_for_status()
            aqi_data = resp.json()

            if _validate_response(aqi_data, AQI_REQUIRED_FIELDS, city_code):
                aqi_data["_city_code"] = city_code
                _write_json_to_landing(
                    s3_client, aqi_data, "aqi",
                    city_code, dt_str, hour_str, batch_id, ingested_at,
                )
            else:
                fail_cnt += 1
                continue
        except Exception as e:
            logger.error(f"AQI fetch failed for {city_code}: {e}")
            fail_cnt += 1
            continue

        success_cnt += 1

    logger.info(f"Ingestion done: {success_cnt} OK, {fail_cnt} failed, batch={batch_id}")

    if success_cnt == 0 and fail_cnt > 0:
        raise RuntimeError("All cities failed during ingestion")

    return {"batch_id": batch_id, "success": success_cnt, "failed": fail_cnt}


def ingest_historical_to_landing(
    start_date: str,
    end_date: str,
    cities: Optional[dict] = None,
):
    """
    Backfill lịch sử từ Open-Meteo Historical API.
    Dùng cho DAG backfill.

    Args:
        start_date: "2024-01-01"
        end_date:   "2024-01-31"
        cities:     Subset cities hoặc None = all 63.
    """
    from ingestion.config import HISTORICAL_WEATHER_API_URL, HISTORICAL_AQI_API_URL

    if cities is None:
        cities = CITIES

    s3_client = _get_s3_client()
    ingested_at = datetime.now(timezone.utc).isoformat()
    batch_id = f"backfill_{start_date}_{end_date}_{uuid.uuid4().hex[:8]}"
    success_cnt = 0
    fail_cnt = 0

    for city_code, city_info in cities.items():
        lat, lon = city_info["lat"], city_info["lon"]

        # ── Historical Weather ──
        try:
            params = {
                "latitude": lat, "longitude": lon,
                "start_date": start_date, "end_date": end_date,
                "hourly": WEATHER_PARAMS["hourly"],
                "timezone": "Asia/Bangkok",
            }
            resp = requests.get(HISTORICAL_WEATHER_API_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            data["_city_code"] = city_code

            key = (
                f"{LANDING_PREFIX}/source=weather_historical/"
                f"dt={start_date}_to_{end_date}/{city_code}.json"
            )
            data["_metadata"] = {
                "city_code": city_code,
                "city_name": CITY_NAMES.get(city_code, city_code),
                "source": "weather_historical",
                "batch_id": batch_id,
                "ingested_at": ingested_at,
                "start_date": start_date,
                "end_date": end_date,
            }
            s3_client.put_object(
                Bucket=LAKE_BUCKET,
                Key=key,
                Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as e:
            logger.error(f"Historical weather failed for {city_code}: {e}")
            fail_cnt += 1
            continue

        # ── Historical AQI ──
        try:
            params = {
                "latitude": lat, "longitude": lon,
                "start_date": start_date, "end_date": end_date,
                "hourly": AQI_PARAMS["hourly"],
                "timezone": "Asia/Bangkok",
            }
            resp = requests.get(HISTORICAL_AQI_API_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            data["_city_code"] = city_code

            key = (
                f"{LANDING_PREFIX}/source=aqi_historical/"
                f"dt={start_date}_to_{end_date}/{city_code}.json"
            )
            data["_metadata"] = {
                "city_code": city_code,
                "city_name": CITY_NAMES.get(city_code, city_code),
                "source": "aqi_historical",
                "batch_id": batch_id,
                "ingested_at": ingested_at,
                "start_date": start_date,
                "end_date": end_date,
            }
            s3_client.put_object(
                Bucket=LAKE_BUCKET,
                Key=key,
                Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as e:
            logger.error(f"Historical AQI failed for {city_code}: {e}")
            fail_cnt += 1
            continue

        success_cnt += 1

    logger.info(
        f"Backfill done: {success_cnt} OK, {fail_cnt} failed, "
        f"range={start_date}→{end_date}, batch={batch_id}"
    )
    if success_cnt == 0 and fail_cnt > 0:
        raise RuntimeError("All cities failed during backfill")

    return {"batch_id": batch_id, "success": success_cnt, "failed": fail_cnt}
