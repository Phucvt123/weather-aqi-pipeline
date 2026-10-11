# 🚀 Hướng dẫn Setup & Chạy — Weather AQI Lakehouse

## Cấu trúc Project hoàn chỉnh

```
weather-aqi-pipeline/
├── docker-compose.yml    ← 10 services: MinIO, Nessie, Spark, Trino, Airflow, Streamlit
├── docker-compose.yml              ← (bản cũ v1 — giữ lại để so sánh)
├── Dockerfile              ← Airflow + Java + Spark client (KHÔNG có PySpark)
├── Dockerfile                      ← (bản cũ v1)
│
├── dags/
│   ├── weather_aqi_dag.py          ← DAG cũ (v1)
│   ├── hourly_ingest.py            ← ★ DAG chính: Ingest → Bronze → Silver → Gold
│   ├── backfill.py                 ← ★ Backfill lịch sử 2 năm
│   └── lake_maintenance.py         ← ★ Compaction + snapshot expiry (daily 03:00 UTC)
│
├── ingestion/
│   ├── config.py                   ← ★ 63 tỉnh thành + MinIO config
│   ├── landing_writer.py           ← ★ Gọi API → ghi JSON lên MinIO landing zone
│   ├── fetch_weather.py            ← (bản cũ v1)
│   └── fetch_aqi.py                ← (bản cũ v1)
│
├── spark/
│   ├── common/
│   │   ├── session.py              ← ★ SparkSession factory (Iceberg/Nessie)
│   │   └── transforms.py           ← ★ Hàm transform thuần (F.when thay UDF)
│   ├── jobs/
│   │   ├── bronze_load.py          ← ★ Landing → Bronze (append-only)
│   │   ├── silver_merge.py         ← ★ Bronze → Silver (MERGE INTO — fix bug #1)
│   │   ├── gold_build.py           ← ★ Silver → Gold (alerts, summary, accuracy)
│   │   └── maintenance.py          ← ★ Compaction, expire snapshots, remove orphans
│   ├── transform.py                ← (bản cũ v1)
│   └── thresholds.py               ← Ngưỡng AQI
│
├── dashboard/
│   ├── app.py                      ← ★ Streamlit + Trino (3 tabs: Real-time, Accuracy, Summary)
│   ├── Dockerfile
│   └── requirements.txt            ← ★ trino thay psycopg2
│
├── infra/
│   ├── spark/spark-defaults.conf   ← ★ Iceberg 1.7.1 + Nessie + MinIO
│   └── trino/catalog/iceberg.properties ← ★ Trino Iceberg connector
│
├── scripts/
│   └── verify_lakehouse.py         ← ★ Test tạo bảng + MERGE INTO
│
└── tests/ db/ docs/ notebooks/     ← giữ nguyên
```

> [!NOTE]
> Các file đánh dấu ★ là file mới/đã sửa cho lakehouse. Bản cũ (v1) vẫn được giữ nguyên.

---

## Bước 1: Chuẩn bị

### Yêu cầu
- **Docker Desktop** ≥ 4.0 (với WSL2 backend trên Windows)
- **RAM:** Ít nhất 10GB cấp cho WSL2

### Cấu hình WSL2 Memory

Tạo hoặc sửa file `C:\Users\<username>\.wslconfig`:

```ini
[wsl2]
memory=10GB
swap=4GB
```

Sau đó restart WSL:
```powershell
wsl --shutdown
```

---

## Bước 2: Khởi động Lakehouse Stack

```powershell
cd c:\VSC\Project\weather-aqi-pipeline

# Build và khởi động tất cả services
docker compose -f docker-compose.yml up -d --build
```

> [!IMPORTANT]
> Lần đầu tiên sẽ mất **5-10 phút** do:
> - Build Airflow image (download Java + Spark ~400MB)
> - Build Dashboard image
> - Pull MinIO, Nessie, Spark, Trino images
> - Spark download Iceberg jar (~200MB) lần đầu submit job

### Kiểm tra services đã healthy

```powershell
docker compose -f docker-compose.yml ps
```

Chờ cho đến khi tất cả services hiện `healthy` hoặc `running`.

---

## Bước 3: Xác nhận hạ tầng hoạt động

### 3.1 Test Spark + Iceberg + Nessie + MinIO

```powershell
docker exec lakehouse-spark-master spark-submit `
    --master spark://spark-master:7077 `
    /opt/spark-apps/scripts/verify_lakehouse.py
```

Kết quả mong đợi: `✅ ALL CHECKS PASSED`

### 3.2 Test Trino đọc được bảng Iceberg

```powershell
docker exec lakehouse-trino trino --execute `
    "SELECT * FROM iceberg.bronze.verify_test"
```

### 3.3 Dọn bảng test

```powershell
docker exec lakehouse-spark-master spark-submit `
    --master spark://spark-master:7077 `
    --conf "spark.sql.defaultCatalog=nessie" `
    -e "DROP TABLE IF EXISTS nessie.bronze.verify_test"
```

---

## Bước 4: Chạy Pipeline lần đầu (Manual)

### 4.1 Mở Airflow UI
- URL: **http://localhost:8080**
- Login: `admin` / `admin`

### 4.2 Enable và trigger DAG `hourly_ingest`
1. Tìm DAG `hourly_ingest` trong danh sách
2. Bật toggle **ON**
3. Click **Trigger DAG** (nút play)

### 4.3 Theo dõi tiến trình
Trong Airflow UI, click vào DAG run để xem 4 tasks:
1. ✅ `ingest_to_landing` — Gọi API, ghi JSON lên MinIO (~1-2 phút cho 63 tỉnh)
2. ✅ `bronze_load` — Spark load landing → Bronze
3. ✅ `silver_merge` — Spark MERGE INTO Silver
4. ✅ `gold_build` — Spark build Gold tables

---

## Bước 5: Truy cập các UI

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | admin / admin |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin123 |
| **Spark Master UI** | http://localhost:8082 | (không cần login) |
| **Trino UI** | http://localhost:8083 | (không cần login) |
| **Streamlit Dashboard** | http://localhost:8501 | (không cần login) |
| **Nessie API** | http://localhost:19120/api/v2/config | (REST API) |

---

## Bước 6: Kiểm tra dữ liệu

### Query trực tiếp từ Trino

```powershell
# Đếm rows trong Silver
docker exec lakehouse-trino trino --execute `
    "SELECT COUNT(*) FROM iceberg.silver.hourly_metrics"

# Xem forecast alerts
docker exec lakehouse-trino trino --execute `
    "SELECT * FROM iceberg.gold.forecast_alerts LIMIT 10"

# Xem forecast accuracy (sau khi chạy pipeline vài ngày)
docker exec lakehouse-trino trino --execute `
    "SELECT * FROM iceberg.gold.forecast_accuracy ORDER BY lead_time_bucket"

# Xem daily summary
docker exec lakehouse-trino trino --execute `
    "SELECT * FROM iceberg.gold.city_daily_summary ORDER BY date_local DESC LIMIT 20"
```

### Xem dữ liệu trên MinIO Console
1. Mở http://localhost:9001
2. Login: `minioadmin` / `minioadmin123`
3. Vào bucket `lake` → thấy:
   - `landing/` — JSON files gốc từ API
   - `warehouse/` — Iceberg table data (Parquet files + metadata)

---

## Bước 7: Backfill lịch sử (tuỳ chọn)

Để có dữ liệu lịch sử cho forecast accuracy:

1. Mở Airflow UI → DAG `backfill`
2. Click **Trigger DAG w/ Config**
3. Nhập JSON:
```json
{"start_date": "2024-10-01", "end_date": "2024-12-31"}
```
4. Lặp lại cho các khoảng thời gian khác

> [!WARNING]
> Open-Meteo Historical API có rate limit. Nên backfill theo từng tháng hoặc quý, không nên lấy cả 2 năm cùng lúc.

---

## Dừng / Khởi động lại

```powershell
# Dừng (giữ data)
docker compose -f docker-compose.yml down

# Dừng + xoá data (reset hoàn toàn)
docker compose -f docker-compose.yml down --volumes

# Khởi động lại
docker compose -f docker-compose.yml up -d
```

---

## Troubleshooting

| Vấn đề | Giải pháp |
|--------|-----------|
| Spark jar download chậm/fail | Kiểm tra mạng. Jar cache sau lần đầu |
| `OutOfMemoryError` | Tăng WSL2 memory lên 12GB |
| Nessie unhealthy | Kiểm tra Postgres đã ready: `docker logs lakehouse-postgres` |
| Trino không thấy bảng | Đảm bảo Nessie healthy + đã chạy verify_lakehouse.py |
| Airflow DAG not showing | Check `docker logs lakehouse-airflow-scheduler` cho import errors |
| MinIO connection refused | `docker logs lakehouse-minio` — kiểm tra port 9000 |
| `spark-submit` not found trong Airflow | Rebuild image: `docker compose -f docker-compose.yml build airflow-webserver` |

---

## So sánh v1 vs v2 (Lakehouse)

| Khía cạnh | v1 (Postgres) | v2 (Lakehouse) |
|-----------|---------------|----------------|
| **Storage** | 2 Postgres DBs | MinIO (S3) + Iceberg |
| **Catalog** | Postgres schemas | Nessie (git-like) |
| **Forecast update** | ❌ `left_anti` → forecast cũ mãi | ✅ `MERGE INTO` → update forecast mới |
| **Read pattern** | Full scan mỗi giờ | Incremental (lookback window) |
| **Spark** | Chạy trong Airflow worker | Cluster riêng biệt |
| **Time reference** | `datetime.now()` | `data_interval_end` (idempotent) |
| **Query engine** | Postgres | Trino |
| **Risk UDF** | Python UDF (chậm) | `F.when()` native Spark |
| **Provinces** | 11 thành phố | 63 tỉnh thành |
| **Dashboard** | 1 tab | 3 tabs (Real-time, Accuracy, Summary) |
