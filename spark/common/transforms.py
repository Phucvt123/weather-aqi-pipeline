"""
transforms.py — Hàm xử lý thuần (pure functions), dễ unit test.

Tất cả logic transform được tách ra khỏi I/O (đọc/ghi bảng).
Chỉ nhận DataFrame vào, trả DataFrame ra.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# ===========================================================================
# HEALTH RISK — Thay UDF bằng native Spark (fix bug #5a)
# ===========================================================================
def add_health_risk(df: DataFrame, aqi_col: str = "aqi_eu") -> DataFrame:
    """
    Tính health_risk từ European AQI bằng F.when() thay vì Python UDF.
    Nhanh hơn vì chạy hoàn toàn trên JVM, không serialize qua Python.
    """
    return df.withColumn(
        "health_risk",
        F.when(F.col(aqi_col).isNull(), None)
        .when(F.col(aqi_col) <= 20, F.lit("Good"))
        .when(F.col(aqi_col) <= 50, F.lit("Moderate"))
        .when(F.col(aqi_col) <= 100, F.lit("Unhealthy"))
        .otherwise(F.lit("Hazardous"))
    )


# ===========================================================================
# DEDUPLICATE
# ===========================================================================
def deduplicate(df: DataFrame, key_columns: list) -> DataFrame:
    """
    Giữ bản ghi mới nhất (ingested_at lớn nhất) cho mỗi tổ hợp key.
    """
    window_spec = (
        Window
        .partitionBy(*key_columns)
        .orderBy(F.col("ingested_at").desc())
    )
    return (
        df.withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ===========================================================================
# CLEAN / FORWARD-FILL
# ===========================================================================
def forward_fill(df: DataFrame, value_columns: list) -> DataFrame:
    """
    Forward-fill null trong window 2 giờ.
    Sau đó drop row nếu TẤT CẢ value columns đều null.
    """
    window_spec = (
        Window
        .partitionBy("city_code")
        .orderBy("timestamp_local")
        .rowsBetween(-2, 0)
    )
    for col_name in value_columns:
        df = df.withColumn(
            col_name,
            F.coalesce(
                F.col(col_name),
                F.last(F.col(col_name), ignorenulls=True).over(window_spec),
            ),
        )
    return df.dropna(subset=value_columns, how="all")


# ===========================================================================
# JOIN WEATHER + AQI
# ===========================================================================
def join_weather_aqi(weather_df: DataFrame, aqi_df: DataFrame) -> DataFrame:
    """
    Join weather + AQI theo city_code và timestamp_local.
    Dùng outer join để giữ lại data nếu 1 bên thiếu.
    """
    return weather_df.join(
        aqi_df,
        on=["city_code", "timestamp_local"],
        how="outer",
    )


# ===========================================================================
# ALERT GENERATION — Dùng execution_time thay cho datetime.now() (fix bug #3)
# ===========================================================================
def generate_alerts(
    df: DataFrame,
    execution_time_str: str,
    city_names: dict,
) -> DataFrame:
    """
    Tìm các forecast giờ tương lai có AQI >= Unhealthy.
    Dùng execution_time từ Airflow thay cho datetime.now().

    Args:
        df: DataFrame đã có health_risk
        execution_time_str: ISO string (data_interval_end từ Airflow)
        city_names: Dict city_code → city_name
    """
    exec_ts = F.to_timestamp(F.lit(execution_time_str))

    # Tạo city_name mapping
    from pyspark.sql.types import StringType, MapType
    city_map = F.create_map(
        *[item for code, name in city_names.items()
          for item in (F.lit(code), F.lit(name))]
    )

    alerts_df = (
        df.filter(F.col("timestamp_local") > exec_ts)
        .filter(F.col("health_risk").isin("Unhealthy", "Hazardous"))
        .withColumn("forecast_for_time", F.col("timestamp_local"))
        .withColumn(
            "lead_time_hours",
            (
                (F.unix_timestamp("timestamp_local") - F.unix_timestamp(exec_ts))
                / 3600
            ).cast("int"),
        )
        .withColumn(
            "city_name",
            F.coalesce(city_map[F.col("city_code")], F.col("city_code")),
        )
        .withColumn(
            "message",
            F.concat(
                F.lit("Dự báo "),
                F.col("city_name"),
                F.lit(": AQI = "),
                F.col("aqi_eu").cast("string"),
                F.lit(" ("),
                F.col("health_risk"),
                F.lit(") lúc "),
                F.date_format("timestamp_local", "HH:mm dd/MM"),
                F.lit(". PM2.5 = "),
                F.coalesce(F.col("pm2_5").cast("string"), F.lit("N/A")),
                F.lit(" µg/m³"),
            ),
        )
        .withColumn("created_at", F.current_timestamp())
        .select(
            "city_code", "city_name", "timestamp_local",
            "health_risk",
            F.col("aqi_eu").alias("aqi_value"),
            "message", "forecast_for_time", "lead_time_hours",
            "created_at",
        )
    )
    return alerts_df
