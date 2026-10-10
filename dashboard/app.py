import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import time

# ============================================================
# CONFIG — Chuyển từ Postgres sang Trino
# ============================================================
TRINO_CONFIG = {
    "host": os.getenv("TRINO_HOST", "localhost"),
    "port": int(os.getenv("TRINO_PORT", "8083")),
    "catalog": os.getenv("TRINO_CATALOG", "iceberg"),
}

RISK_COLORS = {
    "Good": "linear-gradient(135deg, #10B981 0%, #059669 100%)",
    "Moderate": "linear-gradient(135deg, #F59E0B 0%, #D97706 100%)",
    "Unhealthy": "linear-gradient(135deg, #EF4444 0%, #DC2626 100%)",
    "Hazardous": "linear-gradient(135deg, #8B5CF6 0%, #7C3AED 100%)",
}

RISK_HEX = {
    "Good": "#10B981",
    "Moderate": "#F59E0B",
    "Unhealthy": "#EF4444",
    "Hazardous": "#8B5CF6",
}

REFRESH_INTERVAL = 300


# ============================================================
# DATABASE FUNCTIONS — Trino via trino-python-client
# ============================================================
@st.cache_resource
def get_trino_connection():
    from trino.dbapi import connect
    return connect(
        host=TRINO_CONFIG["host"],
        port=TRINO_CONFIG["port"],
        catalog=TRINO_CONFIG["catalog"],
        schema="silver",
        user="streamlit",
    )


def query_df(sql, params=None):
    conn = get_trino_connection()
    try:
        return pd.read_sql(sql, conn, params=params)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


def get_all_cities():
    """Lấy danh sách city có data từ Silver."""
    df = query_df("""
        SELECT DISTINCT city_code, city_name
        FROM silver.hourly_metrics
        ORDER BY city_code
    """)
    if not df.empty:
        return dict(zip(df["city_code"], df["city_name"]))
    return {}


def get_latest_metrics(city_code):
    """Row thời gian thực mới nhất cho 1 thành phố."""
    df = query_df(f"""
        SELECT * FROM silver.hourly_metrics
        WHERE city_code = '{city_code}'
          AND timestamp_local <= current_timestamp
        ORDER BY timestamp_local DESC
        LIMIT 1
    """)
    return df.iloc[0] if not df.empty else None


def get_metrics_range(city_code, hours=24):
    """Lấy data lịch sử + dự báo."""
    forecast_hours = min(hours, 120)
    df = query_df(f"""
        SELECT timestamp_local, temperature_c, humidity_pct,
               windspeed_kmh, pm2_5, pm10, aqi_eu, health_risk
        FROM silver.hourly_metrics
        WHERE city_code = '{city_code}'
          AND timestamp_local >= current_timestamp - INTERVAL '{hours}' HOUR
          AND timestamp_local <= current_timestamp + INTERVAL '{forecast_hours}' HOUR
        ORDER BY timestamp_local
    """)
    return df


def get_forecast_alerts(city_code=None):
    """Lấy forecast alerts từ Gold."""
    if city_code:
        return query_df(f"""
            SELECT * FROM gold.forecast_alerts
            WHERE forecast_for_time > current_timestamp
              AND city_code = '{city_code}'
            ORDER BY forecast_for_time
        """)
    else:
        return query_df("""
            SELECT * FROM gold.forecast_alerts
            WHERE forecast_for_time > current_timestamp
            ORDER BY forecast_for_time
            LIMIT 20
        """)


def get_daily_summary(city_code, days=7):
    """Lấy daily summary từ Gold."""
    return query_df(f"""
        SELECT * FROM gold.city_daily_summary
        WHERE city_code = '{city_code}'
          AND date_local >= current_date - INTERVAL '{days}' DAY
        ORDER BY date_local DESC
    """)


def get_forecast_accuracy():
    """Lấy forecast accuracy từ Gold — điểm nhấn project."""
    return query_df("""
        SELECT city_code, lead_time_bucket, mae, bias, rmse, sample_count
        FROM gold.forecast_accuracy
        ORDER BY lead_time_bucket, city_code
    """)


# ============================================================
# UI COMPONENTS
# ============================================================
def inject_custom_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');
    html, body, [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
        font-family: 'Plus Jakarta Sans', sans-serif;
        background: linear-gradient(135deg, #0b0f19 0%, #111827 50%, #1e1b4b 100%);
        color: #f3f4f6 !important;
    }
    [data-testid="stHeader"] { background-color: rgba(0, 0, 0, 0); }
    [data-testid="stSidebar"] {
        background-color: rgba(17, 24, 39, 0.85) !important;
        border-right: 1px solid rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
    }
    h1, h2, h3 { color: #ffffff !important; font-weight: 700 !important; letter-spacing: -0.02em !important; }
    .glass-card {
        background: rgba(255, 255, 255, 0.03); backdrop-filter: blur(16px);
        border: 1px solid rgba(255, 255, 255, 0.06); border-radius: 20px;
        padding: 24px; transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25); margin-bottom: 20px;
    }
    .glass-card:hover {
        transform: translateY(-4px); border-color: rgba(255, 255, 255, 0.12);
        box-shadow: 0 15px 40px rgba(99, 102, 241, 0.15); background: rgba(255, 255, 255, 0.05);
    }
    .metric-card {
        background: rgba(255, 255, 255, 0.03); backdrop-filter: blur(16px);
        border: 1px solid rgba(255, 255, 255, 0.06); border-radius: 20px;
        padding: 20px; text-align: center; transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25); margin-bottom: 20px;
        min-height: 155px; display: flex; flex-direction: column; justify-content: space-between;
    }
    .metric-card:hover {
        transform: translateY(-4px); border-color: rgba(255, 255, 255, 0.12);
        box-shadow: 0 15px 40px rgba(99, 102, 241, 0.15); background: rgba(255, 255, 255, 0.05);
    }
    .metric-title { color: #9ca3af; font-size: 0.8rem; font-weight: 600; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.075em; }
    .metric-val { color: #ffffff; font-size: 1.85rem; font-weight: 800; line-height: 1.1; margin-bottom: 6px; }
    .metric-desc { color: #38bdf8; font-size: 0.75rem; font-weight: 600; }
    .aqi-banner {
        border-radius: 24px; padding: 28px; text-align: center; margin-bottom: 28px;
        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.3); border: 1px solid rgba(255, 255, 255, 0.08);
    }
    .aqi-banner-title { font-size: 2.25rem; font-weight: 800; color: #ffffff; letter-spacing: -0.03em; margin: 0; }
    .aqi-banner-desc { font-size: 0.95rem; font-weight: 600; color: rgba(255, 255, 255, 0.95); margin-top: 6px; text-transform: uppercase; letter-spacing: 0.05em; }
    </style>
    """, unsafe_allow_html=True)


def render_alert_banner(risk_level):
    if risk_level is None:
        risk_level = "N/A"
        bg = "linear-gradient(135deg, #6B7280 0%, #4B5563 100%)"
        desc = "Không có dữ liệu chất lượng không khí"
    else:
        bg = RISK_COLORS.get(risk_level, "linear-gradient(135deg, #6B7280 0%, #4B5563 100%)")
        descs = {
            "Good": "Không khí trong lành, rất thích hợp cho hoạt động ngoài trời 🌱",
            "Moderate": "Chất lượng không khí chấp nhận được. Nhóm nhạy cảm nên lưu ý 🌤️",
            "Unhealthy": "Bắt đầu ảnh hưởng sức khỏe. Hạn chế hoạt động gắng sức ngoài trời ⚠️",
            "Hazardous": "Nguy hại cực độ! Tránh ra ngoài và hãy đóng kín cửa sổ 🚨",
        }
        desc = descs.get(risk_level, "N/A")
    st.markdown(f"""
    <div class="aqi-banner" style="background: {bg};">
        <h2 class="aqi-banner-title">Air Quality: {risk_level}</h2>
        <div class="aqi-banner-desc">{desc}</div>
    </div>
    """, unsafe_allow_html=True)


def render_weather_metrics(latest):
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    metrics = [
        ("🌡️ Nhiệt độ", f"{latest['temperature_c']:.1f}°C" if latest['temperature_c'] is not None else "N/A", "Nhiệt độ hiện tại", col1),
        ("💧 Độ ẩm", f"{latest['humidity_pct']:.0f}%" if latest['humidity_pct'] is not None else "N/A", "Độ ẩm không khí", col2),
        ("💨 Tốc độ gió", f"{latest['windspeed_kmh']:.1f} km/h" if latest['windspeed_kmh'] is not None else "N/A", "Sức gió hiện tại", col3),
        ("🫁 PM2.5", f"{latest['pm2_5']:.1f} µg/m³" if latest['pm2_5'] is not None else "N/A", "Bụi siêu mịn PM2.5", col4),
        ("🌫️ PM10", f"{latest['pm10']:.1f} µg/m³" if latest['pm10'] is not None else "N/A", "Bụi mịn PM10", col5),
        ("📊 Chỉ số AQI", f"{latest['aqi_eu']}" if latest['aqi_eu'] is not None else "N/A", "European AQI", col6),
    ]
    for title, val, desc, col in metrics:
        with col:
            st.markdown(f"""
            <div class="metric-card">
                <div><div class="metric-title">{title}</div><div class="metric-val">{val}</div></div>
                <div class="metric-desc">{desc}</div>
            </div>
            """, unsafe_allow_html=True)


def render_aqi_chart(df, city_label):
    if df.empty:
        st.info("Chưa có dữ liệu lịch sử")
        return
    fig = px.line(df, x="timestamp_local", y="aqi_eu",
                  title=f"Chỉ số AQI (European Standard) — {city_label}",
                  labels={"timestamp_local": "Thời gian", "aqi_eu": "AQI"})
    fig.add_hrect(y0=0, y1=20, fillcolor="#10B981", opacity=0.04, annotation_text="Good", annotation_position="right", annotation_font=dict(color="#10B981"))
    fig.add_hrect(y0=20, y1=50, fillcolor="#F59E0B", opacity=0.04, annotation_text="Moderate", annotation_position="right", annotation_font=dict(color="#F59E0B"))
    fig.add_hrect(y0=50, y1=100, fillcolor="#EF4444", opacity=0.04, annotation_text="Unhealthy", annotation_position="right", annotation_font=dict(color="#EF4444"))
    fig.add_hrect(y0=100, y1=200, fillcolor="#8B5CF6", opacity=0.04, annotation_text="Hazardous", annotation_position="right", annotation_font=dict(color="#8B5CF6"))
    fig.update_traces(line=dict(width=3, color="#6366F1"), mode="lines")
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e5e7eb", family="Plus Jakarta Sans"),
        title=dict(font=dict(size=16, color="#ffffff")),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickfont=dict(color="#9ca3af")),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickfont=dict(color="#9ca3af"),
                   range=[0, max(df["aqi_eu"].max() * 1.2, 50)] if not df["aqi_eu"].isna().all() else [0, 100]),
        height=380, hovermode="x unified", margin=dict(l=20, r=20, t=60, b=20)
    )
    st.plotly_chart(fig, use_container_width=True)


def render_temperature_chart(df):
    if df.empty:
        st.info("Chưa có dữ liệu")
        return
    fig = px.line(df, x="timestamp_local", y="temperature_c", title="Biểu đồ Nhiệt độ (°C)",
                  labels={"timestamp_local": "Thời gian", "temperature_c": "Nhiệt độ (°C)"})
    fig.update_traces(line=dict(width=3, color="#FF8C00"))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e5e7eb", family="Plus Jakarta Sans"),
        title=dict(font=dict(size=16, color="#ffffff")),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickfont=dict(color="#9ca3af")),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickfont=dict(color="#9ca3af")),
        height=350, hovermode="x unified", margin=dict(l=20, r=20, t=50, b=20)
    )
    st.plotly_chart(fig, use_container_width=True)


def render_humidity_chart(df):
    if df.empty:
        st.info("Chưa có dữ liệu")
        return
    fig = px.line(df, x="timestamp_local", y="humidity_pct", title="Biểu đồ Độ ẩm (%)",
                  labels={"timestamp_local": "Thời gian", "humidity_pct": "Độ ẩm (%)"})
    fig.update_traces(line=dict(width=3, color="#00BFFF"))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e5e7eb", family="Plus Jakarta Sans"),
        title=dict(font=dict(size=16, color="#ffffff")),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickfont=dict(color="#9ca3af")),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickfont=dict(color="#9ca3af"), range=[0, 100]),
        height=350, hovermode="x unified", margin=dict(l=20, r=20, t=50, b=20)
    )
    st.plotly_chart(fig, use_container_width=True)


def render_forecast_alerts(alerts_df):
    if alerts_df.empty:
        st.markdown("""
        <div style="background: rgba(16, 185, 129, 0.1); border: 1px solid rgba(16, 185, 129, 0.2);
                    border-radius: 16px; padding: 18px; text-align: center; color: #10B981; font-weight: 600; margin-top: 10px;">
            🌱 Không có cảnh báo ô nhiễm không khí nào. Chất lượng không khí rất trong lành!
        </div>
        """, unsafe_allow_html=True)
        return
    st.markdown("<h3 style='margin-top:20px;margin-bottom:15px;'>Cảnh báo chất lượng không khí nguy hại</h3>", unsafe_allow_html=True)
    for _, alert in alerts_df.iterrows():
        bg = RISK_COLORS.get(alert.get("health_risk", ""), "linear-gradient(135deg, #6B7280 0%, #4B5563 100%)")
        hex_c = RISK_HEX.get(alert.get("health_risk", ""), "#9CA3AF")
        msg = str(alert.get("message", "")) if pd.notna(alert.get("message")) else ""
        lead = alert.get("lead_time_hours", "?")
        st.markdown(f"""
        <div style="background-color: rgba(30,30,50,0.5); border-left: 6px solid {hex_c}; padding: 12px 18px;
                    margin-bottom: 12px; border-radius: 4px 16px 16px 4px; box-shadow: 0 4px 20px rgba(0,0,0,0.15);
                    display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 15px;">
            <div style="font-size: 1.02rem; color: #f3f4f6; font-weight: 600;">
                {msg} <span style="color: #9ca3af; font-size: 0.85rem; font-weight: 400; margin-left: 10px;">(cảnh báo trước {lead} giờ)</span>
            </div>
            <span style="background: {bg}; padding: 4px 12px; border-radius: 20px; color: #ffffff;
                         font-size: 0.72rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em;">{alert.get('health_risk', 'N/A')}</span>
        </div>
        """, unsafe_allow_html=True)


def render_forecast_accuracy_tab():
    """Tab Forecast Accuracy — điểm nhấn project."""
    st.markdown("### 📊 Forecast Accuracy (MAE by Lead Time)")
    st.markdown("*Chỉ có thể tính được nhờ data lake giữ lại mọi phiên bản forecast.*")

    acc_df = get_forecast_accuracy()
    if acc_df.empty:
        st.info("Chưa có đủ dữ liệu lịch sử để tính accuracy. Hãy chạy pipeline ít nhất vài ngày.")
        return

    # Bảng tổng hợp
    pivot = acc_df.pivot_table(index="city_code", columns="lead_time_bucket", values="mae", aggfunc="mean")
    st.dataframe(pivot.style.format("{:.1f}").background_gradient(cmap="RdYlGn_r"), use_container_width=True)

    # Chart
    fig = px.bar(acc_df, x="lead_time_bucket", y="mae", color="city_code",
                 title="MAE theo Lead Time Bucket",
                 labels={"mae": "MAE (AQI points)", "lead_time_bucket": "Lead Time"},
                 barmode="group")
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e5e7eb", family="Plus Jakarta Sans"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        height=400, margin=dict(l=20, r=20, t=60, b=20)
    )
    st.plotly_chart(fig, use_container_width=True)


# ============================================================
# MAIN APP
# ============================================================
def main():
    st.set_page_config(page_title="Weather & AQI Lakehouse", page_icon="🌤️", layout="wide")
    inject_custom_css()

    st.markdown(f"""
    <div style="margin-bottom: 25px;">
        <h1 style="margin: 0; font-size: 2.75rem;">🌤️ Weather & AQI Dashboard</h1>
        <div style="color: #9ca3af; font-size: 0.9rem; margin-top: 4px;">
            Hệ thống Lakehouse theo dõi chất lượng không khí 63 tỉnh thành Việt Nam —
            Cập nhật: <strong>{datetime.now().strftime('%H:%M:%S %d/%m/%Y')}</strong>
            &nbsp;|&nbsp; Powered by <strong>Apache Iceberg + Trino</strong>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Sidebar
    with st.sidebar:
        st.markdown("<h2 style='font-size: 1.5rem; margin-bottom: 20px;'>⚙️ Cấu hình</h2>", unsafe_allow_html=True)

        city_map = get_all_cities()
        if not city_map:
            st.warning("⚠️ Chưa tìm thấy dữ liệu trong Iceberg. Vui lòng đảm bảo pipeline đang chạy.")
            return

        city_codes = list(city_map.keys())
        selected = st.selectbox("Chọn khu vực", options=city_codes,
                                format_func=lambda x: city_map.get(x, x), index=0)
        time_range = st.radio("Phạm vi thời gian", options=[24, 48, 120],
                              format_func=lambda x: {24: "24 giờ", 48: "48 giờ", 120: "5 ngày"}[x])
        st.divider()
        st.markdown(f"""
        <div style="font-size: 0.8rem; color: #6b7280; text-align: center;">
            Tự động tải lại sau {REFRESH_INTERVAL // 60} phút<br>
            Stack: Iceberg · Nessie · Trino · MinIO
        </div>
        """, unsafe_allow_html=True)

    # Tabs
    tab1, tab2, tab3 = st.tabs(["📍 Real-time", "📊 Forecast Accuracy", "📋 Daily Summary"])

    with tab1:
        latest = get_latest_metrics(selected)
        if latest is None:
            st.warning(f"Chưa có dữ liệu cho {city_map.get(selected, selected)}")
        else:
            render_alert_banner(latest.get("health_risk"))
            render_weather_metrics(latest)
            st.divider()
            df = get_metrics_range(selected, hours=time_range)
            if not df.empty:
                try:
                    df["timestamp_local"] = pd.to_datetime(df["timestamp_local"])
                except Exception:
                    pass
            st.markdown("<div class='glass-card' style='padding:20px;'>", unsafe_allow_html=True)
            render_aqi_chart(df, city_map.get(selected, selected))
            st.markdown("</div>", unsafe_allow_html=True)

            col_t, col_h = st.columns(2)
            with col_t:
                st.markdown("<div class='glass-card' style='padding:15px;'>", unsafe_allow_html=True)
                render_temperature_chart(df)
                st.markdown("</div>", unsafe_allow_html=True)
            with col_h:
                st.markdown("<div class='glass-card' style='padding:15px;'>", unsafe_allow_html=True)
                render_humidity_chart(df)
                st.markdown("</div>", unsafe_allow_html=True)

            st.divider()
            alerts_df = get_forecast_alerts(selected)
            render_forecast_alerts(alerts_df)

    with tab2:
        render_forecast_accuracy_tab()

    with tab3:
        st.markdown(f"### 📋 Daily Summary — {city_map.get(selected, selected)}")
        summary_df = get_daily_summary(selected, days=30)
        if not summary_df.empty:
            st.dataframe(
                summary_df[["date_local", "temp_min", "temp_max", "temp_avg",
                            "aqi_min", "aqi_max", "aqi_avg",
                            "hours_unhealthy", "hours_hazardous"]],
                use_container_width=True,
            )
        else:
            st.info("Chưa có daily summary. Hãy chạy pipeline ít nhất 1 ngày.")

    # Auto refresh
    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    main()
