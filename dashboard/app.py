import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import time

# ============================================================
# CONFIG
# ============================================================
DB_CONFIG = {
    "host": os.getenv("DATA_DB_HOST", "localhost"),
    "port": os.getenv("DATA_DB_PORT", "5433"),
    "dbname": os.getenv("DATA_DB_NAME", "weather_aqi"),
    "user": os.getenv("DATA_DB_USER", "datauser"),
    "password": os.getenv("DATA_DB_PASSWORD", "datapass"),
}

# Màu sắc gradients theo risk level (Premium)
RISK_COLORS = {
    "Good": "linear-gradient(135deg, #10B981 0%, #059669 100%)",       # Emerald
    "Moderate": "linear-gradient(135deg, #F59E0B 0%, #D97706 100%)",   # Amber
    "Unhealthy": "linear-gradient(135deg, #EF4444 0%, #DC2626 100%)",  # Red
    "Hazardous": "linear-gradient(135deg, #8B5CF6 0%, #7C3AED 100%)",  # Purple
}

# Màu Hex trơn cho các biểu đồ
RISK_HEX = {
    "Good": "#10B981",
    "Moderate": "#F59E0B",
    "Unhealthy": "#EF4444",
    "Hazardous": "#8B5CF6",
}

CITY_NAMES = {
    "HCM": "Hồ Chí Minh",
    "HN": "Hà Nội",
    "DN": "Đà Nẵng",
    "HP": "Hải Phòng",
    "CT": "Cần Thơ",
    "HUE": "Huế",
    "NT": "Nha Trang",
    "DL": "Đà Lạt",
    "VT": "Vũng Tàu",
    "VINH": "Vinh",
    "QN": "Quy Nhơn"
}

REFRESH_INTERVAL = 300  


# ============================================================
# DATABASE FUNCTIONS
# ============================================================
@st.cache_resource
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def query_df(sql, params=None):
    conn = get_db_connection()
    return pd.read_sql(sql, conn, params=params)


def get_latest_metrics(city_code):
    """Lấy row thời gian thực mới nhất cho 1 thành phố."""
    sql = """
        SELECT * FROM processed.hourly_metrics
        WHERE city_code = %s
          AND timestamp_local <= NOW()
        ORDER BY timestamp_local DESC
        LIMIT 1
    """
    df = query_df(sql, (city_code,))
    return df.iloc[0] if not df.empty else None


def get_metrics_range(city_code, hours=24):
    """Lấy data lịch sử và dự báo tương ứng với khoảng thời gian được chọn."""
    # Lấy lịch sử theo 'hours' đã chọn, và đồng thời lấy dự báo trong tương lai bằng đúng khoảng đó
    # Cập nhật giới hạn tương lai tối đa 120 giờ (5 ngày) do giới hạn của mô hình CAMS Global
    forecast_hours = min(hours, 120)
    sql = """
        SELECT timestamp_local, temperature_c, humidity_pct, 
               windspeed_kmh, pm2_5, pm10, aqi_eu, health_risk
        FROM processed.hourly_metrics
        WHERE city_code = %s
          AND timestamp_local >= NOW() - INTERVAL '%s hours'
          AND timestamp_local <= NOW() + INTERVAL '%s hours'
        ORDER BY timestamp_local
    """
    return query_df(sql, (city_code, hours, forecast_hours))


def get_forecast_alerts(city_code=None):
    """Lấy forecast alerts chưa quá hạn."""
    if city_code:
        sql = """
            SELECT * FROM processed.forecast_alerts
            WHERE forecast_for_time > NOW()
              AND city_code = %s
            ORDER BY forecast_for_time
        """
        return query_df(sql, (city_code,))
    else:
        sql = """
            SELECT * FROM processed.forecast_alerts
            WHERE forecast_for_time > NOW()
            ORDER BY forecast_for_time
            LIMIT 20
        """
        return query_df(sql)


def get_all_cities():
    """Lấy danh sách city có data."""
    sql = """
        SELECT DISTINCT city_code 
        FROM processed.hourly_metrics 
        ORDER BY city_code
    """
    df = query_df(sql)
    return df["city_code"].tolist() if not df.empty else []


# ============================================================
# UI COMPONENTS
# ============================================================
def inject_custom_css():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');

        /* Reset font và hình nền gradient chìm */
        html, body, [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
            font-family: 'Plus Jakarta Sans', sans-serif;
            background: linear-gradient(135deg, #0b0f19 0%, #111827 50%, #1e1b4b 100%);
            color: #f3f4f6 !important;
        }

        /* Ẩn header mặc định và tinh chỉnh sidebar */
        [data-testid="stHeader"] {
            background-color: rgba(0, 0, 0, 0);
        }
        
        [data-testid="stSidebar"] {
            background-color: rgba(17, 24, 39, 0.85) !important;
            border-right: 1px solid rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(10px);
        }

        /* Tinh chỉnh tiêu đề và caption */
        h1, h2, h3 {
            color: #ffffff !important;
            font-weight: 700 !important;
            letter-spacing: -0.02em !important;
        }

        /* Premium Glassmorphic Cards */
        .glass-card {
            background: rgba(255, 255, 255, 0.03);
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            border: 1px solid rgba(255, 255, 255, 0.06);
            border-radius: 20px;
            padding: 24px;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25);
            margin-bottom: 20px;
        }

        .glass-card:hover {
            transform: translateY(-4px);
            border-color: rgba(255, 255, 255, 0.12);
            box-shadow: 0 15px 40px rgba(99, 102, 241, 0.15);
            background: rgba(255, 255, 255, 0.05);
        }

        /* Equal-sized Custom Metric Cards */
        .metric-card {
            background: rgba(255, 255, 255, 0.03);
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            border: 1px solid rgba(255, 255, 255, 0.06);
            border-radius: 20px;
            padding: 20px;
            text-align: center;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25);
            margin-bottom: 20px;
            min-height: 155px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }

        .metric-card:hover {
            transform: translateY(-4px);
            border-color: rgba(255, 255, 255, 0.12);
            box-shadow: 0 15px 40px rgba(99, 102, 241, 0.15);
            background: rgba(255, 255, 255, 0.05);
        }

        /* Custom Weather Metrics Grid */
        .metric-title {
            color: #9ca3af;
            font-size: 0.8rem;
            font-weight: 600;
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 0.075em;
        }

        .metric-val {
            color: #ffffff;
            font-size: 1.85rem;
            font-weight: 800;
            line-height: 1.1;
            margin-bottom: 6px;
        }

        .metric-desc {
            color: #38bdf8;
            font-size: 0.75rem;
            font-weight: 600;
        }

        /* Air Quality Big Banner */
        .aqi-banner {
            border-radius: 24px;
            padding: 28px;
            text-align: center;
            margin-bottom: 28px;
            box-shadow: 0 12px 40px rgba(0, 0, 0, 0.3);
            border: 1px solid rgba(255, 255, 255, 0.08);
            transition: all 0.3s ease;
        }

        .aqi-banner-title {
            font-size: 2.25rem;
            font-weight: 800;
            color: #ffffff;
            letter-spacing: -0.03em;
            margin: 0;
        }

        .aqi-banner-desc {
            font-size: 0.95rem;
            font-weight: 600;
            color: rgba(255, 255, 255, 0.95);
            margin-top: 6px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        /* Divider màu neon mờ */
        .stDivider {
            border-bottom: 1px solid rgba(255, 255, 255, 0.08) !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_alert_banner(risk_level):
    """Hiển thị banner trạng thái chất lượng không khí lớn ở đầu trang."""
    if risk_level is None:
        risk_level = "N/A"
        bg_gradient = "linear-gradient(135deg, #6B7280 0%, #4B5563 100%)"
        risk_desc = "Không có dữ liệu chất lượng không khí"
    else:
        bg_gradient = RISK_COLORS.get(risk_level, "linear-gradient(135deg, #6B7280 0%, #4B5563 100%)")
        if risk_level == "Good":
            risk_desc = "Không khí trong lành, rất thích hợp cho hoạt động ngoài trời 🌱"
        elif risk_level == "Moderate":
            risk_desc = "Chất lượng không khí chấp nhận được. Nhóm nhạy cảm nên lưu ý 🌤️"
        elif risk_level == "Unhealthy":
            risk_desc = "Bắt đầu ảnh hưởng sức khỏe. Hạn chế hoạt động gắng sức ngoài trời ⚠️"
        else:
            risk_desc = "Nguy hại cực độ! Tránh ra ngoài và hãy đóng kín cửa sổ 🚨"

    st.markdown(
        f"""
        <div class="aqi-banner" style="background: {bg_gradient};">
            <h2 class="aqi-banner-title">Air Quality: {risk_level}</h2>
            <div class="aqi-banner-desc">{risk_desc}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_weather_metrics(latest):
    """Hiển thị các thông số thời tiết chính dưới dạng 6 glassmorphic cards bằng nhau."""
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    metrics = [
        ("🌡️ Nhiệt độ", f"{latest['temperature_c']:.1f}°C" if latest['temperature_c'] is not None else "N/A", "Nhiệt độ hiện tại", col1),
        ("💧 Độ ẩm", f"{latest['humidity_pct']:.0f}%" if latest['humidity_pct'] is not None else "N/A", "Độ ẩm không khí", col2),
        ("💨 Tốc độ gió", f"{latest['windspeed_kmh']:.1f} km/h" if latest['windspeed_kmh'] is not None else "N/A", "Sức gió hiện tại", col3),
        ("🫁 PM2.5", f"{latest['pm2_5']:.1f} µg/m³" if latest['pm2_5'] is not None else "N/A", "Bụi siêu mịn PM2.5", col4),
        ("🌫️ PM10", f"{latest['pm10']:.1f} µg/m³" if latest['pm10'] is not None else "N/A", "Bụi mịn PM10", col5),
        ("📊 Chỉ số AQI", f"{latest['aqi_eu']}" if latest['aqi_eu'] is not None else "N/A", "Chỉ số chất lượng không khí", col6),
    ]

    for title, val, desc, col in metrics:
        with col:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div>
                        <div class="metric-title">{title}</div>
                        <div class="metric-val">{val}</div>
                    </div>
                    <div class="metric-desc">{desc}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_aqi_chart(df, city_label):
    """Biểu đồ đường AQI bóng bẩy, đồng bộ theme tối."""
    if df.empty:
        st.info("Chưa có dữ liệu lịch sử")
        return

    fig = px.line(
        df,
        x="timestamp_local",
        y="aqi_eu",
        title=f"Chỉ số AQI (European Standard) — {city_label}",
        labels={"timestamp_local": "Thời gian", "aqi_eu": "AQI"},
    )

    # Thêm các dải màu hiển thị mức độ nguy hiểm
    fig.add_hrect(y0=0, y1=20, fillcolor="#10B981", opacity=0.04,
                  annotation_text="Good", annotation_position="right", annotation_font=dict(color="#10B981"))
    fig.add_hrect(y0=20, y1=50, fillcolor="#F59E0B", opacity=0.04,
                  annotation_text="Moderate", annotation_position="right", annotation_font=dict(color="#F59E0B"))
    fig.add_hrect(y0=50, y1=100, fillcolor="#EF4444", opacity=0.04,
                  annotation_text="Unhealthy", annotation_position="right", annotation_font=dict(color="#EF4444"))
    fig.add_hrect(y0=100, y1=200, fillcolor="#8B5CF6", opacity=0.04,
                  annotation_text="Hazardous", annotation_position="right", annotation_font=dict(color="#8B5CF6"))

    # Làm đẹp layout theo theme dark tối giản
    fig.update_traces(
        line=dict(width=3, color="#6366F1"),  # Màu Indigo bóng bẩy
        mode="lines",
        hoverinfo="x+y"
    )
    
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e5e7eb", family="Plus Jakarta Sans"),
        title=dict(font=dict(size=16, weight="bold", color="#ffffff")),
        xaxis=dict(
            gridcolor="rgba(255, 255, 255, 0.05)",
            linecolor="rgba(255, 255, 255, 0.1)",
            tickfont=dict(color="#9ca3af")
        ),
        yaxis=dict(
            gridcolor="rgba(255, 255, 255, 0.05)",
            linecolor="rgba(255, 255, 255, 0.1)",
            tickfont=dict(color="#9ca3af"),
            range=[0, max(df["aqi_eu"].max() * 1.2, 50)]
        ),
        height=380,
        hovermode="x unified",
        margin=dict(l=20, r=20, t=60, b=20)
    )

    st.plotly_chart(fig, use_container_width=True)


def render_temperature_chart(df):
    """Biểu đồ đường Nhiệt độ, đồng bộ theme tối."""
    if df.empty:
        st.info("Chưa có dữ liệu lịch sử")
        return

    fig = px.line(
        df,
        x="timestamp_local",
        y="temperature_c",
        title="Biểu đồ Nhiệt độ (°C)",
        labels={"timestamp_local": "Thời gian", "temperature_c": "Nhiệt độ (°C)"},
    )

    fig.update_traces(
        line=dict(width=3, color="#FF8C00"),
        mode="lines",
        hoverinfo="x+y"
    )
    
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e5e7eb", family="Plus Jakarta Sans"),
        title=dict(font=dict(size=16, weight="bold", color="#ffffff")),
        xaxis=dict(
            gridcolor="rgba(255, 255, 255, 0.05)",
            linecolor="rgba(255, 255, 255, 0.1)",
            tickfont=dict(color="#9ca3af")
        ),
        yaxis=dict(
            gridcolor="rgba(255, 255, 255, 0.05)",
            linecolor="rgba(255, 255, 255, 0.1)",
            tickfont=dict(color="#9ca3af")
        ),
        height=350,
        hovermode="x unified",
        margin=dict(l=20, r=20, t=50, b=20)
    )

    st.plotly_chart(fig, use_container_width=True)


def render_humidity_chart(df):
    """Biểu đồ đường Độ ẩm, đồng bộ theme tối."""
    if df.empty:
        st.info("Chưa có dữ liệu lịch sử")
        return

    fig = px.line(
        df,
        x="timestamp_local",
        y="humidity_pct",
        title="Biểu đồ Độ ẩm (%)",
        labels={"timestamp_local": "Thời gian", "humidity_pct": "Độ ẩm (%)"},
    )

    fig.update_traces(
        line=dict(width=3, color="#00BFFF"),
        mode="lines",
        hoverinfo="x+y"
    )
    
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e5e7eb", family="Plus Jakarta Sans"),
        title=dict(font=dict(size=16, weight="bold", color="#ffffff")),
        xaxis=dict(
            gridcolor="rgba(255, 255, 255, 0.05)",
            linecolor="rgba(255, 255, 255, 0.1)",
            tickfont=dict(color="#9ca3af")
        ),
        yaxis=dict(
            gridcolor="rgba(255, 255, 255, 0.05)",
            linecolor="rgba(255, 255, 255, 0.1)",
            tickfont=dict(color="#9ca3af"),
            range=[0, 100]
        ),
        height=350,
        hovermode="x unified",
        margin=dict(l=20, r=20, t=50, b=20)
    )

    st.plotly_chart(fig, use_container_width=True)


def render_forecast_alerts(alerts_df):
    """Hiển thị cảnh báo dự báo ô nhiễm dạng card neon dùng CSS variables tương thích Light/Dark theme."""
    if alerts_df.empty:
        st.markdown(
            """
            <div style="
                background: rgba(16, 185, 129, 0.1);
                border: 1px solid rgba(16, 185, 129, 0.2);
                border-radius: 16px;
                padding: 18px;
                text-align: center;
                color: #10B981;
                font-weight: 600;
                margin-top: 10px;
            ">
                🌱 Không có cảnh báo ô nhiễm không khí nào trong tương lai. Chất lượng không khí rất trong lành!
            </div>
            """,
            unsafe_allow_html=True
        )
        return

    st.markdown("<h3 style='margin-top:20px;margin-bottom:15px; color: var(--text-color);'>Cảnh báo chất lượng không khí nguy hại</h3>", unsafe_allow_html=True)

    for _, alert in alerts_df.iterrows():
        bg_gradient = RISK_COLORS.get(alert["health_risk"], "linear-gradient(135deg, #6B7280 0%, #4B5563 100%)")
        hex_color = RISK_HEX.get(alert["health_risk"], "#9CA3AF")
        
        clean_msg = alert["message"]
        if pd.isnull(clean_msg):
            clean_msg = ""
        else:
            clean_msg = str(clean_msg)
            for risk in ["Good", "Moderate", "Unhealthy", "Hazardous"]:
                clean_msg = clean_msg.replace(f" ({risk})", "")
                clean_msg = clean_msg.replace(f"({risk})", "")
        
        lead_time = alert["lead_time_hours"]
        
        st.markdown(
            f"""
            <div style="
                background-color: var(--secondary-background-color);
                border-left: 6px solid {hex_color};
                padding: 12px 18px;
                margin-bottom: 12px;
                border-radius: 4px 16px 16px 4px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
                display: flex;
                justify-content: space-between;
                align-items: center;
                flex-wrap: wrap;
                gap: 15px;
            ">
                <div style="font-size: 1.02rem; color: var(--text-color); font-weight: 600;">
                    {clean_msg} <span style="color: #9ca3af; font-size: 0.85rem; font-weight: 400; margin-left: 10px;">(cảnh báo trước {lead_time} giờ)</span>
                </div>
                <span style="
                    background: {bg_gradient};
                    padding: 4px 12px;
                    border-radius: 20px;
                    color: #ffffff;
                    font-size: 0.72rem;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
                ">{alert['health_risk']}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ============================================================
# MAIN APP
# ============================================================
def main():
    st.set_page_config(
        page_title="Weather & AQI Monitor",
        page_icon="🌤️",
        layout="wide",
    )

    # Khởi tạo CSS tùy biến cho giao diện tối và đẹp mắt
    inject_custom_css()

    st.markdown(
        f"""
        <div style="margin-bottom: 25px;">
            <h1 style="margin: 0; font-size: 2.75rem;">🌤️ Weather & AQI Dashboard</h1>
            <div style="color: #9ca3af; font-size: 0.9rem; margin-top: 4px;">
                Hệ thống theo dõi chất lượng không khí & dự báo thời tiết tự động — Cập nhật lần cuối: <strong>{datetime.now().strftime('%H:%M:%S %d/%m/%Y')}</strong>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    # Sidebar: cấu hình bộ lọc chọn thành phố + khoảng thời gian
    with st.sidebar:
        st.markdown("<h2 style='font-size: 1.5rem; margin-bottom: 20px;'>⚙️ Cấu hình hệ thống</h2>", unsafe_allow_html=True)

        cities = get_all_cities()
        if not cities:
            st.warning("⚠️ Chưa tìm thấy dữ liệu. Vui lòng đảm bảo các Pipeline Airflow đang chạy và đẩy dữ liệu vào DB.")
            return

        selected_city_code = st.selectbox(
            "Chọn khu vực theo dõi",
            options=cities,
            format_func=lambda x: CITY_NAMES.get(x, x),
            index=0
        )

        time_range = st.radio(
            "Phạm vi thời gian biểu đồ",
            options=[24, 48, 120],
            format_func=lambda x: {24: "24 giờ", 48: "48 giờ", 120: "5 ngày"}[x],
        )

        st.divider()
        st.markdown(
            f"""
            <div style="font-size: 0.8rem; color: #6b7280; text-align: center;">
                Tự động tải lại sau {REFRESH_INTERVAL // 60} phút<br>
                Mô hình dữ liệu: Copernicus CAMS & Open-Meteo
            </div>
            """,
            unsafe_allow_html=True
        )

    # Tìm thông tin bản ghi thời gian thực mới nhất cho thành phố được chọn
    latest = get_latest_metrics(selected_city_code)

    if latest is None:
        st.warning(f"Chưa có dữ liệu thời tiết hợp lệ cho khu vực {CITY_NAMES.get(selected_city_code, selected_city_code)}")
        return

    # === Banner Cảnh báo Chất lượng không khí (Lớn) ===
    render_alert_banner(latest["health_risk"])

    # === Hệ thống Cards thông số thời tiết chi tiết ===
    render_weather_metrics(latest)

    st.markdown("<div style='margin: 15px 0;'></div>", unsafe_allow_html=True)
    st.divider()

    # === Đồ thị trực quan hóa ===
    df = get_metrics_range(selected_city_code, hours=time_range)
    if not df.empty:
        try:
            df["timestamp_local"] = pd.to_datetime(df["timestamp_local"]).dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
        except Exception:
            pass

    st.markdown("<div class='glass-card' style='padding:20px;'>", unsafe_allow_html=True)
    render_aqi_chart(df, CITY_NAMES.get(selected_city_code, selected_city_code))
    st.markdown("</div>", unsafe_allow_html=True)

    col_temp, col_hum = st.columns(2)

    with col_temp:
        st.markdown("<div class='glass-card' style='padding:15px;'>", unsafe_allow_html=True)
        render_temperature_chart(df)
        st.markdown("</div>", unsafe_allow_html=True)

    with col_hum:
        st.markdown("<div class='glass-card' style='padding:15px;'>", unsafe_allow_html=True)
        render_humidity_chart(df)
        st.markdown("</div>", unsafe_allow_html=True)

    st.divider()

    # === Hiển thị danh sách cảnh báo ô nhiễm trong tương lai (nếu có) ===
    alerts_df = get_forecast_alerts(selected_city_code)
    render_forecast_alerts(alerts_df)

    # === Tự động refresh trang định kỳ ===
    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    main()
