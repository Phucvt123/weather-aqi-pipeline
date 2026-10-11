import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# ============================================================
# CONFIG — Trino connection & styling tokens
# ============================================================
TRINO_CONFIG = {
    "host": os.getenv("TRINO_HOST", "localhost"),
    "port": int(os.getenv("TRINO_PORT", "8083")),
    "catalog": os.getenv("TRINO_CATALOG", "iceberg"),
}

RISK_THEMES = {
    "Good": {
        "color": "#10B981",
        "bg_gradient": "linear-gradient(135deg, rgba(16, 185, 129, 0.15) 0%, rgba(5, 150, 105, 0.08) 100%)",
        "border": "rgba(16, 185, 129, 0.35)",
        "badge_bg": "#10B981",
        "desc": "Không khí trong lành, rất thích hợp cho các hoạt động ngoài trời.",
    },
    "Moderate": {
        "color": "#F59E0B",
        "bg_gradient": "linear-gradient(135deg, rgba(245, 158, 11, 0.15) 0%, rgba(217, 119, 6, 0.08) 100%)",
        "border": "rgba(245, 158, 11, 0.35)",
        "badge_bg": "#F59E0B",
        "desc": "Chất lượng không khí ở mức chấp nhận được. Nhóm người nhạy cảm nên hạn chế hoạt động mạnh.",
    },
    "Unhealthy": {
        "color": "#EF4444",
        "bg_gradient": "linear-gradient(135deg, rgba(239, 68, 68, 0.15) 0%, rgba(220, 38, 38, 0.08) 100%)",
        "border": "rgba(239, 68, 68, 0.35)",
        "badge_bg": "#EF4444",
        "desc": "Chất lượng không khí bắt đầu ảnh hưởng sức khỏe. Người dân nên hạn chế ra ngoài khi không cần thiết.",
    },
    "Hazardous": {
        "color": "#8B5CF6",
        "bg_gradient": "linear-gradient(135deg, rgba(139, 92, 246, 0.2) 0%, rgba(124, 58, 237, 0.1) 100%)",
        "border": "rgba(139, 92, 246, 0.4)",
        "badge_bg": "#8B5CF6",
        "desc": "Mức độ nguy hại cao! Tránh hoàn toàn các hoạt động ngoài trời và giữ cửa sổ kín.",
    },
}

DEFAULT_THEME = {
    "color": "#9CA3AF",
    "bg_gradient": "linear-gradient(135deg, rgba(107, 114, 128, 0.1) 0%, rgba(75, 85, 99, 0.05) 100%)",
    "border": "rgba(107, 114, 128, 0.25)",
    "badge_bg": "#6B7280",
    "desc": "Không có thông tin đánh giá rủi ro cho khu vực này.",
}


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
        st.error(f"Lỗi truy vấn Trino: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_all_cities():
    """Lấy danh sách tỉnh thành có dữ liệu từ Silver."""
    df = query_df("""
        SELECT DISTINCT city_code, city_name
        FROM silver.hourly_metrics
        ORDER BY city_name
    """)
    if not df.empty:
        return dict(zip(df["city_code"], df["city_name"]))
    return {}


def get_latest_metrics(city_code):
    """Bản ghi thời gian thực mới nhất cho 1 tỉnh thành."""
    df = query_df(f"""
        SELECT * FROM silver.hourly_metrics
        WHERE city_code = '{city_code}'
          AND timestamp_local <= current_timestamp
        ORDER BY timestamp_local DESC
        LIMIT 1
    """)
    return df.iloc[0] if not df.empty else None


def get_metrics_range(city_code, hours=24):
    """Lấy dữ liệu chuỗi thời gian lịch sử và dự báo."""
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
    """Lấy cảnh báo ô nhiễm không khí từ Gold layer."""
    if city_code:
        return query_df(f"""
            SELECT * FROM gold.forecast_alerts
            WHERE forecast_for_time > current_timestamp
              AND city_code = '{city_code}'
            ORDER BY forecast_for_time
        """)
    return query_df("""
        SELECT * FROM gold.forecast_alerts
        WHERE forecast_for_time > current_timestamp
        ORDER BY forecast_for_time
        LIMIT 20
    """)


def get_daily_summary(city_code, days=30):
    """Lấy tổng hợp ngày từ Gold layer."""
    return query_df(f"""
        SELECT * FROM gold.city_daily_summary
        WHERE city_code = '{city_code}'
          AND date_local >= current_date - INTERVAL '{days}' DAY
        ORDER BY date_local DESC
    """)


def get_forecast_accuracy():
    """Lấy thống kê sai số dự báo MAE/Bias từ Gold layer."""
    return query_df("""
        SELECT city_code, lead_time_bucket, mae, bias, rmse, sample_count
        FROM gold.forecast_accuracy
        ORDER BY lead_time_bucket, city_code
    """)


# ============================================================
# DESIGN SYSTEM & STYLING
# ============================================================
def inject_custom_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');
    
    html, body, [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
        font-family: 'Plus Jakarta Sans', sans-serif;
        background: #090d16;
        color: #f3f4f6 !important;
    }
    
    [data-testid="stHeader"] {
        background-color: transparent !important;
    }
    
    [data-testid="stSidebar"] {
        background-color: #0d131f !important;
        border-right: 1px solid rgba(255, 255, 255, 0.06);
    }
    
    h1, h2, h3, h4 {
        color: #ffffff !important;
        font-weight: 700 !important;
        letter-spacing: -0.02em !important;
    }
    
    /* Header Container */
    .app-header {
        margin-bottom: 24px;
        padding-bottom: 16px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.06);
    }
    .app-title {
        font-size: 2.2rem;
        font-weight: 800;
        margin: 0;
        letter-spacing: -0.03em;
        color: #ffffff;
    }
    .app-subtitle {
        color: #9ca3af;
        font-size: 0.88rem;
        margin-top: 6px;
        line-height: 1.5;
    }
    .app-tag {
        display: inline-block;
        background: rgba(56, 189, 248, 0.1);
        color: #38bdf8;
        border: 1px solid rgba(56, 189, 248, 0.25);
        border-radius: 6px;
        padding: 2px 8px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-left: 8px;
    }
    
    /* Metric Cards */
    .metric-card {
        background: rgba(17, 24, 39, 0.7);
        border: 1px solid rgba(255, 255, 255, 0.07);
        border-radius: 14px;
        padding: 16px 18px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.2);
        transition: all 0.2s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .metric-card:hover {
        border-color: rgba(56, 189, 248, 0.3);
        transform: translateY(-2px);
    }
    .metric-title {
        color: #9ca3af;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-bottom: 8px;
    }
    .metric-val {
        color: #ffffff;
        font-size: 1.75rem;
        font-weight: 800;
        line-height: 1.1;
        margin-bottom: 4px;
    }
    .metric-desc {
        color: #64748b;
        font-size: 0.75rem;
        font-weight: 500;
    }
    
    /* AQI Status Banner */
    .aqi-banner {
        border-radius: 16px;
        padding: 22px 24px;
        margin-bottom: 24px;
        box-shadow: 0 8px 30px rgba(0, 0, 0, 0.25);
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 20px;
        flex-wrap: wrap;
    }
    .aqi-banner-left {
        display: flex;
        align-items: center;
        gap: 16px;
    }
    .aqi-dot {
        width: 14px;
        height: 14px;
        border-radius: 50%;
        display: inline-block;
        flex-shrink: 0;
    }
    .aqi-status-text {
        font-size: 1.5rem;
        font-weight: 800;
        color: #ffffff;
        letter-spacing: -0.02em;
        margin: 0;
    }
    .aqi-desc-text {
        font-size: 0.88rem;
        color: #d1d5db;
        margin-top: 4px;
    }
    .aqi-badge {
        padding: 6px 16px;
        border-radius: 9999px;
        font-size: 0.8rem;
        font-weight: 700;
        color: #ffffff;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    /* Content Glass Card */
    .chart-panel {
        background: rgba(17, 24, 39, 0.6);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 16px;
        padding: 20px;
        margin-bottom: 20px;
    }
    
    /* Alert Item */
    .alert-card {
        background: rgba(17, 24, 39, 0.6);
        border-radius: 12px;
        padding: 14px 18px;
        margin-bottom: 10px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 16px;
        flex-wrap: wrap;
    }
    </style>
    """, unsafe_allow_html=True)


# ============================================================
# UI RENDERING FUNCTIONS
# ============================================================
def render_header():
    current_time = datetime.now().strftime("%H:%M - %d/%m/%Y")
    st.markdown(f"""
    <div class="app-header">
        <div class="app-title">
            Vietnam Weather & AQI Lakehouse
            <span class="app-tag">Live 63 Tỉnh Thành</span>
        </div>
        <div class="app-subtitle">
            Hạ tầng Medallion Lakehouse lưu trữ chuẩn hóa trên MinIO S3 & Apache Iceberg · Truy vấn tức thì với Trino<br>
            Thời điểm truy vấn: <strong>{current_time}</strong> (UTC+7)
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_alert_banner(risk_level):
    theme = RISK_THEMES.get(risk_level, DEFAULT_THEME)
    label = risk_level if risk_level else "Không xác định"
    
    st.markdown(f"""
    <div class="aqi-banner" style="background: {theme['bg_gradient']}; border: 1px solid {theme['border']};">
        <div class="aqi-banner-left">
            <span class="aqi-dot" style="background-color: {theme['color']}; box-shadow: 0 0 12px {theme['color']};"></span>
            <div>
                <div class="aqi-status-text">Chất lượng không khí: {label}</div>
                <div class="aqi-desc-text">{theme['desc']}</div>
            </div>
        </div>
        <div>
            <span class="aqi-badge" style="background-color: {theme['badge_bg']};">{label}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_weather_metrics(latest):
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    
    metrics = [
        ("Nhiệt độ", f"{latest['temperature_c']:.1f}°C" if pd.notna(latest['temperature_c']) else "—", "Nhiệt độ hiện tại", c1),
        ("Độ ẩm", f"{latest['humidity_pct']:.0f}%" if pd.notna(latest['humidity_pct']) else "—", "Độ ẩm không khí", c2),
        ("Tốc độ gió", f"{latest['windspeed_kmh']:.1f} km/h" if pd.notna(latest['windspeed_kmh']) else "—", "Vận tốc gió 10m", c3),
        ("Bụi mịn PM2.5", f"{latest['pm2_5']:.1f} µg/m³" if pd.notna(latest['pm2_5']) else "—", "Nồng độ bụi PM2.5", c4),
        ("Bụi mịn PM10", f"{latest['pm10']:.1f} µg/m³" if pd.notna(latest['pm10']) else "—", "Nồng độ bụi PM10", c5),
        ("Chỉ số AQI", f"{int(latest['aqi_eu'])}" if pd.notna(latest['aqi_eu']) else "—", "Chuẩn châu Âu (EU AQI)", c6),
    ]
    
    for title, val, desc, col in metrics:
        with col:
            st.markdown(f"""
            <div class="metric-card">
                <div>
                    <div class="metric-title">{title}</div>
                    <div class="metric-val">{val}</div>
                </div>
                <div class="metric-desc">{desc}</div>
            </div>
            """, unsafe_allow_html=True)


def render_aqi_chart(df, city_label):
    if df.empty:
        st.info("Chưa có dữ liệu lịch sử hoặc dự báo cho khu vực này.")
        return

    fig = px.line(
        df, x="timestamp_local", y="aqi_eu",
        title=f"Diễn biến & Dự báo chỉ số AQI — {city_label}",
        labels={"timestamp_local": "Thời gian (Giờ địa phương)", "aqi_eu": "AQI"}
    )
    
    # Dải ngưỡng tham chiếu an toàn theo chuẩn EU
    fig.add_hrect(y0=0, y1=20, fillcolor="#10B981", opacity=0.05, annotation_text="Tốt (0-20)", annotation_position="top right", annotation_font=dict(color="#10B981", size=10))
    fig.add_hrect(y0=20, y1=50, fillcolor="#F59E0B", opacity=0.05, annotation_text="Trung bình (20-50)", annotation_position="top right", annotation_font=dict(color="#F59E0B", size=10))
    fig.add_hrect(y0=50, y1=100, fillcolor="#EF4444", opacity=0.05, annotation_text="Kém (50-100)", annotation_position="top right", annotation_font=dict(color="#EF4444", size=10))
    fig.add_hrect(y0=100, y1=200, fillcolor="#8B5CF6", opacity=0.05, annotation_text="Nguy hại (>100)", annotation_position="top right", annotation_font=dict(color="#8B5CF6", size=10))
    
    fig.update_traces(line=dict(width=2.5, color="#38BDF8"), mode="lines")
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#9ca3af", family="Plus Jakarta Sans"),
        title=dict(font=dict(size=15, color="#f3f4f6")),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickfont=dict(color="#9ca3af")),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickfont=dict(color="#9ca3af")),
        height=380,
        hovermode="x unified",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    st.plotly_chart(fig, use_container_width=True)


def render_environment_charts(df):
    if df.empty:
        return

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("<div class='chart-panel'>", unsafe_allow_html=True)
        fig_temp = px.line(
            df, x="timestamp_local", y="temperature_c",
            title="Biểu đồ Nhiệt độ (°C)",
            labels={"timestamp_local": "Thời gian", "temperature_c": "Nhiệt độ (°C)"}
        )
        fig_temp.update_traces(line=dict(width=2.5, color="#FB923C"))
        fig_temp.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#9ca3af", family="Plus Jakarta Sans"),
            title=dict(font=dict(size=14, color="#f3f4f6")),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            height=300, hovermode="x unified", margin=dict(l=10, r=10, t=40, b=10)
        )
        st.plotly_chart(fig_temp, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with c2:
        st.markdown("<div class='chart-panel'>", unsafe_allow_html=True)
        fig_hum = px.line(
            df, x="timestamp_local", y="humidity_pct",
            title="Biểu đồ Độ ẩm (%)",
            labels={"timestamp_local": "Thời gian", "humidity_pct": "Độ ẩm (%)"}
        )
        fig_hum.update_traces(line=dict(width=2.5, color="#38BDF8"))
        fig_hum.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#9ca3af", family="Plus Jakarta Sans"),
            title=dict(font=dict(size=14, color="#f3f4f6")),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)", range=[0, 100]),
            height=300, hovermode="x unified", margin=dict(l=10, r=10, t=40, b=10)
        )
        st.plotly_chart(fig_hum, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)


def render_forecast_alerts(alerts_df):
    if alerts_df.empty:
        st.markdown("""
        <div style="background: rgba(16, 185, 129, 0.08); border: 1px solid rgba(16, 185, 129, 0.2);
                    border-radius: 12px; padding: 14px 18px; color: #10B981; font-weight: 500; font-size: 0.9rem;">
            Chất lượng không khí trong ngưỡng an toàn — Không có cảnh báo ô nhiễm trong các giờ tới.
        </div>
        """, unsafe_allow_html=True)
        return

    st.markdown("<h4 style='margin-top: 16px; margin-bottom: 12px;'>Cảnh báo dự báo ô nhiễm không khí</h4>", unsafe_allow_html=True)
    for _, alert in alerts_df.iterrows():
        risk = alert.get("health_risk", "Moderate")
        theme = RISK_THEMES.get(risk, DEFAULT_THEME)
        msg = str(alert.get("message", "")) if pd.notna(alert.get("message")) else "Cảnh báo chất lượng không khí"
        lead = alert.get("lead_time_hours", "—")
        
        st.markdown(f"""
        <div class="alert-card" style="border-left: 4px solid {theme['color']};">
            <div>
                <span style="color: #f3f4f6; font-weight: 600; font-size: 0.95rem;">{msg}</span>
                <span style="color: #64748b; font-size: 0.8rem; margin-left: 8px;">(Trước {lead} giờ)</span>
            </div>
            <span style="background: {theme['badge_bg']}; padding: 3px 10px; border-radius: 9999px; color: #ffffff; font-size: 0.72rem; font-weight: 700; text-transform: uppercase;">
                {risk}
            </span>
        </div>
        """, unsafe_allow_html=True)


def render_accuracy_tab():
    st.markdown("#### Đánh giá độ chính xác dự báo (Forecast Accuracy)")
    st.caption("Chỉ số MAE và sai số phân tích qua từng mốc thời gian (Lead Time Bucket), khả thi nhờ Apache Iceberg lưu trữ toàn bộ phiên bản dự báo.")

    acc_df = get_forecast_accuracy()
    if acc_df.empty:
        st.info("Chưa có đủ số lượng bản ghi lịch sử để tính toán sai số. Cần pipeline tích lũy dữ liệu sau nhiều chu kỳ.")
        return

    # Bảng MAE pivot
    pivot = acc_df.pivot_table(index="city_code", columns="lead_time_bucket", values="mae", aggfunc="mean")
    st.dataframe(pivot.style.format("{:.1f}").background_gradient(cmap="Blues"), use_container_width=True)

    fig = px.bar(
        acc_df, x="lead_time_bucket", y="mae", color="city_code",
        title="Sai số tuyệt đối trung bình (MAE) theo Lead Time",
        labels={"mae": "MAE (Điểm AQI)", "lead_time_bucket": "Khoảng thời gian dự báo"},
        barmode="group"
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#9ca3af", family="Plus Jakarta Sans"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        height=380, margin=dict(l=10, r=10, t=50, b=10)
    )
    st.plotly_chart(fig, use_container_width=True)


def render_summary_tab(city_code, city_name):
    st.markdown(f"#### Thống kê tổng hợp ngày — {city_name}")
    summary_df = get_daily_summary(city_code, days=30)
    
    if not summary_df.empty:
        cols_show = ["date_local", "temp_min", "temp_max", "temp_avg",
                     "aqi_min", "aqi_max", "aqi_avg",
                     "hours_unhealthy", "hours_hazardous"]
        existing_cols = [c for c in cols_show if c in summary_df.columns]
        st.dataframe(
            summary_df[existing_cols].style.format({
                "temp_min": "{:.1f}°C",
                "temp_max": "{:.1f}°C",
                "temp_avg": "{:.1f}°C",
                "aqi_avg": "{:.0f}",
                "aqi_min": "{:.0f}",
                "aqi_max": "{:.0f}",
            }),
            use_container_width=True
        )
    else:
        st.info("Chưa có dữ liệu tổng hợp ngày. Bảng Gold sẽ được cập nhật sau chu kỳ tổng hợp.")


# ============================================================
# MAIN APPLICATION
# ============================================================
def main():
    st.set_page_config(
        page_title="Weather & AQI Lakehouse",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    inject_custom_css()
    render_header()

    # Sidebar controls
    with st.sidebar:
        st.markdown("<h3 style='font-size: 1.1rem; margin-bottom: 16px;'>Bộ lọc & Tùy chọn</h3>", unsafe_allow_html=True)

        city_map = get_all_cities()
        if not city_map:
            st.warning("Chưa tìm thấy dữ liệu trong Iceberg. Vui lòng kiểm tra pipeline đang hoạt động.")
            if st.button("Tải lại danh sách", use_container_width=True):
                st.cache_data.clear()
                st.rerun()
            return

        city_codes = list(city_map.keys())
        selected_code = st.selectbox(
            "Chọn tỉnh thành",
            options=city_codes,
            format_func=lambda x: city_map.get(x, x),
            index=0
        )
        selected_name = city_map.get(selected_code, selected_code)

        time_range = st.radio(
            "Khoảng thời gian",
            options=[24, 48, 120],
            format_func=lambda x: {24: "24 giờ qua", 48: "48 giờ qua", 120: "5 ngày qua"}[x],
            index=0
        )

        st.markdown("---")
        if st.button("Làm mới dữ liệu", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown("""
        <div style="font-size: 0.75rem; color: #64748b; margin-top: 24px; line-height: 1.6;">
            <strong>Công nghệ Lakehouse:</strong><br>
            • Storage: MinIO (S3-compatible)<br>
            • Table Format: Apache Iceberg<br>
            • Metadata Catalog: Project Nessie<br>
            • Query Engine: Trino 462
        </div>
        """, unsafe_allow_html=True)

    # Main Tabs
    tab1, tab2, tab3 = st.tabs([
        "Thời gian thực (Real-time)",
        "Độ chính xác dự báo (Accuracy)",
        "Tổng hợp ngày (Daily Summary)"
    ])

    with tab1:
        latest = get_latest_metrics(selected_code)
        if latest is None:
            st.warning(f"Chưa có dữ liệu cập nhật cho {selected_name}.")
        else:
            render_alert_banner(latest.get("health_risk"))
            render_weather_metrics(latest)
            
            st.markdown("<div style='margin-top: 24px;'></div>", unsafe_allow_html=True)
            df = get_metrics_range(selected_code, hours=time_range)
            if not df.empty:
                try:
                    df["timestamp_local"] = pd.to_datetime(df["timestamp_local"])
                except Exception:
                    pass
            
            st.markdown("<div class='chart-panel'>", unsafe_allow_html=True)
            render_aqi_chart(df, selected_name)
            st.markdown("</div>", unsafe_allow_html=True)

            render_environment_charts(df)
            
            alerts_df = get_forecast_alerts(selected_code)
            render_forecast_alerts(alerts_df)

    with tab2:
        render_accuracy_tab()

    with tab3:
        render_summary_tab(selected_code, selected_name)


if __name__ == "__main__":
    main()
