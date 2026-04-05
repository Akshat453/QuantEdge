"""
=================================================================
Streamlit Dashboard — Real-Time Stock Market Analytics
=================================================================
What this does:
  1. Reads processed Parquet files from data/processed/
  2. Auto-refreshes every 10 seconds
  3. Shows interactive charts and metric cards
  4. Displays crash alerts when price drops > 3%

Run on Mac terminal (with venv activated):
  cd /Users/akshatsingh/Desktop/BDAProject
  source venv/bin/activate
  streamlit run dashboard/app.py
=================================================================
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import time
from datetime import datetime

# =================================================================
# PAGE CONFIG — must be the first Streamlit command
# =================================================================
st.set_page_config(
    page_title="📈 Stock Market Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =================================================================
# AUTO-REFRESH — reruns the entire app every 10 seconds
# =================================================================
# st.rerun() is called via a placeholder at the bottom

REFRESH_INTERVAL = 10  # seconds
PARQUET_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "processed"
)

# =================================================================
# CUSTOM CSS — dark theme with vibrant accents
# =================================================================
st.markdown("""
<style>
    /* Main background */
    .stApp {
        background: linear-gradient(135deg, #0f0f23 0%, #1a1a2e 50%, #16213e 100%);
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 16px;
        backdrop-filter: blur(10px);
    }

    [data-testid="stMetricLabel"] {
        color: #a0a0b8 !important;
        font-size: 0.85rem !important;
    }

    [data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-weight: 700 !important;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a1a2e 0%, #16213e 100%);
        border-right: 1px solid rgba(255,255,255,0.08);
    }

    /* Headers */
    h1, h2, h3 {
        color: #e0e0ff !important;
    }

    /* Alert banner */
    .crash-alert {
        background: linear-gradient(90deg, #ff4444, #cc0000);
        color: white;
        padding: 16px 24px;
        border-radius: 12px;
        font-size: 1.1rem;
        font-weight: 700;
        text-align: center;
        margin-bottom: 16px;
        animation: pulse 1.5s ease-in-out infinite;
        box-shadow: 0 4px 20px rgba(255, 0, 0, 0.3);
    }

    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }

    .healthy-banner {
        background: linear-gradient(90deg, #00c853, #009624);
        color: white;
        padding: 12px 24px;
        border-radius: 12px;
        font-size: 1rem;
        font-weight: 600;
        text-align: center;
        margin-bottom: 16px;
    }

    /* Divider */
    hr {
        border-color: rgba(255,255,255,0.1) !important;
    }
</style>
""", unsafe_allow_html=True)


# =================================================================
# DATA LOADING FUNCTION
# =================================================================
@st.cache_data(ttl=8)  # cache for 8 seconds (refresh every 10s)
def load_data():
    """
    Reads the Parquet files produced by Spark Streaming.
    Returns a Pandas DataFrame, or None if no data exists.
    """
    if not os.path.exists(PARQUET_PATH):
        return None

    try:
        df = pd.read_parquet(PARQUET_PATH)
        if df.empty:
            return None

        # Ensure timestamp is datetime type
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp")

        return df
    except Exception as e:
        st.error(f"Error reading Parquet: {e}")
        return None


# =================================================================
# SIDEBAR — Stock Symbol Selector
# =================================================================
st.sidebar.markdown("## 🎯 Filters")

# Load data first
df = load_data()

if df is None or df.empty:
    st.title("📈 Real-Time Stock Market Analytics")
    st.warning("⏳ **Waiting for data...** No Parquet files found yet.")
    st.info(
        "Make sure the pipeline is running:\n"
        "1. Producer: `python producer/producer.py`\n"
        "2. Spark: `spark-submit --packages ... spark/spark_stream.py`\n"
        "3. Wait for 2-3 batches to be processed."
    )
    time.sleep(REFRESH_INTERVAL)
    st.rerun()

# Get available symbols
available_symbols = sorted(df["symbol"].unique().tolist())

selected_symbol = st.sidebar.selectbox(
    "Select Stock Symbol",
    options=available_symbols,
    index=0
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Available Stocks")
for sym in available_symbols:
    latest = df[df["symbol"] == sym].iloc[-1]
    change = latest.get("price_change_pct", 0)
    color = "🟢" if change >= 0 else "🔴"
    st.sidebar.markdown(f"{color} **{sym}** — ${latest['close']:.2f} ({change:+.2f}%)")

st.sidebar.markdown("---")
st.sidebar.markdown(
    f"🕐 **Last refresh:** {datetime.now().strftime('%H:%M:%S')}\n\n"
    f"🔄 Auto-refreshes every {REFRESH_INTERVAL}s"
)

# =================================================================
# FILTER DATA FOR SELECTED SYMBOL
# =================================================================
sym_df = df[df["symbol"] == selected_symbol].copy()
latest_row = sym_df.iloc[-1]

# =================================================================
# HEADER
# =================================================================
st.title("📈 Real-Time Stock Market Analytics")
st.markdown(f"### Viewing: **{selected_symbol}**")

# =================================================================
# CRASH ALERT BANNER — if price drops > 3%
# =================================================================
crash_threshold = -3.0
price_change = latest_row.get("price_change_pct", 0)

if price_change < crash_threshold:
    st.markdown(
        f'<div class="crash-alert">'
        f'🚨 CRASH ALERT: {selected_symbol} dropped {price_change:.2f}% '
        f'in the latest interval! Current: ${latest_row["close"]:.2f}'
        f'</div>',
        unsafe_allow_html=True
    )
else:
    st.markdown(
        f'<div class="healthy-banner">'
        f'✅ {selected_symbol} is stable — '
        f'Price change: {price_change:+.2f}%'
        f'</div>',
        unsafe_allow_html=True
    )

# =================================================================
# METRIC CARDS — Current Price, Price Change %, Volatility
# =================================================================
st.markdown("---")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="💰 Current Price",
        value=f"${latest_row['close']:.2f}",
        delta=f"{price_change:+.2f}%"
    )

with col2:
    st.metric(
        label="📊 Price Change %",
        value=f"{price_change:+.2f}%",
        delta="Bullish" if price_change >= 0 else "Bearish",
        delta_color="normal" if price_change >= 0 else "inverse"
    )

with col3:
    volatility = latest_row.get("volatility", 0)
    vol_label = "High" if volatility > 2 else "Normal" if volatility > 0.5 else "Low"
    st.metric(
        label="⚡ Volatility",
        value=f"{volatility:.2f}%",
        delta=vol_label
    )

with col4:
    moving_avg = latest_row.get("moving_avg_5", latest_row["close"])
    ma_diff = latest_row["close"] - moving_avg
    st.metric(
        label="📈 Moving Avg (5)",
        value=f"${moving_avg:.2f}",
        delta=f"{ma_diff:+.2f} vs close"
    )

# =================================================================
# CHARTS
# =================================================================
st.markdown("---")

# --- Row 1: Close Price + Moving Average ---
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.markdown("#### 📉 Close Price Over Time")
    fig_price = go.Figure()

    fig_price.add_trace(go.Scatter(
        x=sym_df["timestamp"],
        y=sym_df["close"],
        mode="lines+markers",
        name="Close Price",
        line=dict(color="#00d4ff", width=2),
        marker=dict(size=4)
    ))

    if "moving_avg_5" in sym_df.columns:
        fig_price.add_trace(go.Scatter(
            x=sym_df["timestamp"],
            y=sym_df["moving_avg_5"],
            mode="lines",
            name="Moving Avg (5)",
            line=dict(color="#ff6b6b", width=2, dash="dash")
        ))

    fig_price.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=400,
        margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="Price ($)")
    )
    st.plotly_chart(fig_price, width="stretch")

with chart_col2:
    st.markdown("#### 📊 Volume Trend")
    volume_col = "volume_cumulative" if "volume_cumulative" in sym_df.columns else "volume"

    fig_volume = px.bar(
        sym_df,
        x="timestamp",
        y=volume_col,
        color_discrete_sequence=["#7c4dff"]
    )
    fig_volume.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=400,
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="Volume")
    )
    st.plotly_chart(fig_volume, width="stretch")

# --- Row 2: Price Change % + Volatility ---
chart_col3, chart_col4 = st.columns(2)

with chart_col3:
    st.markdown("#### 📈 Price Change % Over Time")
    if "price_change_pct" in sym_df.columns:
        fig_change = go.Figure()

        colors = ["#00c853" if v >= 0 else "#ff4444"
                  for v in sym_df["price_change_pct"]]

        fig_change.add_trace(go.Bar(
            x=sym_df["timestamp"],
            y=sym_df["price_change_pct"],
            marker_color=colors,
            name="Price Change %"
        ))

        # Add crash threshold line
        fig_change.add_hline(
            y=crash_threshold,
            line_dash="dash",
            line_color="red",
            annotation_text="Crash threshold (-3%)"
        )

        fig_change.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=350,
            margin=dict(l=20, r=20, t=30, b=20),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="Change %")
        )
        st.plotly_chart(fig_change, width="stretch")

with chart_col4:
    st.markdown("#### ⚡ Volatility Over Time")
    if "volatility" in sym_df.columns:
        fig_vol = px.area(
            sym_df,
            x="timestamp",
            y="volatility",
            color_discrete_sequence=["#ff9800"]
        )
        fig_vol.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=350,
            margin=dict(l=20, r=20, t=30, b=20),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="Volatility %")
        )
        st.plotly_chart(fig_vol, width="stretch")

# =================================================================
# ALL STOCKS COMPARISON
# =================================================================
st.markdown("---")
st.markdown("#### 🏆 All Stocks — Latest Snapshot")

latest_all = df.groupby("symbol", observed=True).last().reset_index()
comparison_cols = ["symbol", "close", "price_change_pct", "volatility"]
if "moving_avg_5" in latest_all.columns:
    comparison_cols.append("moving_avg_5")
if "volume" in latest_all.columns:
    comparison_cols.append("volume")

display_df = latest_all[comparison_cols].copy()
display_df.columns = [c.replace("_", " ").title() for c in display_df.columns]

st.dataframe(
    display_df.style.format({
        "Close": "${:.2f}",
        "Price Change Pct": "{:+.2f}%",
        "Volatility": "{:.2f}%",
        "Moving Avg 5": "${:.2f}"
    }),
    width="stretch",
    hide_index=True
)

# =================================================================
# SYSTEM METRICS
# =================================================================
st.markdown("---")

sys_col1, sys_col2, sys_col3, sys_col4 = st.columns(4)

with sys_col1:
    st.metric("📦 Total Records", f"{len(df):,}")

with sys_col2:
    st.metric("📊 Unique Symbols", f"{df['symbol'].nunique()}")

with sys_col3:
    if "timestamp" in df.columns:
        last_ts = df["timestamp"].max()
        st.metric("🕐 Last Data Point", last_ts.strftime("%H:%M:%S"))
    else:
        st.metric("🕐 Last Data Point", "N/A")

with sys_col4:
    st.metric("🔄 Dashboard Refresh", f"Every {REFRESH_INTERVAL}s")

# =================================================================
# FOOTER
# =================================================================
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #666; font-size: 0.8rem;'>"
    "Real-Time Stock Market Analytics System | "
    "Apache Spark + Kafka + Streamlit | "
    f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    "</div>",
    unsafe_allow_html=True
)

# =================================================================
# AUTO-REFRESH TRIGGER
# =================================================================
time.sleep(REFRESH_INTERVAL)
st.rerun()
