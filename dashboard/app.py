"""
=================================================================
Streamlit Dashboard — Real-Time Stock Market Analytics (Upgraded)
=================================================================
6-tab layout:
  Tab 1 — Market Overview     : live price table, sector bars, gainers/losers
  Tab 2 — Live Charts         : candlestick, MA divergence, VWAP
  Tab 3 — 3D Analytics        : Price-Volume-Time surface, Risk Cube
  Tab 4 — Scatter & Correlation: anomaly detection, VWAP/RSI map, heatmap
  Tab 5 — System Performance  : 3Vs monitor, load balancing, CPU/memory
  Tab 6 — Crash Detection     : live alerts, history, timeline

Auto-refreshes every 5 seconds.
Run:  streamlit run dashboard/app.py
=================================================================
"""

import os
import json
import time
import warnings
from datetime import datetime, timedelta
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
import psutil
import streamlit as st

warnings.filterwarnings("ignore")
sns.set_style("darkgrid")
plt.rcParams.update({
    "figure.facecolor": "#0f0f23",
    "axes.facecolor":   "#1a1a2e",
    "axes.edgecolor":   "#3a3a5e",
    "axes.labelcolor":  "#e0e0ff",
    "text.color":       "#e0e0ff",
    "xtick.color":      "#a0a0b8",
    "ytick.color":      "#a0a0b8",
    "grid.color":       "#2a2a4e",
    "legend.facecolor": "#1a1a2e",
    "legend.edgecolor": "#3a3a5e",
    "font.size":         10,
})

# =================================================================
# PAGE CONFIG — must be first Streamlit call
# =================================================================
st.set_page_config(
    page_title="Stock Market Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =================================================================
# CONSTANTS
# =================================================================
REFRESH_INTERVAL = 5  # seconds

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(PROJECT_ROOT, "data", "processed")
RAW_TICKS_PATH = os.path.join(PROJECT_ROOT, "data", "raw_ticks")
LOGS_DIR = os.path.join(PROJECT_ROOT, "data", "logs")
LB_LOG = os.path.join(LOGS_DIR, "lb_metrics.jsonl")
SPARK_LOG = os.path.join(LOGS_DIR, "spark_metrics.jsonl")

# NOTE: Symbol and sector lists are NOT hardcoded here.
# They are derived dynamically from the parquet data in the sidebar below.

# Colour palette used across all matplotlib/seaborn charts
PALETTE = {
    "green":  "#00c853",
    "red":    "#ff4444",
    "blue":   "#00d4ff",
    "orange": "#ff9800",
    "purple": "#7c4dff",
    "pink":   "#e040fb",
}

# =================================================================
# CUSTOM CSS
# =================================================================
st.markdown("""
<style>
.stApp { background: linear-gradient(135deg,#0f0f23 0%,#1a1a2e 50%,#16213e 100%); }
[data-testid="stMetric"] {
    background: rgba(255,255,255,0.05);
    border:1px solid rgba(255,255,255,0.1);
    border-radius:12px; padding:16px;
}
[data-testid="stMetricLabel"]  { color:#a0a0b8 !important; font-size:0.85rem !important; }
[data-testid="stMetricValue"]  { color:#ffffff !important; font-weight:700 !important; }
[data-testid="stSidebar"] {
    background: linear-gradient(180deg,#1a1a2e 0%,#16213e 100%);
    border-right:1px solid rgba(255,255,255,0.08);
}
h1,h2,h3 { color:#e0e0ff !important; }
hr { border-color:rgba(255,255,255,0.1) !important; }
.crash-alert {
    background:linear-gradient(90deg,#ff4444,#cc0000);
    color:white; padding:14px 24px; border-radius:12px;
    font-size:1.05rem; font-weight:700; text-align:center;
    margin-bottom:12px; box-shadow:0 4px 20px rgba(255,0,0,0.3);
}
.healthy-banner {
    background:linear-gradient(90deg,#00c853,#009624);
    color:white; padding:10px 24px; border-radius:12px;
    font-size:0.95rem; font-weight:600; text-align:center; margin-bottom:12px;
}
</style>
""", unsafe_allow_html=True)


# =================================================================
# DATA LOADING
# =================================================================

@st.cache_data(ttl=3)  # 3-second TTL — picks up new Spark batches quickly
def load_processed_data(time_window_minutes: int = 0) -> pd.DataFrame | None:
    """
    Read processed Parquet files from Spark output.
    Returns None with a warning if no data yet.
    time_window_minutes=0 means load all data.
    """
    if not os.path.exists(PROCESSED_PATH):
        return None
    try:
        df = pd.read_parquet(PROCESSED_PATH)
        if df.empty:
            return None

        # Normalise time column: new Spark job writes "event_time",
        # old job wrote "timestamp". Alias old name so the rest of the
        # dashboard always works against "event_time".
        if "event_time" not in df.columns and "timestamp" in df.columns:
            df["event_time"] = df["timestamp"]

        if "event_time" in df.columns:
            df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
            df = df.dropna(subset=["event_time"])
            df = df.sort_values("event_time")

        if time_window_minutes > 0 and "event_time" in df.columns:
            cutoff = df["event_time"].max() - timedelta(minutes=time_window_minutes)
            df = df[df["event_time"] >= cutoff]

        # Ensure sector column — Spark writes it directly from Kafka message;
        # fall back to "Unknown" only if truly missing in the parquet data
        if "sector" not in df.columns:
            df["sector"] = "Unknown"
        else:
            df["sector"] = df["sector"].fillna("Unknown")

        return df if not df.empty else None
    except Exception as e:
        st.warning(f"Could not read processed data: {e}")
        return None


@st.cache_data(ttl=10)
def load_raw_ticks() -> pd.DataFrame | None:
    """Read raw (unprocessed) tick data for Volume V demonstration."""
    if not os.path.exists(RAW_TICKS_PATH):
        return None
    try:
        df = pd.read_parquet(RAW_TICKS_PATH)
        return df if not df.empty else None
    except Exception as e:
        st.warning(f"Could not read raw ticks: {e}")
        return None


@st.cache_data(ttl=8)
def load_lb_metrics() -> pd.DataFrame | None:
    """Read load-balancing metrics JSONL from producer."""
    if not os.path.exists(LB_LOG):
        return None
    try:
        rows = []
        with open(LB_LOG) as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        return df
    except Exception:
        return None


@st.cache_data(ttl=8)
def load_spark_metrics() -> pd.DataFrame | None:
    """Read Spark batch metrics JSONL."""
    if not os.path.exists(SPARK_LOG):
        return None
    try:
        rows = []
        with open(SPARK_LOG) as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        return df
    except Exception:
        return None


def get_parquet_size_mb(path: str) -> float:
    """Walk a directory and sum all .parquet file sizes → MB."""
    sizes: list[int] = []
    if os.path.exists(path):
        for root, _, files in os.walk(path):
            for f in files:
                if f.endswith(".parquet"):
                    try:
                        sizes.append(int(os.path.getsize(os.path.join(root, f))))
                    except OSError:
                        pass
    mb = float(sum(sizes)) / (1024.0 * 1024.0)
    return float(f"{mb:.2f}")


def _time_col(df: pd.DataFrame) -> str | None:
    """Return the name of the time column, preferring event_time over timestamp."""
    if "event_time" in df.columns:
        return "event_time"
    if "timestamp" in df.columns:
        return "timestamp"
    return None


def apply_time_window(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Filter df to last `minutes` minutes. 0 = no filter."""
    tc = _time_col(df)
    if minutes <= 0 or tc is None:
        return df
    cutoff = df[tc].max() - timedelta(minutes=minutes)
    return df[df[tc] >= cutoff]


def get_latest_per_symbol(df: pd.DataFrame) -> pd.DataFrame:
    """Return the most recent row per symbol."""
    tc = _time_col(df)
    if tc is not None:
        return df.sort_values(tc).groupby("symbol", observed=True).last().reset_index()
    return df.groupby("symbol", observed=True).last().reset_index()


# =================================================================
# SIDEBAR
# =================================================================
st.sidebar.markdown("## 🎯 Filters")

# Time window
time_options = {"5 minutes": 5, "15 minutes": 15, "1 hour": 60, "All data": 0}
selected_time_label = st.sidebar.selectbox("Time Window", list(time_options.keys()), index=3)
selected_time_minutes = time_options[selected_time_label]

# Load data now (needed for dynamic symbol/sector lists)
df_all = load_processed_data(selected_time_minutes)

# --- Symbol/sector lists: always from the FULL unfiltered dataframe ---
if df_all is not None and not df_all.empty:
    # Sectors from data
    available_sectors = sorted(
        df_all["sector"].dropna().unique().tolist()
    ) if "sector" in df_all.columns else []
    # ALL symbols — never filter before building this list
    all_symbols_in_data = sorted(df_all["symbol"].dropna().unique().tolist())
else:
    available_sectors = []
    all_symbols_in_data = []

# Sector filter — populated from actual data
selected_sectors = st.sidebar.multiselect(
    "Sectors", available_sectors, default=available_sectors
)

# Symbol filter: sector-filtered view for the multiselect choices,
# but starting from the full list (no cap)
if df_all is not None and not df_all.empty and "sector" in df_all.columns and selected_sectors:
    available_symbols = sorted(
        df_all[df_all["sector"].isin(selected_sectors)]["symbol"].dropna().unique().tolist()
    )
else:
    available_symbols = all_symbols_in_data

selected_symbols = st.sidebar.multiselect(
    "Symbols", available_symbols, default=available_symbols
)

st.sidebar.markdown("---")
st.sidebar.markdown(f"🕐 **Refreshed:** {datetime.now().strftime('%H:%M:%S')}")
st.sidebar.markdown(f"🔄 Auto-refresh every {REFRESH_INTERVAL}s")

# --- Debug panel (always visible until data is healthy) ---
with st.sidebar.expander("🔍 Data Debug", expanded=(len(all_symbols_in_data) < 10)):
    st.write(f"**Symbols in parquet:** {len(all_symbols_in_data)}")
    st.write(all_symbols_in_data if all_symbols_in_data else "*(none yet)*")
    st.write(f"**Sectors:** {available_sectors}")
    st.write(f"**Total rows:** {len(df_all) if df_all is not None else 0}")

# Apply symbol filter
if df_all is not None and selected_symbols:
    df = df_all[df_all["symbol"].isin(selected_symbols)].copy()
else:
    df = df_all

# =================================================================
# STALE DATA / LOADING STATE
# =================================================================
if df is None or df.empty:
    st.title("📈 Real-Time Stock Market Analytics")
    st.info(
        "⏳ **Waiting for data...** No Parquet files found yet.\n\n"
        "Make sure the pipeline is running:\n"
        "1. `./start_all.sh`  ← launches everything\n"
        "2. Wait 2–3 batches (~30 seconds) for first data\n"
        "3. This page will auto-refresh."
    )
    time.sleep(REFRESH_INTERVAL)
    st.rerun()

# Warn if stale 5-symbol data is still lingering
if len(all_symbols_in_data) > 0 and len(all_symbols_in_data) < 10:
    st.warning(
        f"⚠️ Only {len(all_symbols_in_data)} symbol(s) in parquet — "
        "data may still be loading or old stale files are present. "
        "Expected 50 symbols within ~60 seconds of pipeline start. "
        f"Found: `{'`, `'.join(all_symbols_in_data)}`"
    )

# =================================================================
# MAIN CONTENT — 6 TABS
# =================================================================
st.title("📈 Real-Time Stock Market Analytics")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Market Overview",
    "📈 Live Charts",
    "🔮 3D Analytics",
    "🔍 Scatter & Correlation",
    "⚡ System Performance",
    "🚨 Crash Detection",
])


# ─────────────────────────────────────────────────────────────────
# TAB 1 — MARKET OVERVIEW
# ─────────────────────────────────────────────────────────────────
with tab1:
    latest = get_latest_per_symbol(df)

    # --- Metric Cards ---
    processed_mb = get_parquet_size_mb(PROCESSED_PATH)
    raw_mb = get_parquet_size_mb(RAW_TICKS_PATH)
    avg_latency = df["latency_ms"].mean() if "latency_ms" in df.columns else 0
    total_events = len(df)

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("📦 Stocks Tracked", f"{df['symbol'].nunique()}")
    with m2:
        st.metric("🔢 Total Events", f"{total_events:,}")
    with m3:
        st.metric("⏱ Avg Latency", f"{avg_latency:.0f} ms")
    with m4:
        st.metric("💾 Data on Disk", f"{processed_mb + raw_mb:.1f} MB")

    st.markdown("---")

    # --- Live Price Table (color-coded) ---
    st.markdown("#### 🏷️ Live Price Snapshot — All Symbols")

    display_cols = ["symbol", "sector", "close", "price_change_pct", "volatility",
                    "moving_avg_5", "volume", "data_type"]
    avail_cols = [c for c in display_cols if c in latest.columns]
    table_df = latest[avail_cols].copy()
    table_df.columns = [c.replace("_", " ").title() for c in table_df.columns]

    def color_change(val):
        try:
            v = float(val)
            if v > 0:
                return "color: #00c853; font-weight:600"
            elif v < 0:
                return "color: #ff4444; font-weight:600"
        except Exception:
            pass
        return ""

    fmt = {}
    col_map = {c.replace("_", " ").title(): c for c in avail_cols}
    if "Close" in table_df.columns:
        fmt["Close"] = "${:.2f}"
    if "Price Change Pct" in table_df.columns:
        fmt["Price Change Pct"] = "{:+.2f}%"
    if "Volatility" in table_df.columns:
        fmt["Volatility"] = "{:.2f}%"
    if "Moving Avg 5" in table_df.columns:
        fmt["Moving Avg 5"] = "${:.2f}"

    styled = table_df.style.format(fmt, na_rep="—")
    if "Price Change Pct" in table_df.columns:
        styled = styled.applymap(color_change, subset=["Price Change Pct"])

    st.dataframe(styled, use_container_width=True, hide_index=True)

    st.markdown("---")
    col_a, col_b = st.columns(2)

    # --- Sector Performance Bar ---
    with col_a:
        st.markdown("#### 🏭 Sector Performance")
        if "price_change_pct" in latest.columns and "sector" in latest.columns:
            sector_perf = (
                latest.groupby("sector")["price_change_pct"]
                .mean()
                .reset_index()
                .sort_values("price_change_pct")
            )
            colors = [PALETTE["green"] if v >= 0 else PALETTE["red"]
                      for v in sector_perf["price_change_pct"]]
            fig, ax = plt.subplots(figsize=(6, 3.5))
            bars = ax.barh(sector_perf["sector"], sector_perf["price_change_pct"],
                           color=colors)
            ax.axvline(0, color="#a0a0b8", linewidth=0.8, linestyle="--")
            for bar, v in zip(bars, sector_perf["price_change_pct"]):
                ax.text(v + (0.05 if v >= 0 else -0.05), bar.get_y() + bar.get_height() / 2,
                        f"{v:+.2f}%", va="center",
                        ha="left" if v >= 0 else "right", fontsize=9,
                        color=PALETTE["green"] if v >= 0 else PALETTE["red"])
            ax.set_xlabel("Avg Price Change %")
            ax.set_title("Sector Performance")
            plt.tight_layout()
            st.pyplot(fig)
            plt.close(fig)
        else:
            st.info("Sector data not yet available.")

    # --- Top 5 Gainers / Losers ---
    with col_b:
        st.markdown("#### 🏆 Top 5 Gainers & Losers")
        if "price_change_pct" in latest.columns:
            gainers = latest.nlargest(5, "price_change_pct")[["symbol", "close", "price_change_pct"]]
            losers = latest.nsmallest(5, "price_change_pct")[["symbol", "close", "price_change_pct"]]
            g_col, l_col = st.columns(2)
            with g_col:
                st.markdown("**📈 Gainers**")
                for _, row in gainers.iterrows():
                    st.markdown(
                        f"🟢 **{row['symbol']}** — ${row['close']:.2f} "
                        f"({row['price_change_pct']:+.2f}%)"
                    )
            with l_col:
                st.markdown("**📉 Losers**")
                for _, row in losers.iterrows():
                    st.markdown(
                        f"🔴 **{row['symbol']}** — ${row['close']:.2f} "
                        f"({row['price_change_pct']:+.2f}%)"
                    )
        else:
            st.info("Price change data not yet available.")


# ─────────────────────────────────────────────────────────────────
# TAB 2 — LIVE CHARTS
# ─────────────────────────────────────────────────────────────────
with tab2:
    available_syms = sorted(df["symbol"].unique())
    chart_symbol = st.selectbox("Select Symbol", available_syms, key="chart_sym")
    sym_df = df[df["symbol"] == chart_symbol].sort_values("event_time")

    if sym_df.empty:
        st.info("No data for selected symbol yet.")
    else:
        # --- OHLCV Line chart (matplotlib candlestick substitute) ---
        st.markdown(f"#### 🕯️ OHLCV Price & Volume — {chart_symbol}")

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6),
                                        gridspec_kw={"height_ratios": [3, 1]},
                                        sharex=True)
        # Price lines
        ax1.plot(sym_df["event_time"], sym_df["close"],
                 color=PALETTE["blue"], linewidth=1.5, label="Close", zorder=3)
        ax1.fill_between(sym_df["event_time"], sym_df["low"], sym_df["high"],
                         alpha=0.15, color=PALETTE["blue"], label="High–Low range")
        if "moving_avg_5" in sym_df.columns:
            ax1.plot(sym_df["event_time"], sym_df["moving_avg_5"],
                     color=PALETTE["blue"], linewidth=1, linestyle=":", label="MA 5")
        if "moving_avg_20" in sym_df.columns:
            ax1.plot(sym_df["event_time"], sym_df["moving_avg_20"],
                     color=PALETTE["orange"], linewidth=1, linestyle="--", label="MA 20")
        if "vwap" in sym_df.columns:
            ax1.plot(sym_df["event_time"], sym_df["vwap"],
                     color=PALETTE["pink"], linewidth=1.5, linestyle="-", label="VWAP")
        ax1.set_ylabel("Price ($)")
        ax1.set_title(f"{chart_symbol} — Close Price, MA & VWAP")
        ax1.legend(fontsize=8, loc="upper left")
        ax1.grid(True, alpha=0.3)
        # Volume bars
        ax2.bar(sym_df["event_time"], sym_df["volume"],
                color=PALETTE["purple"], alpha=0.6, width=0.0005)
        ax2.set_ylabel("Volume")
        ax2.grid(True, alpha=0.3)
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=8)
        plt.tight_layout()
        st.pyplot(fig)
        plt.close(fig)

        chart_col1, chart_col2 = st.columns(2)

        # --- MA Divergence ---
        with chart_col1:
            st.markdown(f"#### 📐 MA Divergence — {chart_symbol}")
            if "moving_avg_5" in sym_df.columns and "moving_avg_20" in sym_df.columns:
                ma_div = sym_df.copy()
                ma_div["ma_div_pct"] = (
                    (ma_div["moving_avg_5"] - ma_div["moving_avg_20"])
                    / ma_div["moving_avg_20"].replace(0, float("nan"))
                    * 100
                )
                ma_div = ma_div.dropna(subset=["ma_div_pct"])
                fig, ax = plt.subplots(figsize=(6, 3.5))
                pos_mask = ma_div["ma_div_pct"] >= 0
                ax.fill_between(ma_div["event_time"], ma_div["ma_div_pct"],
                                where=pos_mask, color=PALETTE["green"], alpha=0.4, label="Bullish")
                ax.fill_between(ma_div["event_time"], ma_div["ma_div_pct"],
                                where=~pos_mask, color=PALETTE["red"], alpha=0.4, label="Bearish")
                ax.plot(ma_div["event_time"], ma_div["ma_div_pct"],
                        color="white", linewidth=0.8)
                ax.axhline(0, color="#a0a0b8", linewidth=0.8, linestyle="--")
                ax.set_title("MA Divergence (MA5−MA20)/MA20 × 100 %")
                ax.set_ylabel("Divergence %")
                ax.legend(fontsize=8)
                ax.grid(True, alpha=0.3)
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=8)
                plt.tight_layout()
                st.pyplot(fig)
                plt.close(fig)
            else:
                st.info("Moving average data not yet available (needs ~20 periods).")

        # --- VWAP Line ---
        with chart_col2:
            st.markdown(f"#### 💹 VWAP Over Time — {chart_symbol}")
            if "vwap" in sym_df.columns:
                fig, ax = plt.subplots(figsize=(6, 3.5))
                ax.plot(sym_df["event_time"], sym_df["close"],
                        color=PALETTE["blue"], linewidth=1.5, label="Close")
                ax.plot(sym_df["event_time"], sym_df["vwap"],
                        color=PALETTE["pink"], linewidth=2, linestyle="--", label="VWAP")
                ax.set_title("Close vs VWAP")
                ax.set_ylabel("Price ($)")
                ax.legend(fontsize=8)
                ax.grid(True, alpha=0.3)
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=8)
                plt.tight_layout()
                st.pyplot(fig)
                plt.close(fig)
            else:
                st.info("VWAP data not yet available.")


# ─────────────────────────────────────────────────────────────────
# TAB 3 — 3D ANALYTICS
# ─────────────────────────────────────────────────────────────────
with tab3:
    st.markdown("### 🔮 3D Market Visualizations")

    # Limit to 500 rows for performance
    plot3d_df = df.tail(500).copy() if len(df) > 500 else df.copy()
    n_points = len(plot3d_df)
    st.caption(f"Displaying {n_points} most recent data points (max 500 for performance)")

    col3d_1, col3d_2 = st.columns(2)

    with col3d_1:
        st.markdown("#### Price × Volume × Time — 3D Market Landscape")
        if not plot3d_df.empty and "event_time" in plot3d_df.columns:
            # Encode time as numeric (seconds since min)
            t_min = plot3d_df["event_time"].min()
            plot3d_df["t_numeric"] = (
                plot3d_df["event_time"] - t_min
            ).dt.total_seconds()

            # Encode symbol as integer
            sym_cats = {s: i for i, s in enumerate(sorted(plot3d_df["symbol"].unique()))}
            plot3d_df["sym_int"] = plot3d_df["symbol"].map(sym_cats)

            fig = plt.figure(figsize=(7, 5))
            ax = fig.add_subplot(111, projection="3d")
            sc = ax.scatter(
                plot3d_df["t_numeric"],
                plot3d_df["sym_int"],
                plot3d_df["close"],
                c=plot3d_df["volume"],
                cmap="viridis", s=10, alpha=0.7,
            )
            fig.colorbar(sc, ax=ax, shrink=0.5, label="Volume")
            ax.set_xlabel("Time (s)", fontsize=8)
            ax.set_ylabel("Symbol", fontsize=8)
            ax.set_zlabel("Close ($)", fontsize=8)
            ax.set_title("Price × Volume × Time", fontsize=10)
            plt.tight_layout()
            st.pyplot(fig)
            plt.close(fig)
        else:
            st.info("Waiting for data...")

    with col3d_2:
        st.markdown("#### Risk Cube: Volatility × RSI × Volume")
        needed = ["volatility", "rsi_14", "volume", "price_change_pct"]
        has_all = all(c in plot3d_df.columns for c in needed)
        if has_all and not plot3d_df.empty:
            cube_df = plot3d_df.dropna(subset=needed).copy()
            if not cube_df.empty:
                log_vol = np.log1p(cube_df["volume"])
                fig = plt.figure(figsize=(7, 5))
                ax = fig.add_subplot(111, projection="3d")
                sc = ax.scatter(
                    cube_df["volatility"],
                    cube_df["rsi_14"],
                    log_vol,
                    c=cube_df["price_change_pct"],
                    cmap="RdYlGn", vmin=-3, vmax=3,
                    s=20, alpha=0.8,
                )
                fig.colorbar(sc, ax=ax, shrink=0.5, label="Price Δ%")
                ax.set_xlabel("Volatility", fontsize=8)
                ax.set_ylabel("RSI 14", fontsize=8)
                ax.set_zlabel("log(Vol)", fontsize=8)
                ax.set_title("Risk Cube: Volatility vs RSI vs Volume", fontsize=10)
                plt.tight_layout()
                st.pyplot(fig)
                plt.close(fig)
            else:
                st.info("Not enough data for Risk Cube (needs RSI computed — ~14 periods).")
        else:
            st.info("RSI/Volatility data not yet available. Accumulating history...")


# ─────────────────────────────────────────────────────────────────
# TAB 4 — SCATTER & CORRELATION
# ─────────────────────────────────────────────────────────────────
with tab4:
    st.markdown("### 🔍 Scatter Plots & Correlation Analysis")

    scatter_latest = get_latest_per_symbol(df)

    col_s1, col_s2 = st.columns(2)

    # --- Scatter 1: Anomaly Detection ---
    with col_s1:
        st.markdown("#### 🚨 Anomaly Detection: Price Change vs Volume Surge")
        needed_s1 = ["volume_trend", "price_change_pct", "volatility", "crash_score"]
        if all(c in scatter_latest.columns for c in needed_s1):
            s1_df = scatter_latest.dropna(subset=["volume_trend", "price_change_pct"]).copy()
            normal = s1_df[s1_df["crash_score"] != True]
            crashes = s1_df[s1_df["crash_score"] == True]
            fig, ax = plt.subplots(figsize=(6, 4))
            if not normal.empty:
                sizes = (normal["volatility"].fillna(0.5) * 80 + 30).clip(lower=20, upper=200)
                sc = ax.scatter(normal["volume_trend"], normal["price_change_pct"],
                                s=sizes,
                                c=normal["symbol"].astype("category").cat.codes,
                                cmap="tab20", alpha=0.75, label="Normal")
                for _, row in normal.iterrows():
                    ax.annotate(row["symbol"],
                                (row["volume_trend"], row["price_change_pct"]),
                                fontsize=6, color="#a0a0b8")
            if not crashes.empty:
                ax.scatter(crashes["volume_trend"], crashes["price_change_pct"],
                           s=150, color=PALETTE["red"], marker="*",
                           label="⚠️ Crash", zorder=5)
            ax.axvline(1, color="#a0a0b8", linestyle=":", linewidth=0.8)
            ax.axhline(0, color="#a0a0b8", linestyle=":", linewidth=0.8)
            ax.set_xlabel("Volume Trend (current/5-avg)")
            ax.set_ylabel("Price Change %")
            ax.set_title("Anomaly Detection: Price Δ% vs Volume Surge")
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            st.pyplot(fig)
            plt.close(fig)
        else:
            st.info("Volume trend data not yet available (needs ~5 periods).")

    # --- Scatter 2: VWAP Deviation vs RSI ---
    with col_s2:
        st.markdown("#### 📊 VWAP Deviation vs RSI — Signal Map")
        needed_s2 = ["rsi_14", "vwap", "close", "sector"]
        if all(c in scatter_latest.columns for c in needed_s2):
            s2_df = scatter_latest.dropna(subset=["rsi_14", "vwap", "close"]).copy()
            s2_df = s2_df[s2_df["vwap"] > 0]
            if not s2_df.empty:
                s2_df["vwap_dev_pct"] = (
                    (s2_df["close"] - s2_df["vwap"]) / s2_df["vwap"] * 100
                )
                fig, ax = plt.subplots(figsize=(6, 4))
                sectors = s2_df["sector"].unique()
                colors = plt.cm.tab10(np.linspace(0, 1, len(sectors)))
                for sec, col in zip(sectors, colors):
                    sub = s2_df[s2_df["sector"] == sec]
                    ax.scatter(sub["rsi_14"], sub["vwap_dev_pct"],
                               s=70, color=col, label=sec, alpha=0.8)
                    for _, row in sub.iterrows():
                        ax.annotate(row["symbol"],
                                    (row["rsi_14"], row["vwap_dev_pct"]),
                                    fontsize=6, color="#c0c0d8")
                ax.axvline(30, color=PALETTE["green"], linestyle="--",
                           linewidth=1, label="Oversold 30")
                ax.axvline(70, color=PALETTE["red"], linestyle="--",
                           linewidth=1, label="Overbought 70")
                ax.axhline(0, color="#a0a0b8", linestyle=":", linewidth=0.8)
                ax.set_xlabel("RSI 14")
                ax.set_ylabel("VWAP Deviation %")
                ax.set_title("VWAP Dev % vs RSI — Overbought/Oversold Map")
                ax.legend(fontsize=7, ncol=2)
                ax.grid(True, alpha=0.3)
                plt.tight_layout()
                st.pyplot(fig)
                plt.close(fig)
            else:
                st.info("Insufficient data for VWAP deviation plot.")
        else:
            st.info("RSI/VWAP data not yet available (needs ~14 periods).")

    # --- Correlation Heatmap ---
    st.markdown("#### 🌡️ Real-Time Cross-Asset Correlation Heatmap")

    if "event_time" in df.columns and "close" in df.columns:
        pivot = df.pivot_table(
            index="event_time", columns="symbol", values="close"
        )
        n_periods = len(pivot)
        n_syms = int(pivot.notna().any().sum())

        if n_periods >= 20 and n_syms >= 10:
            corr_matrix = pivot.corr()
            corr_matrix = corr_matrix.dropna(how="all", axis=0).dropna(how="all", axis=1)
            fig_w = max(10.0, len(corr_matrix) * 0.4)
            fig, ax = plt.subplots(figsize=(fig_w, fig_w * 0.75))
            sns.heatmap(
                corr_matrix, ax=ax,
                cmap="RdBu_r", vmin=-1, vmax=1,
                annot=(len(corr_matrix) <= 20),
                fmt=".2f", annot_kws={"size": 6},
                linewidths=0.3, linecolor="#2a2a4e",
                cbar_kws={"label": "Correlation"},
            )
            ax.set_title(
                f"Cross-Asset Correlation ({n_periods} periods, {n_syms} symbols)",
                fontsize=11,
            )
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right", fontsize=7)
            plt.setp(ax.yaxis.get_majorticklabels(), rotation=0, fontsize=7)
            plt.tight_layout()
            st.pyplot(fig)
            plt.close(fig)
        else:
            st.info(
                f"Collecting data for heatmap — need ≥20 time periods for ≥10 symbols. "
                f"Currently: {n_periods} periods, {n_syms} symbols. "
                "Available after ~2 minutes of streaming."
            )
    else:
        st.info("Waiting for close price data...")


# ─────────────────────────────────────────────────────────────────
# TAB 5 — SYSTEM PERFORMANCE (3Vs MONITOR)
# ─────────────────────────────────────────────────────────────────
with tab5:
    st.markdown("### ⚡ System Performance — 3Vs Evidence Monitor")

    raw_df = load_raw_ticks()
    lb_df = load_lb_metrics()
    spark_df = load_spark_metrics()

    # ── VOLUME ──
    st.markdown("#### 📦 Volume — Data Growth & Storage")
    v_col1, v_col2, v_col3 = st.columns(3)
    processed_rows = len(df)
    raw_rows = len(raw_df) if raw_df is not None else 0
    with v_col1:
        st.metric("Processed Rows", f"{processed_rows:,}")
    with v_col2:
        st.metric("Raw Tick Rows", f"{raw_rows:,}")
    with v_col3:
        total_mb = get_parquet_size_mb(PROCESSED_PATH) + get_parquet_size_mb(RAW_TICKS_PATH)
        st.metric("Total Parquet Size", f"{total_mb:.2f} MB")

    # Cumulative row growth chart
    if "event_time" in df.columns:
        growth_df = df.set_index("event_time").resample("1min").size().reset_index()
        growth_df.columns = ["time", "count"]
        growth_df["cumulative"] = growth_df["count"].cumsum()
        if not growth_df.empty:
            fig, ax = plt.subplots(figsize=(10, 2.8))
            ax.fill_between(growth_df["time"], growth_df["cumulative"],
                            color=PALETTE["blue"], alpha=0.25)
            ax.plot(growth_df["time"], growth_df["cumulative"],
                    color=PALETTE["blue"], linewidth=1.5, marker="o", markersize=3)
            ax.set_title("Cumulative Data Growth Over Time")
            ax.set_ylabel("Total Rows Stored")
            ax.grid(True, alpha=0.3)
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=8)
            plt.tight_layout()
            st.pyplot(fig)
            plt.close(fig)

    st.markdown("---")

    # ── VELOCITY ──
    st.markdown("#### ⚡ Velocity — Latency & Throughput")
    vel_col1, vel_col2 = st.columns(2)

    with vel_col1:
        if "latency_ms" in df.columns:
            lat_data = df["latency_ms"].dropna()
            avg_lat = lat_data.mean()
            p95_lat = lat_data.quantile(0.95)
            st.metric("Avg End-to-End Latency", f"{avg_lat:.0f} ms",
                      delta=f"P95: {p95_lat:.0f} ms")
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.hist(lat_data, bins=30, color=PALETTE["purple"], alpha=0.8,
                    edgecolor="#2a2a4e")
            ax.axvline(avg_lat, color=PALETTE["blue"], linestyle="--", linewidth=1.5,
                       label=f"Avg {avg_lat:.0f} ms")
            ax.set_xlabel("Latency (ms)")
            ax.set_ylabel("Count")
            ax.set_title("End-to-End Latency Distribution (Kafka → Dashboard)")
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            st.pyplot(fig)
            plt.close(fig)
        else:
            st.info("Latency data not yet available.")

    with vel_col2:
        # Throughput bar from Spark metrics
        if spark_df is not None and "total_rows" in spark_df.columns:
            recent = spark_df.tail(6)
            avg_rows_per_batch = recent["total_rows"].mean()
            rows_per_sec = avg_rows_per_batch / 10.0  # 10-second trigger
            st.metric("Throughput", f"{rows_per_sec:.1f} rows/sec")

            # Simple bar showing recent batch sizes
            fig, ax = plt.subplots(figsize=(6, 3))
            batch_ids = recent.index if "batch_id" not in recent.columns \
                else recent["batch_id"]
            ax.bar(range(len(recent)), recent["total_rows"],
                   color=PALETTE["blue"], alpha=0.8, edgecolor="#2a2a4e")
            ax.axhline(avg_rows_per_batch, color=PALETTE["orange"], linestyle="--",
                       linewidth=1.5, label=f"Avg {avg_rows_per_batch:.0f}")
            ax.set_xlabel("Recent Batches")
            ax.set_ylabel("Rows / batch")
            ax.set_title("Spark Batch Sizes (last 6 batches)")
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            st.pyplot(fig)
            plt.close(fig)

            if "elapsed_ms" in spark_df.columns:
                avg_batch_ms = spark_df["elapsed_ms"].mean()
                st.metric("Avg Spark Batch Duration", f"{avg_batch_ms:.0f} ms")
        else:
            st.info("Spark metrics not yet available.")

    st.markdown("---")

    # ── VARIETY ──
    st.markdown("#### 🎨 Variety — Data Types & Schema")
    var_col1, var_col2 = st.columns(2)

    with var_col1:
        st.markdown("**Data Schema Explorer**")
        schema_df = pd.DataFrame({
            "Column": df.columns.tolist(),
            "Dtype": [str(d) for d in df.dtypes],
            "Non-Null": [df[c].notna().sum() for c in df.columns],
            "Sample": [str(df[c].iloc[-1]) if len(df) > 0 else "—" for c in df.columns],
        })
        st.dataframe(schema_df, use_container_width=True, hide_index=True, height=300)

    with var_col2:
        if "data_type" in df.columns:
            type_counts = df["data_type"].value_counts()
            color_list = [PALETTE["green"] if k == "real" else PALETTE["orange"]
                          for k in type_counts.index]
            fig, ax = plt.subplots(figsize=(5, 4))
            wedges, texts, autotexts = ax.pie(
                type_counts.values, labels=type_counts.index,
                autopct="%1.1f%%", colors=color_list,
                startangle=90, pctdistance=0.8,
            )
            for t in autotexts:
                t.set_fontsize(9)
            ax.set_title("Real vs Simulated Data Breakdown")
            plt.tight_layout()
            st.pyplot(fig)
            plt.close(fig)

    st.markdown("---")

    # ── LOAD BALANCING ──
    st.markdown("#### ⚖️ Load Balancing — Producer Workers")
    lb_col1, lb_col2 = st.columns(2)

    with lb_col1:
        cpu_pct = psutil.cpu_percent(interval=None)
        mem_pct = psutil.virtual_memory().percent
        sys_c1, sys_c2 = st.columns(2)
        with sys_c1:
            st.metric("CPU Usage", f"{cpu_pct:.1f}%")
        with sys_c2:
            st.metric("Memory Usage", f"{mem_pct:.1f}%")

        if lb_df is not None and "worker_id" in lb_df.columns:
            recent_lb = lb_df.tail(40)
            worker_throughput = (
                recent_lb.groupby("worker_id")["messages_sent"].sum().reset_index()
            )
            bar_colors = [PALETTE["blue"], PALETTE["purple"],
                          PALETTE["orange"], PALETTE["green"]]
            fig, ax = plt.subplots(figsize=(5, 3))
            labels = [f"Worker {w}" for w in worker_throughput["worker_id"]]
            c = bar_colors[:len(worker_throughput)]
            bars = ax.bar(labels, worker_throughput["messages_sent"],
                          color=c, edgecolor="#2a2a4e")
            for bar in bars:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 0.5, f"{bar.get_height():.0f}",
                        ha="center", va="bottom", fontsize=8)
            ax.set_ylabel("Messages Sent")
            ax.set_title("Per-Worker Message Throughput")
            ax.grid(True, alpha=0.3, axis="y")
            plt.tight_layout()
            st.pyplot(fig)
            plt.close(fig)

    with lb_col2:
        if lb_df is not None and "queue_depth" in lb_df.columns:
            recent_lb = lb_df.tail(100)
            fig, ax = plt.subplots(figsize=(6, 3.5))
            ax.fill_between(recent_lb["timestamp"], recent_lb["queue_depth"],
                            color=PALETTE["orange"], alpha=0.2)
            ax.plot(recent_lb["timestamp"], recent_lb["queue_depth"],
                    color=PALETTE["orange"], linewidth=1.5)
            ax.axhline(1000, color=PALETTE["red"], linestyle="--",
                       linewidth=1, label="Warn threshold 1000")
            ax.set_ylabel("Messages in Queue")
            ax.set_title("Producer Queue Depth Over Time")
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=8)
            plt.tight_layout()
            st.pyplot(fig)
            plt.close(fig)
        else:
            st.info("Load-balancing metrics not yet available. Start the producer first.")


# ─────────────────────────────────────────────────────────────────
# TAB 6 — CRASH DETECTION
# ─────────────────────────────────────────────────────────────────
with tab6:
    st.markdown("### 🚨 Crash Detection Alerts")

    if "crash_score" in df.columns:
        crash_df = df[df["crash_score"] == True].copy()
        total_crashes = len(crash_df)

        c_m1, c_m2, c_m3 = st.columns(3)
        with c_m1:
            st.metric("Total Crash Signals", str(total_crashes))
        with c_m2:
            crash_syms = crash_df["symbol"].nunique() if total_crashes > 0 else 0
            st.metric("Affected Symbols", str(crash_syms))
        with c_m3:
            if total_crashes > 0 and "price_change_pct" in crash_df.columns:
                worst = crash_df["price_change_pct"].min()
                st.metric("Worst Drop", f"{worst:.2f}%")
            else:
                st.metric("Worst Drop", "—")

        if total_crashes > 0:
            st.markdown(
                '<div class="crash-alert">'
                f'🚨 {total_crashes} CRASH SIGNAL{"S" if total_crashes > 1 else ""} DETECTED — '
                f'{crash_syms} symbol{"s" if crash_syms > 1 else ""} flagged'
                '</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="healthy-banner">✅ No crash signals — all symbols within normal range</div>',
                unsafe_allow_html=True,
            )

        st.markdown("---")

        if total_crashes > 0:
            st.markdown("#### 📋 Alert History (last 50)")
            alert_cols = ["event_time", "symbol", "sector", "close",
                          "price_change_pct", "volume_trend", "volatility"]
            avail_alert = [c for c in alert_cols if c in crash_df.columns]
            alert_table = crash_df.sort_values("event_time", ascending=False)[avail_alert].head(50)
            fmt_alert = {}
            if "close" in alert_table.columns:
                fmt_alert["close"] = "${:.2f}"
            if "price_change_pct" in alert_table.columns:
                fmt_alert["price_change_pct"] = "{:+.2f}%"
            if "volume_trend" in alert_table.columns:
                fmt_alert["volume_trend"] = "{:.2f}x"
            if "volatility" in alert_table.columns:
                fmt_alert["volatility"] = "{:.3f}"
            st.dataframe(
                alert_table.style.format(fmt_alert, na_rep="—"),
                use_container_width=True, hide_index=True,
            )

            # Crash Timeline
            st.markdown("#### ⏱️ Crash Signal Timeline")
            fig, ax = plt.subplots(figsize=(10, 3.5))
            syms_uniq = crash_df["symbol"].unique()
            colors = plt.cm.tab20(np.linspace(0, 1, len(syms_uniq)))
            for sym, col in zip(syms_uniq, colors):
                sym_crashes = crash_df[crash_df["symbol"] == sym]
                y_vals = sym_crashes["price_change_pct"] \
                    if "price_change_pct" in sym_crashes.columns \
                    else pd.Series([0.0] * len(sym_crashes))
                ax.scatter(sym_crashes["event_time"], y_vals,
                           s=80, color=col, marker="x",
                           linewidths=2, label=sym)
            ax.axhline(-2, color=PALETTE["red"], linestyle="--",
                       linewidth=1, label="-2% crash threshold")
            ax.set_xlabel("Time")
            ax.set_ylabel("Price Change %")
            ax.set_title("Crash Signal Events Over Time")
            ax.legend(fontsize=7, ncol=4)
            ax.grid(True, alpha=0.3)
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=8)
            plt.tight_layout()
            st.pyplot(fig)
            plt.close(fig)
        else:
            st.info(
                "No crash signals detected yet. Crash signals trigger when:\n"
                "- Price drops > 2% in a period, AND\n"
                "- Volume surge > 2× 5-period average, AND\n"
                "- Volatility > 1.5%"
            )
    else:
        st.info("Crash score data not yet available. Spark is still computing analytics...")


# =================================================================
# AUTO-REFRESH
# =================================================================
time.sleep(REFRESH_INTERVAL)
st.rerun()
