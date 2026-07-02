import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
from time import sleep
from db import query_pg, query_trino
from config import DASHBOARD_TITLE, DASHBOARD_REFRESH_SECONDS
import system_monitor as smon
import data_quality as dq
import data_catalog as dc
import ml_observability as mlo
import log_explorer as logexp

st.set_page_config(layout="wide", page_title=DASHBOARD_TITLE)

# ─── Custom CSS ──────────────────────────────────────────────
st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }
    .stAppHeader { display: none; }
    .stApp { background-color: #0f131b; }
    h1, h2, h3 { color: #f0f2f6 !important; font-weight: 600; }
    h1 { font-size: 1.6rem !important; margin-bottom: 0.5rem !important; }
    h2 { font-size: 1.2rem !important; }
    .st-emotion-cache-16idsys p { font-size: 0.85rem; }

    .sidebar-section { border-bottom: 1px solid #2d3139; padding-bottom: 0.5rem; margin-bottom: 0.5rem; }
    .stTabs [data-baseweb="tab-list"] { gap: 0.5rem; }
    .stTabs [data-baseweb="tab"] { font-size: 0.85rem; font-weight: 500; }
    .stDataFrame { background: #1a1d23; border: 1px solid #2d3139; border-radius: 6px; }
    .stButton > button {
        background: #1a1d23; border: 1px solid #2d3139; color: #f0f2f6;
        font-weight: 500; border-radius: 6px;
    }
    .stButton > button:hover { border-color: #f7931a; color: #f7931a; }
    .stRadio > div { gap: 0.25rem; }
    .stRadio [data-baseweb="radio"] p { font-size: 0.85rem; }
    hr { border-color: #2d3139; margin: 1rem 0; }
    .card-compact {
        background: #1a1d23; padding: 0.6rem 1rem; border-radius: 6px;
        border: 1px solid #2d3139; margin-bottom: 0.5rem;
    }
    .card-compact .label { font-size: 0.7rem; color: #9ba3af; text-transform: uppercase; }
    .card-compact .value { font-size: 1.1rem; font-weight: 700; color: #f0f2f6; }
    .card-compact .delta { font-size: 0.75rem; }
</style>
""", unsafe_allow_html=True)

# ─── Theme helper ──────────────────────────────────────────────
def _apply_theme(fig, x_title=None, y_title=None, legend=True, height=280, hover="x unified"):
    fig.update_layout(
        paper_bgcolor="#0f131b", plot_bgcolor="#0f131b",
        font_color="#9ba3af", font_size=11,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(showgrid=False, title=dict(text=x_title or "", font_size=10)),
        yaxis=dict(gridcolor="#2d3139", title=dict(text=y_title or "", font_size=10)),
        legend=dict(orientation="h", y=1.08, font_size=10) if legend else {},
        height=height, hovermode=hover,
    )


# ─── Sidebar ─────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f'<div class="sidebar-section"><span style="color:#9ba3af;font-size:0.75rem">BITCOIN VOLATILITY ML</span><br><span style="color:#f0f2f6;font-weight:600">{datetime.now():%H:%M:%S} UTC</span></div>', unsafe_allow_html=True)

    if st.button("Refresh", use_container_width=True):
        st.rerun()

    page = st.radio(
        "Menu",
        ["Market Overview", "Volatility Analytics", "Sentiment Analytics", "Operations"],
        label_visibility="collapsed",
    )

    st.divider()
    st.markdown("""<div style="font-size:0.75rem;color:#5b616e;line-height:1.4">
PostgreSQL — real-time data<br>
Trino — cross-source analytics<br><br>
Kelompok 4 — IPBD 2026
</div>""", unsafe_allow_html=True)

st.title(DASHBOARD_TITLE)


# ═══════════════════════════════════════════════════════════════
# PAGE 0: Summary (Home)
# ═══════════════════════════════════════════════════════════════
def page_summary():
    ohlc = pd.DataFrame()
    sentiment = pd.DataFrame()
    pred = pd.DataFrame()
    containers = pd.DataFrame()
    recent_alerts = pd.DataFrame()

    try:
        ohlc = query_pg(
            "SELECT window_start, close FROM btc_ohlc_1m "
            "WHERE window_start > NOW() - INTERVAL '24 hours' ORDER BY window_start"
        )
    except Exception:
        pass
    try:
        sentiment = query_pg(
            "SELECT compound_score FROM sentiment_30m ORDER BY window_start DESC LIMIT 1"
        )
    except Exception:
        pass
    try:
        pred = query_pg(
            "SELECT COUNT(*) AS cnt FROM volatility_pred "
            "WHERE window_start > NOW() - INTERVAL '1 hour'"
        )
    except Exception:
        pass
    try:
        containers = smon.get_container_status()
    except Exception:
        pass
    try:
        recent_alerts = query_pg(
            "SELECT timestamp, service, level, message FROM app_logs "
            "WHERE level IN ('ERROR','CRITICAL','WARNING') "
            "ORDER BY timestamp DESC LIMIT 5"
        )
    except Exception:
        pass

    btc_price = ohlc["close"].iloc[-1] if not ohlc.empty else None
    btc_change = (ohlc["close"].iloc[-1] / ohlc["close"].iloc[0] - 1) * 100 if not ohlc.empty else None
    sent_val = sentiment["compound_score"].iloc[0] if not sentiment.empty else None
    pred_cnt = int(pred["cnt"].iloc[0]) if not pred.empty else 0

    # ── Row 1: 4 large KPI cards ──────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    if btc_price is not None:
        pc_color = "#00d4aa" if (btc_change or 0) >= 0 else "#ff4b4b"
        c1.markdown(
            f'<div class="card-compact"><div class="label">BTC/USDT</div>'
            f'<div class="value">${btc_price:,.2f}</div>'
            f'<div class="delta" style="color:{pc_color}">{btc_change:+.2f}%</div></div>' if btc_change else
            f'<div class="card-compact"><div class="label">BTC/USDT</div><div class="value">${btc_price:,.2f}</div></div>',
            unsafe_allow_html=True,
        )
    else:
        c1.markdown('<div class="card-compact"><div class="label">BTC/USDT</div><div class="value">--</div></div>', unsafe_allow_html=True)
    if sent_val is not None:
        sc = "#00d4aa" if sent_val > 0.05 else "#ffc107" if sent_val > -0.05 else "#ff4b4b"
        c2.markdown(
            f'<div class="card-compact"><div class="label">Sentiment</div>'
            f'<div class="value" style="color:{sc}">{sent_val:+.4f}</div></div>',
            unsafe_allow_html=True,
        )
    else:
        c2.markdown('<div class="card-compact"><div class="label">Sentiment</div><div class="value">--</div></div>', unsafe_allow_html=True)
    c3.markdown(f'<div class="card-compact"><div class="label">Predictions (1h)</div><div class="value">{pred_cnt}</div></div>', unsafe_allow_html=True)
    if not containers.empty:
        running = len(containers[containers["status"] == "running"])
        total = len(containers)
        cs = "#00d4aa" if running == total else "#ffc107"
        c4.markdown(
            f'<div class="card-compact"><div class="label">System</div>'
            f'<div class="value" style="color:{cs}">{running}/{total} Up</div></div>',
            unsafe_allow_html=True,
        )
    else:
        c4.markdown('<div class="card-compact"><div class="label">System</div><div class="value">--</div></div>', unsafe_allow_html=True)

    # ── Row 2: Mini charts ────────────────────────────────────
    col_l, col_m, col_r = st.columns(3)
    with col_l:
        if not ohlc.empty and len(ohlc) > 2:
            ohlc_2h = ohlc.tail(120)
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=ohlc_2h["window_start"], y=ohlc_2h["close"],
                mode="lines", line=dict(color="#f7931a", width=2),
                fill="tozeroy", fillcolor="rgba(247,147,26,0.1)",
            ))
            _apply_theme(fig, y_title="BTC Price", legend=False, height=220)
            st.plotly_chart(fig, use_container_width=True, key="summary_btc")
        else:
            st.info("Price data belum tersedia")
    with col_m:
        cpu = smon.get_cpu_latest()
        mem = smon.get_host_memory_latest()
        if cpu and mem:
            fig = go.Figure()
            fig.add_trace(go.Indicator(mode="gauge+number", value=cpu.get("usage_total", 0),
                title={"text": "CPU %"}, gauge={"axis": {"range": [0, 100]}, "bar": {"color": "#f7931a"}},
                number={"font": {"color": "#f0f2f6"}}))
            fig.update_layout(paper_bgcolor="#0f131b", font_color="#9ba3af", height=220, margin=dict(l=20, r=20, t=40, b=10))
            st.plotly_chart(fig, use_container_width=True, key="summary_cpu")
        else:
            st.info("CPU data belum tersedia")
    with col_r:
        perf = mlo.get_latest_model_performance()
        if perf:
            st.markdown(f'<div style="text-align:center;margin-top:1rem">'
                         f'<div style="font-size:0.75rem;color:#9ba3af">Model Performance</div>'
                         f'<div style="font-size:1.2rem;color:#00d4aa;font-weight:700">MAE {perf["mae"]:.6f}</div>'
                         f'<div style="font-size:0.75rem;color:#5b616e">RMSE {perf["rmse"]:.6f}</div>'
                         f'<div style="font-size:0.7rem;color:#5b616e">n={perf["n"]} | v{perf["model_version"]}</div>'
                         f'</div>', unsafe_allow_html=True)
        else:
            st.info("Model perf data belum tersedia")

    # ── Row 3: Recent alerts ──────────────────────────────────
    st.divider()
    st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Recent Alerts</div>', unsafe_allow_html=True)
    if not recent_alerts.empty:
        for _, alert in recent_alerts.iterrows():
            lvl = alert["level"]
            icon = "🔴" if lvl in ("ERROR", "CRITICAL") else "🟡"
            ts = alert["timestamp"]
            ts_str = ts.strftime("%H:%M") if hasattr(ts, "strftime") else str(ts)[:16]
            st.markdown(
                f'<div style="background:#1a1d23;border-left:3px solid '
                f'{"#ff4b4b" if lvl in ("ERROR","CRITICAL") else "#ffc107"};'
                f'padding:0.3rem 0.8rem;margin-bottom:0.3rem;font-size:0.85rem">'
                f'{icon} [{ts_str}] <b>{alert["service"]}</b>: {alert["message"]}</div>',
                unsafe_allow_html=True,
            )
    else:
        st.success("No recent alerts.")


# ═══════════════════════════════════════════════════════════════
# PAGE 1: Market Overview
# ═══════════════════════════════════════════════════════════════
def page_overview():
    ohlc = pd.DataFrame()
    hourly = pd.DataFrame()
    sent_val = None
    pred_cnt = 0

    try:
        ohlc = query_pg(
            "SELECT window_start, open, high, low, close, volume, trade_count, volatility "
            "FROM btc_ohlc_1m WHERE window_start > NOW() - INTERVAL '24 hours' "
            "ORDER BY window_start"
        )
    except Exception:
        pass
    try:
        hourly = query_pg(
            "SELECT date_trunc('hour', window_start) AS hour, "
            "AVG(close) AS avg_close, SUM(volume) AS total_volume, COUNT(*) AS trade_count "
            "FROM btc_ohlc_1m WHERE window_start > NOW() - INTERVAL '24 hours' "
            "GROUP BY 1 ORDER BY 1"
        )
    except Exception:
        pass
    try:
        s = query_pg(
            "SELECT compound_score FROM sentiment_30m ORDER BY window_start DESC LIMIT 1"
        )
        sent_val = s["compound_score"].iloc[0] if not s.empty else None
    except Exception:
        pass
    try:
        p = query_pg(
            "SELECT COUNT(*) AS cnt FROM volatility_pred "
            "WHERE window_start > NOW() - INTERVAL '1 hour'"
        )
        pred_cnt = int(p["cnt"].iloc[0]) if not p.empty else 0
    except Exception:
        pass

    total_vol = ohlc["volume"].sum() if not ohlc.empty else 0
    total_trades = ohlc["trade_count"].sum() if not ohlc.empty else 0
    vol_latest = ohlc["volatility"].iloc[-1] if not ohlc.empty and ohlc["volatility"].notna().any() else None

    # ── Row 1: 6 KPI cards ──────────────────────────────────────
    k1, k2, k3 = st.columns(3)
    if not ohlc.empty:
        lp = ohlc["close"].iloc[-1]
        pc = (ohlc["close"].iloc[-1] / ohlc["close"].iloc[0] - 1) * 100
        k1.markdown(
            f'<div class="card-compact"><div class="label">BTC/USDT</div><div class="value">${lp:,.2f}</div>'
            f'<div class="delta" style="color:{"#00d4aa" if pc >= 0 else "#ff4b4b"}">{pc:+.2f}%</div></div>',
            unsafe_allow_html=True,
        )
    else:
        k1.error("No data")
    k2.markdown(
        f'<div class="card-compact"><div class="label">Volatility</div>'
        f'<div class="value">{vol_latest:.4f}</div></div>' if vol_latest is not None
        else '<div class="card-compact"><div class="label">Volatility</div><div class="value">--</div></div>',
        unsafe_allow_html=True,
    )
    k3.markdown(
        f'<div class="card-compact"><div class="label">Volume 24h</div>'
        f'<div class="value">{total_vol:,.0f}</div></div>',
        unsafe_allow_html=True,
    )
    k4, k5, k6 = st.columns(3)
    k4.markdown(
        f'<div class="card-compact"><div class="label">Trades 24h</div>'
        f'<div class="value">{total_trades:,}</div></div>',
        unsafe_allow_html=True,
    )
    if sent_val is not None:
        sc = "#00d4aa" if sent_val > 0.05 else "#ffc107" if sent_val > -0.05 else "#ff4b4b"
        k5.markdown(
            f'<div class="card-compact"><div class="label">Sentiment</div>'
            f'<div class="value" style="color:{sc}">{sent_val:+.3f}</div></div>',
            unsafe_allow_html=True,
        )
    else:
        k5.markdown('<div class="card-compact"><div class="label">Sentiment</div><div class="value">--</div></div>', unsafe_allow_html=True)
    k6.markdown(
        f'<div class="card-compact"><div class="label">Predictions 1h</div>'
        f'<div class="value">{pred_cnt}</div></div>',
        unsafe_allow_html=True,
    )

    # ── Row 2: Candlestick + Volume bar ─────────────────────────
    col_l, col_r = st.columns([2, 1])
    with col_l:
        if not ohlc.empty and ohlc["open"].notna().any():
            fig = go.Figure()
            fig.add_trace(go.Candlestick(
                x=ohlc["window_start"],
                open=ohlc["open"], high=ohlc["high"],
                low=ohlc["low"], close=ohlc["close"],
                increasing_line_color="#00d4aa", decreasing_line_color="#ff4b4b",
                name="BTC/USDT",
            ))
            _apply_theme(fig, y_title="Price (USD)")
            fig.update_xaxes(rangeslider_visible=False)
            st.plotly_chart(fig, use_container_width=True, key="candle")
        else:
            st.info("Candlestick data tidak tersedia")

    with col_r:
        if not hourly.empty:
            fig = px.bar(
                hourly, x="hour", y="total_volume",
                color="total_volume", color_continuous_scale="Oranges",
                labels={"total_volume": "Volume"},
            )
            _apply_theme(fig, y_title="Volume")
            fig.update_layout(coloraxis_showscale=False)
            fig.update_traces(hovertemplate="%{y:,.0f}")
            st.plotly_chart(fig, use_container_width=True, key="vol_bar")
        else:
            st.info("Volume data tidak tersedia")

    # ── Row 3: Volatility timeseries + Price histogram ──────────
    col_l2, col_r2 = st.columns(2)
    with col_l2:
        if not ohlc.empty and ohlc["volatility"].notna().any():
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=ohlc["window_start"], y=ohlc["volatility"],
                mode="lines", line=dict(color="#f7931a", width=2),
                fill="tozeroy", fillcolor="rgba(247,147,26,0.15)",
                name="Volatility",
            ))
            _apply_theme(fig, y_title="Volatility")
            st.plotly_chart(fig, use_container_width=True, key="vol_ts")
        else:
            st.info("Volatility data tidak tersedia")

    with col_r2:
        if not ohlc.empty:
            fig = px.histogram(
                ohlc, x="close", nbins=30,
                color_discrete_sequence=["#f7931a"],
                labels={"close": "Price (USD)"},
            )
            _apply_theme(fig, y_title="Count")
            st.plotly_chart(fig, use_container_width=True, key="price_hist")
        else:
            st.info("Histogram tidak tersedia")


# ═══════════════════════════════════════════════════════════════
# PAGE 2: Volatility Analytics
# ═══════════════════════════════════════════════════════════════
def page_model():
    preds = pd.DataFrame()
    features = pd.DataFrame()
    scatter = pd.DataFrame()
    hourly_vol = pd.DataFrame()
    models = pd.DataFrame()

    try:
        preds = query_pg(
            "SELECT window_start, predicted_vol_5m, rolling_vol_5m, "
            "price_range_ratio, vol_ratio, inference_latency_ms "
            "FROM volatility_pred WHERE window_start > NOW() - INTERVAL '7 days' "
            "ORDER BY window_start"
        )
    except Exception:
        pass
    try:
        features = query_pg(
            "SELECT 'rolling_vol_5m' AS feature, AVG(rolling_vol_5m) AS mean_val FROM volatility_pred "
            "UNION ALL SELECT 'price_range_ratio', AVG(price_range_ratio) FROM volatility_pred "
            "UNION ALL SELECT 'vol_ratio', AVG(vol_ratio) FROM volatility_pred"
        )
    except Exception:
        pass
    try:
        scatter = query_pg(
            "SELECT volume, volatility, trade_count FROM btc_ohlc_1m "
            "WHERE window_start > NOW() - INTERVAL '24 hours' AND volatility IS NOT NULL"
        )
    except Exception:
        pass
    try:
        hourly_vol = query_pg(
            "SELECT EXTRACT(HOUR FROM window_start) AS hour, volatility "
            "FROM btc_ohlc_1m WHERE window_start > NOW() - INTERVAL '7 days' "
            "AND volatility IS NOT NULL"
        )
    except Exception:
        pass
    try:
        models = query_pg(
            "SELECT model_version, COUNT(*) AS n_preds, "
            "MAX(window_start) AS last_seen "
            "FROM volatility_pred WHERE model_version IS NOT NULL "
            "GROUP BY model_version ORDER BY last_seen DESC"
        )
    except Exception:
        pass

    mean_pred = preds["predicted_vol_5m"].mean() if not preds.empty else None
    max_vol = scatter["volatility"].max() if not scatter.empty else None
    latest_model = models["model_version"].iloc[0] if not models.empty else "—"
    total_inf = len(preds)

    # ── Row 1: 4 KPI cards ──────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(
        f'<div class="card-compact"><div class="label">Mean Pred Vol 7d</div>'
        f'<div class="value">{mean_pred:.6f}</div></div>' if mean_pred is not None
        else '<div class="card-compact"><div class="label">Mean Pred Vol 7d</div><div class="value">--</div></div>',
        unsafe_allow_html=True,
    )
    c2.markdown(
        f'<div class="card-compact"><div class="label">Max Volatility 24h</div>'
        f'<div class="value">{max_vol:.4f}</div></div>' if max_vol is not None
        else '<div class="card-compact"><div class="label">Max Volatility 24h</div><div class="value">--</div></div>',
        unsafe_allow_html=True,
    )
    c3.markdown(
        f'<div class="card-compact"><div class="label">Model Version</div>'
        f'<div class="value">{latest_model}</div></div>',
        unsafe_allow_html=True,
    )
    c4.markdown(
        f'<div class="card-compact"><div class="label">Inferences 7d</div>'
        f'<div class="value">{total_inf:,}</div></div>',
        unsafe_allow_html=True,
    )

    # ── Row 2: Predicted volatility + Feature means ─────────────
    col_l, col_r = st.columns(2)
    with col_l:
        if not preds.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=preds["window_start"], y=preds["predicted_vol_5m"],
                mode="lines", line=dict(color="#f7931a", width=2),
                name="Predicted Vol",
            ))
            _apply_theme(fig, y_title="Volatility")
            st.plotly_chart(fig, use_container_width=True, key="pred_ts")
        else:
            st.info("Prediction data belum tersedia")
    with col_r:
        if not features.empty:
            f_df = features.sort_values("mean_val")
            fig = px.bar(
                f_df, x="mean_val", y="feature", orientation="h",
                color="mean_val", color_continuous_scale="Oranges",
                labels={"mean_val": "Mean Value", "feature": "Feature"},
            )
            _apply_theme(fig, x_title="Mean Value")
            fig.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True, key="feat_bar")
        else:
            st.info("Feature data belum tersedia")

    # ── Row 3: Volume-Volatility scatter + Box plot ─────────────
    col_l2, col_r2 = st.columns(2)
    with col_l2:
        if not scatter.empty:
            fig = px.scatter(
                scatter, x="volume", y="volatility",
                size="trade_count", color="volatility",
                color_continuous_scale="Oranges",
                labels={"volume": "Volume", "volatility": "Volatility", "trade_count": "Trades"},
            )
            _apply_theme(fig, x_title="Volume", y_title="Volatility")
            fig.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True, key="vol_scatter")
        else:
            st.info("Scatter data belum tersedia")
    with col_r2:
        if not hourly_vol.empty:
            hourly_vol["hour"] = hourly_vol["hour"].astype(int)
            fig = px.box(
                hourly_vol, x="hour", y="volatility",
                labels={"hour": "Hour (UTC)", "volatility": "Volatility"},
            )
            fig.update_traces(marker_color="#f7931a")
            _apply_theme(fig, x_title="Hour (UTC)", y_title="Volatility")
            fig.update_layout(boxgap=0.3)
            st.plotly_chart(fig, use_container_width=True, key="vol_box")
        else:
            st.info("Box plot data belum tersedia")

    # ── Row 4: Predicted vs Actual + Residuals ────────────────
    st.divider()
    st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Model Prediction Accuracy</div>', unsafe_allow_html=True)

    pred_actual = mlo.get_pred_vs_actual(48)
    residuals_df = mlo.get_residuals()
    perf_latest = mlo.get_latest_model_performance()
    perf_hist = mlo.get_model_performance_history()

    if not pred_actual.empty:
        col_a, col_b = st.columns(2)
        with col_a:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=pred_actual["window_start"], y=pred_actual["predicted_vol"],
                mode="lines", line=dict(color="#f7931a", width=2), name="Predicted",
            ))
            fig.add_trace(go.Scatter(
                x=pred_actual["window_start"], y=pred_actual["actual_vol"],
                mode="lines", line=dict(color="#00d4aa", width=1.5, dash="dot"), name="Actual",
            ))
            _apply_theme(fig, y_title="Volatility")
            st.plotly_chart(fig, use_container_width=True, key="pred_vs_actual")
        with col_b:
            if not residuals_df.empty and "residual" in residuals_df.columns:
                fig = px.histogram(
                    residuals_df, x="residual", nbins=30,
                    color_discrete_sequence=["#f7931a"],
                    labels={"residual": "Residual (Actual - Predicted)"},
                )
                fig.add_vline(x=0, line_dash="dash", line_color="#ff4b4b", opacity=0.5)
                _apply_theme(fig, y_title="Count")
                st.plotly_chart(fig, use_container_width=True, key="residual_hist")
            else:
                st.info("Residual data belum tersedia")
    else:
        st.info("Predicted vs Actual data belum tersedia. Butuh data actual_vol dari model-performance-check.")

    # ── Row 5: Model performance metrics ──────────────────────
    if perf_latest:
        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.markdown(
            f'<div class="card-compact"><div class="label">Production MAE</div>'
            f'<div class="value">{perf_latest["mae"]:.6f}</div></div>',
            unsafe_allow_html=True,
        )
        col_m2.markdown(
            f'<div class="card-compact"><div class="label">Production RMSE</div>'
            f'<div class="value">{perf_latest["rmse"]:.6f}</div></div>',
            unsafe_allow_html=True,
        )
        col_m3.markdown(
            f'<div class="card-compact"><div class="label">N Predictions</div>'
            f'<div class="value">{perf_latest["n"]}</div></div>',
            unsafe_allow_html=True,
        )

    if not perf_hist.empty and len(perf_hist) > 1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=perf_hist["checked_at"], y=perf_hist["mae"],
            mode="lines+markers", line=dict(color="#f7931a", width=2), name="MAE",
        ))
        fig.add_trace(go.Scatter(
            x=perf_hist["checked_at"], y=perf_hist["rmse"],
            mode="lines+markers", line=dict(color="#ff4b4b", width=2), name="RMSE",
        ))
        _apply_theme(fig, y_title="Error", legend=True)
        st.plotly_chart(fig, use_container_width=True, key="perf_history")


# ═══════════════════════════════════════════════════════════════
# PAGE 3: Sentiment Analytics
# ═══════════════════════════════════════════════════════════════
def page_cross_source():
    sentiment = pd.DataFrame()
    latest = pd.Series(dtype="float64")

    try:
        sentiment = query_pg(
            "SELECT window_start, compound_score, positive_ratio, negative_ratio, "
            "neutral_ratio, weighted_compound, tweet_count, data_quality "
            "FROM sentiment_30m WHERE window_start > NOW() - INTERVAL '7 days' "
            "ORDER BY window_start"
        )
    except Exception:
        pass
    try:
        l = query_pg(
            "SELECT compound_score, positive_ratio, negative_ratio, neutral_ratio, "
            "weighted_compound, tweet_count, data_quality "
            "FROM sentiment_30m ORDER BY window_start DESC LIMIT 1"
        )
        if not l.empty:
            latest = l.iloc[0]
    except Exception:
        pass

    compound = latest.get("compound_score", None) if not latest.empty else None
    pos_ratio = latest.get("positive_ratio", None) if not latest.empty else None
    total_tweets = int(sentiment["tweet_count"].sum()) if not sentiment.empty else 0
    dq = latest.get("data_quality", "—") if not latest.empty else "—"

    # ── Row 1: 4 KPI cards ──────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    if compound is not None:
        sc = "#00d4aa" if compound > 0.05 else "#ffc107" if compound > -0.05 else "#ff4b4b"
        c1.markdown(
            f'<div class="card-compact"><div class="label">Compound Score</div>'
            f'<div class="value" style="color:{sc}">{compound:+.4f}</div></div>',
            unsafe_allow_html=True,
        )
    else:
        c1.markdown('<div class="card-compact"><div class="label">Compound</div><div class="value">--</div></div>', unsafe_allow_html=True)
    c2.markdown(
        f'<div class="card-compact"><div class="label">Positive Ratio</div>'
        f'<div class="value">{pos_ratio:.1%}</div></div>' if pos_ratio is not None
        else '<div class="card-compact"><div class="label">Positive Ratio</div><div class="value">--</div></div>',
        unsafe_allow_html=True,
    )
    c3.markdown(
        f'<div class="card-compact"><div class="label">Tweets (7d)</div>'
        f'<div class="value">{total_tweets:,}</div></div>',
        unsafe_allow_html=True,
    )
    dq_color = "#00d4aa" if dq == "ok" else "#ffc107" if dq == "low_sample" else "#5b616e"
    c4.markdown(
        f'<div class="card-compact"><div class="label">Data Quality</div>'
        f'<div class="value" style="color:{dq_color}">{dq}</div></div>',
        unsafe_allow_html=True,
    )

    # ── Row 2: Sentiment timeseries + Donut ─────────────────────
    col_l, col_r = st.columns(2)
    with col_l:
        if not sentiment.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=sentiment["window_start"], y=sentiment["compound_score"],
                mode="lines", line=dict(color="#f7931a", width=2),
                fill="tozeroy", fillcolor="rgba(247,147,26,0.15)",
                name="Compound Score",
            ))
            fig.add_hline(y=0.05, line_dash="dash", line_color="#00d4aa", opacity=0.5)
            fig.add_hline(y=-0.05, line_dash="dash", line_color="#ff4b4b", opacity=0.5)
            _apply_theme(fig, y_title="Compound Score")
            st.plotly_chart(fig, use_container_width=True, key="sent_ts")
        else:
            st.info("Sentiment data belum tersedia")
    with col_r:
        if compound is not None:
            neu = latest.get("neutral_ratio", 1 - pos_ratio - 0) if not latest.empty else 0
            neg = latest.get("negative_ratio", 0) if not latest.empty else 0
            fig = go.Figure()
            fig.add_trace(go.Pie(
                labels=["Positive", "Negative", "Neutral"],
                values=[pos_ratio or 0, neg, max(0, 1 - (pos_ratio or 0) - neg)],
                hole=0.4,
                marker_colors=["#00d4aa", "#ff4b4b", "#ffc107"],
                textinfo="label+percent",
            ))
            fig.update_layout(
                paper_bgcolor="#0f131b", font_color="#9ba3af", font_size=11,
                margin=dict(l=10, r=10, t=10, b=10),
                legend=dict(orientation="h", y=-0.1, font_size=10),
            )
            st.plotly_chart(fig, use_container_width=True, key="sent_donut")
        else:
            st.info("Data donut belum tersedia")

    # ── Row 3: Tweet volume + Weighted vs Unweighted ────────────
    col_l2, col_r2 = st.columns(2)
    with col_l2:
        if not sentiment.empty:
            fig = px.bar(
                sentiment, x="window_start", y="tweet_count",
                color="tweet_count", color_continuous_scale="Blues",
                labels={"tweet_count": "Tweets", "window_start": "Window"},
            )
            _apply_theme(fig, y_title="Tweet Count")
            fig.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True, key="tweet_bar")
        else:
            st.info("Tweet volume data belum tersedia")
    with col_r2:
        if not sentiment.empty and "weighted_compound" in sentiment.columns:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=sentiment["window_start"], y=sentiment["compound_score"],
                mode="lines", line=dict(color="#f7931a", width=2),
                name="Unweighted",
            ))
            fig.add_trace(go.Scatter(
                x=sentiment["window_start"], y=sentiment["weighted_compound"],
                mode="lines", line=dict(color="#00d4aa", width=2, dash="dot"),
                name="Weighted",
            ))
            _apply_theme(fig, y_title="Compound Score")
            st.plotly_chart(fig, use_container_width=True, key="sent_compare")
        else:
            st.info("Weighted comparison belum tersedia")

    # ── Row 4: Trino Federated Query (existing) ─────────────────
    st.divider()
    st.markdown(
        '<div style="color:#9ba3af;font-size:0.85rem;margin-bottom:0.5rem">'
        "Federated Query — PostgreSQL OHLC ⨯ MinIO Parquet (Trino)</div>",
        unsafe_allow_html=True,
    )
    if st.button("Jalankan Query Federasi", use_container_width=True):
        with st.spinner("Querying Trino..."):
            try:
                sql = """
                SELECT o.window_start, o.close, o.volatility,
                       t.compound AS avg_tweet_sentiment, t.tweet_count
                FROM postgresql.public.btc_ohlc_1m o
                LEFT JOIN (
                    SELECT date_trunc('minute', created_at) AS tweet_minute,
                           AVG(compound) AS compound, COUNT(*) AS tweet_count
                    FROM hive.twitter_raw.tweets
                    WHERE created_at > NOW() - INTERVAL '1' HOUR
                    GROUP BY 1
                ) t ON o.window_start = t.tweet_minute
                WHERE o.window_start > NOW() - INTERVAL '1' HOUR
                ORDER BY o.window_start
                """
                df = query_trino(sql)
                if df.empty:
                    st.info("Query succeeded but no data returned. Run sentiment pipeline first.")
                else:
                    st.markdown(f'<div style="color:#00d4aa">{len(df)} rows.</div>', unsafe_allow_html=True)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    if "close" in df.columns and "avg_tweet_sentiment" in df.columns:
                        valid = df.dropna(subset=["avg_tweet_sentiment"])
                        if len(valid) > 2:
                            fig = px.scatter(
                                valid, x="avg_tweet_sentiment", y="close",
                                trendline="ols",
                                labels={"avg_tweet_sentiment": "Avg Sentiment", "close": "BTC Close"},
                            )
                            fig.update_traces(marker_color="#f7931a", marker_size=6)
                            _apply_theme(fig, x_title="Avg Sentiment", y_title="Price")
                            st.plotly_chart(fig, use_container_width=True, key="federated_scatter")
                            corr = valid["avg_tweet_sentiment"].corr(valid["close"])
                            st.markdown(
                                f'<div class="card-compact"><div class="label">Sentiment-Price Correlation</div>'
                                f'<div class="value">{corr:.4f}</div></div>',
                                unsafe_allow_html=True,
                            )
            except Exception as e:
                st.error(f"Trino: {e}")

    with st.expander("SQL Explorer"):
        sql_input = st.text_area("Write any Trino SQL", height=80,
                                 value="SELECT * FROM hive.twitter_raw.tweets LIMIT 10")
        if st.button("Jalankan", key="run_sql_trino"):
            with st.spinner("..."):
                try:
                    df = query_trino(sql_input)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"Query error: {e}")


# ═══════════════════════════════════════════════════════════════
# PAGE 4: Pipeline Operations
# ═══════════════════════════════════════════════════════════════
def page_lineage():
    lineage = pd.DataFrame()
    pstats = pd.DataFrame()
    latency = pd.DataFrame()

    try:
        lineage = query_pg(
            "SELECT run_id, pipeline_name, source, target_table, "
            "rows_processed, rows_rejected, quality_status, started_at, finished_at "
            "FROM pipeline_lineage ORDER BY finished_at DESC NULLS LAST LIMIT 100"
        )
    except Exception:
        pass
    try:
        pstats = query_pg(
            "SELECT pipeline_name, SUM(rows_processed) AS total_rows, COUNT(*) AS runs, "
            "ROUND(SUM(CASE WHEN quality_status='ok' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) "
            "AS success_rate "
            "FROM pipeline_lineage GROUP BY pipeline_name"
        )
    except Exception:
        pass
    try:
        latency = query_pg(
            "SELECT window_start, inference_latency_ms, model_version "
            "FROM volatility_pred "
            "WHERE window_start > NOW() - INTERVAL '7 days' AND inference_latency_ms IS NOT NULL "
            "ORDER BY window_start"
        )
    except Exception:
        pass

    total_rows = int(pstats["total_rows"].sum()) if not pstats.empty else 0
    avg_sr = pstats["success_rate"].mean() if not pstats.empty else None
    n_pipelines = len(pstats) if not pstats.empty else 0
    last_run = lineage["finished_at"].dropna().iloc[0] if not lineage.empty and lineage["finished_at"].notna().any() else None

    # ── Row 1: 4 KPI cards ──────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(
        f'<div class="card-compact"><div class="label">Rows Processed</div>'
        f'<div class="value">{total_rows:,}</div></div>',
        unsafe_allow_html=True,
    )
    c2.markdown(
        f'<div class="card-compact"><div class="label">Success Rate</div>'
        f'<div class="value">{avg_sr:.1f}%</div></div>' if avg_sr is not None
        else '<div class="card-compact"><div class="label">Success Rate</div><div class="value">--</div></div>',
        unsafe_allow_html=True,
    )
    c3.markdown(
        f'<div class="card-compact"><div class="label">Pipelines</div>'
        f'<div class="value">{n_pipelines}</div></div>',
        unsafe_allow_html=True,
    )
    if last_run is not None:
        age = datetime.now(last_run.tzinfo) - last_run if last_run.tzinfo else datetime.now() - last_run
        age_str = f"{int(age.total_seconds() // 60)}m ago" if age.total_seconds() < 3600 else f"{age.total_seconds() / 3600:.1f}h ago"
    else:
        age_str = "--"
    c4.markdown(
        f'<div class="card-compact"><div class="label">Last Run</div>'
        f'<div class="value">{age_str}</div></div>',
        unsafe_allow_html=True,
    )

    # ── Row 2: Bar chart + Donut ────────────────────────────────
    col_l, col_r = st.columns(2)
    with col_l:
        if not pstats.empty:
            fig = px.bar(
                pstats, x="pipeline_name", y="total_rows",
                color="pipeline_name",
                labels={"pipeline_name": "Pipeline", "total_rows": "Rows"},
            )
            _apply_theme(fig, y_title="Rows Processed")
            fig.update_layout(showlegend=False)
            fig.update_traces(marker_color="#f7931a")
            st.plotly_chart(fig, use_container_width=True, key="pipe_bar")
        else:
            st.info("Pipeline stats belum tersedia")
    with col_r:
        if not lineage.empty:
            qc = lineage["quality_status"].value_counts()
            fig = go.Figure()
            fig.add_trace(go.Pie(
                labels=qc.index.tolist(), values=qc.values.tolist(),
                hole=0.4,
                marker_colors=["#00d4aa", "#ffc107", "#ff4b4b"],
                textinfo="label+percent",
            ))
            fig.update_layout(
                paper_bgcolor="#0f131b", font_color="#9ba3af", font_size=11,
                margin=dict(l=10, r=10, t=10, b=10), showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, key="pipe_donut")
        else:
            st.info("Quality data belum tersedia")

    # ── Tabs: Lineage + Audit ──────────────────────────────────
    tab_a, tab_b = st.tabs(["Pipeline Lineage", "Audit Log"])

    with tab_a:
        # Row 3: Timeline + Latency
        ca, cb = st.columns(2)
        with ca:
            if not lineage.empty and lineage["started_at"].notna().any():
                tl = lineage.dropna(subset=["started_at", "finished_at"]).copy()
                if not tl.empty:
                    fig = px.timeline(
                        tl, x_start="started_at", x_end="finished_at",
                        y="pipeline_name", color="quality_status",
                        labels={"pipeline_name": "Pipeline", "quality_status": "Status"},
                    )
                    _apply_theme(fig, legend=True)
                    fig.update_yaxes(showgrid=False)
                    st.plotly_chart(fig, use_container_width=True, key="timeline")
            else:
                st.info("Timeline data belum tersedia")
        with cb:
            if not latency.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=latency["window_start"], y=latency["inference_latency_ms"],
                    mode="lines", line=dict(color="#00d4aa", width=2),
                    name="Latency",
                ))
                _apply_theme(fig, y_title="Latency (ms)")
                st.plotly_chart(fig, use_container_width=True, key="latency_ts")
            else:
                st.info("Latency data belum tersedia")
        # Row 4: Table
        if not lineage.empty:
            st.dataframe(lineage, use_container_width=True, hide_index=True)
            st.download_button("Download Lineage CSV", lineage.to_csv(index=False),
                               file_name="pipeline_lineage.csv", mime="text/csv", key="dl_lineage")

    with tab_b:
        try:
            audit = query_pg(
                "SELECT id, table_name, operation, changed_by, changed_at "
                "FROM audit_log ORDER BY changed_at DESC LIMIT 100"
            )
            if not audit.empty:
                st.dataframe(audit, use_container_width=True, hide_index=True)
                st.download_button("Download Audit CSV", audit.to_csv(index=False),
                                   file_name="audit_log.csv", mime="text/csv", key="dl_audit")
                ops = audit["operation"].value_counts()
                fig = go.Figure()
                fig.add_trace(go.Pie(
                    labels=ops.index.tolist(), values=ops.values.tolist(),
                    marker_colors=["#f7931a", "#00d4aa", "#ffc107"],
                    textinfo="label+percent",
                ))
                fig.update_layout(
                    paper_bgcolor="#0f131b", font_color="#9ba3af", font_size=11,
                    margin=dict(l=10, r=10, t=10, b=10),
                    legend=dict(orientation="h", y=-0.15, font_size=10),
                )
                st.plotly_chart(fig, use_container_width=True, key="audit_chart")
            else:
                st.info("No audit data available.")
        except Exception as e:
            st.error(f"Audit: {e}")


# ═══════════════════════════════════════════════════════════════
# PAGE 5: Operations (System Health + Data Quality)
# ═══════════════════════════════════════════════════════════════
def page_operations():
    tab_sys, tab_dq, tab_dc, tab_log = st.tabs(["System Health", "Data Quality", "Data Catalog", "Logs"])

    with tab_sys:
        _tab_system_health()

    with tab_dq:
        _tab_data_quality()

    with tab_dc:
        _tab_data_catalog()

    with tab_log:
        _tab_logs()


def _tab_system_health():
    telegraf_available = smon.get_telegraf_available()

    if not telegraf_available:
        st.warning("Telegraf metrics belum tersedia. Tunggu beberapa menit setelah container telegraf berjalan.")
        return

    cpu_latest = smon.get_cpu_latest()
    mem_latest = smon.get_host_memory_latest()
    disk_latest = smon.get_host_disk_latest()
    container_status = smon.get_container_status()
    container_mem = smon.get_container_memory_latest()
    cpu_hist = smon.get_host_cpu()
    mem_hist = smon.get_host_memory()
    disk_hist = smon.get_host_disk()
    container_cpu_hist = smon.get_container_cpu()
    container_mem_hist = smon.get_container_memory()

    # ── Row 1: 4 KPI cards (Host resources) ───────────────────
    c1, c2, c3, c4 = st.columns(4)

    cpu_val = cpu_latest.get("usage_total", None)
    if cpu_val is not None:
        cpu_color = "#00d4aa" if cpu_val < 50 else "#ffc107" if cpu_val < 80 else "#ff4b4b"
        c1.markdown(
            f'<div class="card-compact"><div class="label">Host CPU</div>'
            f'<div class="value" style="color:{cpu_color}">{cpu_val:.1f}%</div></div>',
            unsafe_allow_html=True,
        )
    else:
        c1.markdown('<div class="card-compact"><div class="label">Host CPU</div><div class="value">--</div></div>', unsafe_allow_html=True)

    mem_pct = mem_latest.get("used_percent", None)
    if mem_pct is not None:
        mem_color = "#00d4aa" if mem_pct < 60 else "#ffc107" if mem_pct < 85 else "#ff4b4b"
        used_gb = mem_latest.get("used", 0) / (1024**3)
        total_gb = mem_latest.get("total", 1) / (1024**3)
        c2.markdown(
            f'<div class="card-compact"><div class="label">Host Memory</div>'
            f'<div class="value" style="color:{mem_color}">{mem_pct:.1f}%</div>'
            f'<div class="delta" style="color:#9ba3af">{used_gb:.1f} / {total_gb:.1f} GB</div></div>',
            unsafe_allow_html=True,
        )
    else:
        c2.markdown('<div class="card-compact"><div class="label">Host Memory</div><div class="value">--</div></div>', unsafe_allow_html=True)

    disk_pct = disk_latest.get("used_percent", None)
    if disk_pct is not None:
        disk_color = "#00d4aa" if disk_pct < 70 else "#ffc107" if disk_pct < 90 else "#ff4b4b"
        used_gb = disk_latest.get("used", 0) / (1024**3)
        total_gb = disk_latest.get("total", 1) / (1024**3)
        c3.markdown(
            f'<div class="card-compact"><div class="label">Host Disk</div>'
            f'<div class="value" style="color:{disk_color}">{disk_pct:.1f}%</div>'
            f'<div class="delta" style="color:#9ba3af">{used_gb:.1f} / {total_gb:.1f} GB</div></div>',
            unsafe_allow_html=True,
        )
    else:
        c3.markdown('<div class="card-compact"><div class="label">Host Disk</div><div class="value">--</div></div>', unsafe_allow_html=True)

    if not container_status.empty:
        running = len(container_status[container_status["status"] == "running"])
        total = len(container_status)
        stopped = total - running
        cs_color = "#00d4aa" if stopped == 0 else "#ffc107" if stopped <= 2 else "#ff4b4b"
        c4.markdown(
            f'<div class="card-compact"><div class="label">Containers</div>'
            f'<div class="value" style="color:{cs_color}">{running}/{total} Up</div>'
            f'<div class="delta" style="color:#ff4b4b">{"{} stopped".format(stopped) if stopped > 0 else ""}</div></div>',
            unsafe_allow_html=True,
        )
    else:
        c4.markdown('<div class="card-compact"><div class="label">Containers</div><div class="value">--</div></div>', unsafe_allow_html=True)

    # ── Row 2: CPU + Memory timeseries ────────────────────────
    col_l, col_r = st.columns(2)
    with col_l:
        if not cpu_hist.empty and len(cpu_hist) > 1:
            cpu_hist["usage"] = 100 - cpu_hist["usage_idle"]
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=cpu_hist["time"], y=cpu_hist["usage"],
                mode="lines", line=dict(color="#f7931a", width=2),
                fill="tozeroy", fillcolor="rgba(247,147,26,0.15)",
                name="CPU",
            ))
            fig.add_hline(y=80, line_dash="dash", line_color="#ffc107", opacity=0.5)
            fig.add_hline(y=90, line_dash="dash", line_color="#ff4b4b", opacity=0.5)
            _apply_theme(fig, y_title="CPU %", legend=False)
            st.plotly_chart(fig, use_container_width=True, key="host_cpu_ts")
        else:
            st.info("CPU history belum tersedia")
    with col_r:
        if not mem_hist.empty and len(mem_hist) > 1:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=mem_hist["time"], y=mem_hist["used_percent"],
                mode="lines", line=dict(color="#00d4aa", width=2),
                fill="tozeroy", fillcolor="rgba(0,212,170,0.15)",
                name="Memory",
            ))
            fig.add_hline(y=85, line_dash="dash", line_color="#ff4b4b", opacity=0.5)
            _apply_theme(fig, y_title="Memory %", legend=False)
            st.plotly_chart(fig, use_container_width=True, key="host_mem_ts")
        else:
            st.info("Memory history belum tersedia")

    # ── Row 3: Container health table + Disk gauge ────────────
    col_l2, col_r2 = st.columns([3, 2])
    with col_l2:
        st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Container Status</div>', unsafe_allow_html=True)
        if not container_status.empty and not container_mem.empty:
            merged = container_status.merge(container_mem[["container_name", "usage_percent", "usage", "limit"]],
                                            on="container_name", how="left")
            display = merged.copy()
            display["Status"] = display["status"].apply(
                lambda s: "✅" if s == "running" else "❌" if s == "exited" else "🔄"
            )
            display["Memory"] = display.apply(
                lambda r: f"{r['usage_percent']:.1f}%" if pd.notna(r.get("usage_percent")) else "—", axis=1
            )
            display = display[["Status", "container_name", "Memory"]].rename(
                columns={"container_name": "Container"}
            )
            st.dataframe(display, use_container_width=True, hide_index=True, height=400)
        elif not container_status.empty:
            display = container_status.copy()
            display["Status"] = display["status"].apply(
                lambda s: "✅" if s == "running" else "❌" if s == "exited" else "🔄"
            )
            display = display[["Status", "container_name"]].rename(columns={"container_name": "Container"})
            st.dataframe(display, use_container_width=True, hide_index=True, height=400)
        else:
            st.info("Container data belum tersedia")
    with col_r2:
        st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Resource Gauges</div>', unsafe_allow_html=True)
        gc1, gc2, gc3 = st.columns(3)
        if cpu_val is not None:
            fig = go.Figure()
            fig.add_trace(go.Indicator(
                mode="gauge+number",
                value=cpu_val,
                title={"text": "CPU %", "font": {"color": "#9ba3af"}},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "#f7931a"},
                    "steps": [
                        {"range": [0, 50], "color": "#1a3a2a"},
                        {"range": [50, 80], "color": "#3a351a"},
                        {"range": [80, 100], "color": "#3a1a1a"},
                    ],
                },
                number={"font": {"color": "#f0f2f6"}},
            ))
            fig.update_layout(paper_bgcolor="#0f131b", font_color="#9ba3af", height=200, margin=dict(l=10, r=10, t=30, b=10))
            gc1.plotly_chart(fig, use_container_width=True, key="cpu_gauge")
        if mem_pct is not None:
            fig = go.Figure()
            fig.add_trace(go.Indicator(
                mode="gauge+number",
                value=mem_pct,
                title={"text": "Memory %", "font": {"color": "#9ba3af"}},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "#00d4aa"},
                    "steps": [
                        {"range": [0, 60], "color": "#1a3a2a"},
                        {"range": [60, 85], "color": "#3a351a"},
                        {"range": [85, 100], "color": "#3a1a1a"},
                    ],
                },
                number={"font": {"color": "#f0f2f6"}},
            ))
            fig.update_layout(paper_bgcolor="#0f131b", font_color="#9ba3af", height=200, margin=dict(l=10, r=10, t=30, b=10))
            gc2.plotly_chart(fig, use_container_width=True, key="mem_gauge")
        if disk_pct is not None:
            fig = go.Figure()
            fig.add_trace(go.Indicator(
                mode="gauge+number",
                value=disk_pct,
                title={"text": "Disk %", "font": {"color": "#9ba3af"}},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "#f7931a"},
                    "steps": [
                        {"range": [0, 70], "color": "#1a3a2a"},
                        {"range": [70, 90], "color": "#3a351a"},
                        {"range": [90, 100], "color": "#3a1a1a"},
                    ],
                },
                number={"font": {"color": "#f0f2f6"}},
            ))
            fig.update_layout(paper_bgcolor="#0f131b", font_color="#9ba3af", height=200, margin=dict(l=10, r=10, t=30, b=10))
            gc3.plotly_chart(fig, use_container_width=True, key="disk_gauge")

    # ── Row 4: Container resource trends ──────────────────────
    col_l3, col_r3 = st.columns(2)
    with col_l3:
        if not container_cpu_hist.empty and len(container_cpu_hist) > 1:
            top5 = container_cpu_hist["container_name"].value_counts().head(5).index.tolist()
            fig = go.Figure()
            for name in top5:
                subset = container_cpu_hist[container_cpu_hist["container_name"] == name]
                if len(subset) > 1:
                    fig.add_trace(go.Scatter(
                        x=subset["time"], y=subset["usage_percent"],
                        mode="lines", name=name, line=dict(width=1.5),
                    ))
            _apply_theme(fig, y_title="CPU %", legend=True)
            st.plotly_chart(fig, use_container_width=True, key="container_cpu_ts")
        else:
            st.info("Container CPU data belum tersedia")
    with col_r3:
        if not container_mem_hist.empty and len(container_mem_hist) > 1:
            top5 = container_mem_hist["container_name"].value_counts().head(5).index.tolist()
            fig = go.Figure()
            for name in top5:
                subset = container_mem_hist[container_mem_hist["container_name"] == name]
                if len(subset) > 1:
                    fig.add_trace(go.Scatter(
                        x=subset["time"], y=subset["usage_percent"],
                        mode="lines", name=name, line=dict(width=1.5),
                    ))
            _apply_theme(fig, y_title="Memory %", legend=True)
            st.plotly_chart(fig, use_container_width=True, key="container_mem_ts")
        else:
            st.info("Container memory data belum tersedia")


def _tab_data_quality():
    dq_summary = dq.get_dq_summary()
    dq_latest = dq.get_latest_dq_stats()
    dq_history = dq.get_dq_history()
    null_heatmap = dq.get_null_heatmap()
    freshness = dq.get_freshness()

    # ── Row 1: 4 KPI cards ───────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)

    if not dq_summary.empty:
        avg_completeness = dq_summary["completeness_pct"].mean()
        cc = "#00d4aa" if avg_completeness >= 99 else "#ffc107" if avg_completeness >= 95 else "#ff4b4b"
        c1.markdown(
            f'<div class="card-compact"><div class="label">Completeness</div>'
            f'<div class="value" style="color:{cc}">{avg_completeness:.1f}%</div></div>',
            unsafe_allow_html=True,
        )
    else:
        c1.markdown('<div class="card-compact"><div class="label">Completeness</div><div class="value">--</div></div>', unsafe_allow_html=True)

    total_tables = len(dq_summary) if not dq_summary.empty else 0
    c2.markdown(
        f'<div class="card-compact"><div class="label">Tables Profiled</div>'
        f'<div class="value">{total_tables}</div></div>',
        unsafe_allow_html=True,
    )

    if not dq_history.empty:
        last_pass = dq_history["quality_status"].iloc[0] if not dq_history.empty else "unknown"
        lc = "#00d4aa" if last_pass == "ok" else "#ff4b4b"
        c3.markdown(
            f'<div class="card-compact"><div class="label">Last Validation</div>'
            f'<div class="value" style="color:{lc}">{last_pass.upper()}</div></div>',
            unsafe_allow_html=True,
        )
    else:
        c3.markdown('<div class="card-compact"><div class="label">Last Validation</div><div class="value">--</div></div>', unsafe_allow_html=True)

    type_errors_total = int(dq_summary["total_type_errors"].sum()) if not dq_summary.empty else 0
    tec = "#00d4aa" if type_errors_total == 0 else "#ff4b4b"
    c4.markdown(
        f'<div class="card-compact"><div class="label">Type Errors</div>'
        f'<div class="value" style="color:{tec}">{type_errors_total}</div></div>',
        unsafe_allow_html=True,
    )

    # ── Row 2: Null heatmap + Freshness ───────────────────────
    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Null % Heatmap</div>', unsafe_allow_html=True)
        if not null_heatmap.empty:
            pivot = null_heatmap.pivot(index="table_name", columns="column_name", values="null_percent")

            fig = px.imshow(
                pivot, text_auto=".1f",
                color_continuous_scale=[(0, "#00d4aa"), (0.5, "#ffc107"), (1, "#ff4b4b")],
                range_color=[0, 100],
                labels={"x": "Column", "y": "Table", "color": "Null %"},
            )
            fig.update_layout(
                paper_bgcolor="#0f131b", font_color="#9ba3af", font_size=10,
                margin=dict(l=10, r=10, t=10, b=10),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig, use_container_width=True, key="null_heatmap")
        else:
            st.info("Profiling data belum tersedia. Tunggu Prefect data-quality-check berjalan.")

    with col_r:
        st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Data Freshness</div>', unsafe_allow_html=True)
        if not freshness.empty and freshness["last_data_at"].notna().any():
            now = datetime.now(timezone.utc)
            display_rows = []
            for _, row in freshness.iterrows():
                table = row["table_name"]
                last_data = row["last_data_at"]
                if last_data is not None:
                    if hasattr(last_data, "tzinfo") and last_data.tzinfo:
                        age_mins = (now - last_data).total_seconds() / 60
                    else:
                        age_mins = (datetime.now(timezone.utc) - last_data).total_seconds() / 60
                    icon = "✅" if age_mins < 30 else "🟡" if age_mins < 120 else "🔴"
                    display_rows.append({
                        "Table": table,
                        "Last Data": last_data.strftime("%H:%M:%S") if hasattr(last_data, "strftime") else str(last_data)[:19],
                        "Age": f"{int(age_mins)}m ago" if age_mins < 120 else f"{age_mins/60:.1f}h ago",
                        "": icon,
                    })
            if display_rows:
                disp_df = pd.DataFrame(display_rows)
                st.dataframe(disp_df, use_container_width=True, hide_index=True, height=250)
            else:
                st.info("Freshness data kosong.")
        else:
            st.info("Freshness data belum tersedia.")

    # ── Row 3: Profiling details table ────────────────────────
    st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Column Profiling Stats</div>', unsafe_allow_html=True)
    if not dq_latest.empty:
        display = dq_latest.copy()
        display = display.rename(columns={
            "table_name": "Table", "column_name": "Column",
            "null_count": "Nulls", "total_rows": "Total Rows",
            "null_percent": "Null %", "distinct_count": "Distinct",
            "type_mismatch": "Type Err",
            "min_value": "Min", "max_value": "Max",
            "mean_value": "Mean", "quality_status": "Quality",
        })
        display["Null %"] = display["Null %"].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "—")
        display["Min"] = display["Min"].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "—")
        display["Max"] = display["Max"].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "—")
        display["Mean"] = display["Mean"].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "—")
        st.dataframe(display, use_container_width=True, hide_index=True, height=350)
        st.download_button("Download Profiling CSV", dq_latest.to_csv(index=False),
                           file_name="data_quality_stats.csv", mime="text/csv", key="dl_dq_prof")
    else:
        st.info("Profiling data belum tersedia.")

    # ── Row 4: Validation history timeline ────────────────────
    if not dq_history.empty:
        st.divider()
        st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Validation History</div>', unsafe_allow_html=True)
        col_a, col_b = st.columns([2, 1])
        with col_a:
            if dq_history["started_at"].notna().any():
                fig = px.scatter(
                    dq_history, x="started_at", y="quality_status",
                    color="quality_status",
                    color_discrete_map={"ok": "#00d4aa", "failed": "#ff4b4b"},
                    size_max=10,
                    labels={"started_at": "Time", "quality_status": "Result"},
                )
                _apply_theme(fig, y_title="Result", legend=False)
                st.plotly_chart(fig, use_container_width=True, key="dq_history_ts")
        with col_b:
            if not dq_history.empty:
                total_runs = len(dq_history)
                passed_runs = len(dq_history[dq_history["quality_status"] == "ok"])
                rate = (passed_runs / total_runs * 100) if total_runs > 0 else 0
                fig = go.Figure()
                fig.add_trace(go.Pie(
                    labels=["Passed", "Failed"],
                    values=[passed_runs, total_runs - passed_runs],
                    hole=0.5,
                    marker_colors=["#00d4aa", "#ff4b4b"],
                    textinfo="label+value",
                ))
                fig.update_layout(
                    paper_bgcolor="#0f131b", font_color="#9ba3af", font_size=11,
                    margin=dict(l=10, r=10, t=10, b=10), showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True, key="dq_donut")
                st.markdown(
                    f'<div style="text-align:center;color:#9ba3af;font-size:0.85rem">'
                    f'{passed_runs}/{total_runs} runs passed ({rate:.0f}%)</div>',
                    unsafe_allow_html=True,
                )
    else:
        st.info("Validation history belum tersedia.")


def _tab_data_catalog():
    tables = dc.get_all_tables()
    glossary = dc.get_business_glossary()
    edges = dc.get_lineage_edges()

    # ── Row 1: Lineage Graph (Sankey) ──────────────────────────
    st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Data Lineage</div>', unsafe_allow_html=True)
    if edges:
        all_nodes = set()
        for e in edges:
            all_nodes.add(e["source"])
            all_nodes.add(e["target"])
        node_list = sorted(all_nodes)
        node_idx = {n: i for i, n in enumerate(node_list)}
        source_idx = [node_idx[e["source"]] for e in edges]
        target_idx = [node_idx[e["target"]] for e in edges]
        values = [1] * len(edges)
        colors = ["rgba(247,147,26,0.3)"] * len(edges)

        fig = go.Figure()
        fig.add_trace(go.Sankey(
            node=dict(pad=15, thickness=15, line=dict(color="#2d3139", width=0.5),
                      label=node_list, color="#f7931a"),
            link=dict(source=source_idx, target=target_idx, value=values, color=colors),
        ))
        fig.update_layout(paper_bgcolor="#0f131b", font_color="#9ba3af", font_size=10,
                          margin=dict(l=10, r=10, t=10, b=10), height=420)
        st.plotly_chart(fig, use_container_width=True, key="lineage_sankey_top")
    else:
        st.info("Lineage edges belum tersedia.")

    # ── Row 2: Table Browser ───────────────────────────────────
    st.divider()
    st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Table Browser</div>', unsafe_allow_html=True)

    if tables:
        selected_table = st.selectbox("Pilih tabel", tables, key="catalog_table_select")

        col_a, col_b = st.columns([1, 2])
        with col_a:
            meta_df = dc.get_table_metadata()
            if not meta_df.empty:
                row = meta_df[meta_df["table_name"] == selected_table]
                if not row.empty:
                    r = row.iloc[0]
                    st.markdown("**Deskripsi:**")
                    st.markdown(f'<div style="color:#9ba3af;font-size:0.85rem">{r["description"]}</div>', unsafe_allow_html=True)
                    st.markdown(f'<div style="font-size:0.85rem;margin-top:0.5rem"><b>Owner:</b> {r["owner"]}</div>', unsafe_allow_html=True)
                    st.markdown(f'<div style="font-size:0.85rem"><b>Sensitivity:</b> {r["sensitivity"]}</div>', unsafe_allow_html=True)
                    st.markdown(f'<div style="font-size:0.85rem"><b>Refresh:</b> {r["refresh_frequency"]}</div>', unsafe_allow_html=True)
                    st.markdown(f'<div style="font-size:0.85rem"><b>Source:</b> {r["source_system"]}</div>', unsafe_allow_html=True)
                    if r["retention_days"] is not None:
                        st.markdown(f'<div style="font-size:0.85rem"><b>Retention:</b> {int(r["retention_days"])} days</div>', unsafe_allow_html=True)
                else:
                    st.info("Metadata belum tersedia untuk tabel ini.")
            else:
                st.info("table_metadata belum diisi.")

        with col_b:
            col_df = dc.get_column_info(selected_table)
            if not col_df.empty:
                display = col_df.rename(columns={
                    "column_name": "Column", "data_type": "Type",
                    "is_nullable": "Nullable", "comment": "Description",
                })
                display["Description"] = display["Description"].fillna("—")
                st.dataframe(display, use_container_width=True, hide_index=True,
                             column_config={"Description": st.column_config.TextColumn(width="large")})
            else:
                st.info("information_schema belum tersedia.")
    else:
        st.info("Tidak ada tabel ditemukan di database.")

    # ── Row 2.5: Column Lineage (selected table) ──────────────
    if tables and selected_table:
        st.divider()
        st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Column-Level Lineage</div>', unsafe_allow_html=True)

        col_up, col_down = st.columns(2)
        with col_up:
            upstream = dc.get_upstream_columns(selected_table)
            if not upstream.empty:
                st.markdown(f'<div style="color:#9ba3af;font-size:0.8rem">Columns feeding <b>{selected_table}</b></div>', unsafe_allow_html=True)
                display = upstream.rename(columns={
                    "source_table": "Source Table", "source_column": "Source Column",
                    "target_column": "→ Target Column", "transformation": "Transform",
                })
                st.dataframe(display, use_container_width=True, hide_index=True, height=220)
            else:
                st.info(f"No upstream lineage for {selected_table}")

        with col_down:
            downstream = dc.get_downstream_columns(selected_table)
            if not downstream.empty:
                st.markdown(f'<div style="color:#9ba3af;font-size:0.8rem">Columns consuming from <b>{selected_table}</b></div>', unsafe_allow_html=True)
                display = downstream.rename(columns={
                    "target_table": "Target Table", "target_column": "Target Column",
                    "source_column": "← Source Col", "transformation": "Transform",
                })
                st.dataframe(display, use_container_width=True, hide_index=True, height=220)
            else:
                st.info(f"No downstream lineage for {selected_table}")

    # ── Row 3: Business Glossary ──────────────────────────────
    if not glossary.empty:
        st.divider()
        st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Business Glossary</div>', unsafe_allow_html=True)
        st.dataframe(glossary, use_container_width=True, hide_index=True,
                     column_config={"definition": st.column_config.TextColumn(width="large")})
        st.download_button("Download Glossary CSV", glossary.to_csv(index=False),
                           file_name="business_glossary.csv", mime="text/csv", key="dl_glossary")


def _tab_logs():
    st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Log Explorer</div>', unsafe_allow_html=True)

    services = logexp.get_log_services()
    col_f1, col_f2 = st.columns([1, 1])
    with col_f1:
        sel_service = st.selectbox("Service", ["All"] + services, key="log_svc")
    with col_f2:
        sel_level = st.selectbox("Level", ["All", "INFO", "WARNING", "ERROR", "CRITICAL"], key="log_lvl")

    svc = None if sel_service == "All" else sel_service
    lvl = None if sel_level == "All" else sel_level

    logs_df = logexp.get_app_logs(service=svc, level=lvl, limit=200)

    # ── KPI row ──────────────────────────────────────────────
    if not logs_df.empty:
        errors = len(logs_df[logs_df["level"] == "ERROR"])
        warnings = len(logs_df[logs_df["level"] == "WARNING"])
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="card-compact"><div class="label">Total Logs</div><div class="value">{len(logs_df)}</div></div>', unsafe_allow_html=True)
        ec = "#00d4aa" if errors == 0 else "#ff4b4b"
        c2.markdown(f'<div class="card-compact"><div class="label">Errors</div><div class="value" style="color:{ec}">{errors}</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="card-compact"><div class="label">Warnings</div><div class="value">{warnings}</div></div>', unsafe_allow_html=True)

    col_l, col_r = st.columns([3, 1])
    with col_l:
        if not logs_df.empty:
            display = logs_df.copy()
            display["timestamp"] = display["timestamp"].apply(
                lambda t: t.strftime("%H:%M:%S") if hasattr(t, "strftime") else str(t)[:19]
            )
            st.dataframe(display, use_container_width=True, hide_index=True, height=400,
                         column_config={"message": st.column_config.TextColumn(width="large")})
            st.download_button("Download Logs CSV", logs_df.to_csv(index=False),
                               file_name="app_logs.csv", mime="text/csv", key="dl_logs")
        else:
            st.info("Tidak ada log. Tunggu log-ingester flow berjalan.")

    with col_r:
        level_counts = logexp.get_log_level_counts()
        if not level_counts.empty:
            colors = {"INFO": "#00d4aa", "WARNING": "#ffc107", "ERROR": "#ff4b4b", "CRITICAL": "#ff0000"}
            fig = go.Figure()
            fig.add_trace(go.Pie(
                labels=level_counts["level"].tolist(),
                values=level_counts["count"].tolist(),
                hole=0.5,
                marker_colors=[colors.get(l, "#5b616e") for l in level_counts["level"]],
                textinfo="label+percent",
            ))
            fig.update_layout(paper_bgcolor="#0f131b", font_color="#9ba3af", font_size=11,
                              margin=dict(l=10, r=10, t=10, b=10), showlegend=False)
            st.plotly_chart(fig, use_container_width=True, key="log_donut")

    st.divider()
    col_et, col_pe = st.columns(2)
    with col_et:
        error_ts = logexp.get_error_timeseries()
        if not error_ts.empty and error_ts["errors"].sum() > 0:
            fig = px.bar(error_ts, x="hour", y="errors", color_discrete_sequence=["#ff4b4b"],
                         labels={"hour": "Hour", "errors": "Errors"})
            _apply_theme(fig, y_title="Errors", legend=False)
            st.plotly_chart(fig, use_container_width=True, key="log_err_ts")
        else:
            st.info("No errors in period.")
    with col_pe:
        pipeline_errors = logexp.get_pipeline_error_summary()
        if not pipeline_errors.empty:
            st.dataframe(pipeline_errors, use_container_width=True, hide_index=True)
        else:
            st.info("Pipeline error summary belum tersedia.")

    # ── Alert History ─────────────────────────────────────────
    st.divider()
    st.markdown('<div style="color:#f0f2f6;font-weight:600;margin-bottom:0.5rem">Recent Alerts</div>', unsafe_allow_html=True)
    alert_df = logexp.get_alert_history(30)
    if not alert_df.empty:
        st.dataframe(alert_df, use_container_width=True, hide_index=True, height=250)
        st.download_button(
            "Download Alerts CSV", alert_df.to_csv(index=False),
            file_name="alerts.csv", mime="text/csv", key="dl_alerts",
        )
    else:
        st.info("No alerts recorded.")


# ─── Routing ─────────────────────────────────────────────────
pages = {
    "Market Overview": page_overview,
    "Volatility Analytics": page_model,
    "Sentiment Analytics": page_cross_source,
    "Operations": page_operations,
}
pages[page]()

# ─── Auto-refresh ────────────────────────────────────────────
countdown_placeholder = st.empty()
for remaining in range(DASHBOARD_REFRESH_SECONDS, 0, -1):
    countdown_placeholder.markdown(
        f'<div style="text-align:center;color:#5b616e;font-size:0.75rem;margin-top:1rem">'
        f'Next refresh in {remaining}s</div>',
        unsafe_allow_html=True,
    )
    sleep(1)
st.rerun()