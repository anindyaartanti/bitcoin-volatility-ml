import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from time import sleep
from db import query_pg, query_trino
from config import DASHBOARD_TITLE, DASHBOARD_REFRESH_SECONDS

st.set_page_config(layout="wide", page_title=DASHBOARD_TITLE)

# ─── Custom CSS ──────────────────────────────────────────────
st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }
    .stAppHeader { display: none; }
    .stApp { background-color: #0e1117; }
    h1, h2, h3 { color: #f0f2f6 !important; font-weight: 600; }
    h1 { font-size: 1.6rem !important; margin-bottom: 0.5rem !important; }
    h2 { font-size: 1.2rem !important; }
    .st-emotion-cache-16idsys p { font-size: 0.85rem; }
    .card {
        background: #1a1d23;
        padding: 1rem 1.2rem;
        border-radius: 8px;
        border: 1px solid #2d3139;
        margin-bottom: 1rem;
    }
    .card .label { font-size: 0.75rem; color: #9ba3af; text-transform: uppercase; letter-spacing: 0.5px; }
    .card .value { font-size: 1.5rem; font-weight: 700; color: #f0f2f6; margin-top: 0.2rem; }
    .card .delta { font-size: 0.85rem; margin-top: 0.15rem; }
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
def _apply_theme(fig, x_title=None, y_title=None, legend=True, height=None, hover="x unified"):
    fig.update_layout(
        paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
        font_color="#9ba3af", font_size=11,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(showgrid=False, title=dict(text=x_title or "", font_size=10)),
        yaxis=dict(gridcolor="#2d3139", title=dict(text=y_title or "", font_size=10)),
        legend=dict(orientation="h", y=1.08, font_size=10) if legend else {},
        height=height, hovermode=hover,
    )


# ─── Sidebar ─────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f'<div class="sidebar-section"><span style="color:#9ba3af;font-size:0.75rem">PIPELINE</span><br><span style="color:#f0f2f6;font-weight:600">{datetime.now():%H:%M:%S} UTC</span></div>', unsafe_allow_html=True)

    if st.button("Refresh", use_container_width=True):
        st.rerun()

    page = st.radio(
        "Menu",
        ["Market Overview", "Volatility Analytics", "Sentiment Analytics", "Pipeline Operations"],
        label_visibility="collapsed",
    )

    st.markdown("""<div style="margin-top:1.5rem;font-size:0.75rem;color:#5b616e;line-height:1.4">
PostgreSQL &mdash; real-time data<br>
Trino &mdash; cross-source analytics
</div>""", unsafe_allow_html=True)

st.title(DASHBOARD_TITLE)


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
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    if not ohlc.empty:
        lp = ohlc["close"].iloc[-1]
        pc = (ohlc["close"].iloc[-1] / ohlc["close"].iloc[0] - 1) * 100
        c1.markdown(
            f'<div class="card-compact"><div class="label">BTC/USDT</div><div class="value">${lp:,.2f}</div>'
            f'<div class="delta" style="color:{"#00d4aa" if pc >= 0 else "#ff4b4b"}">{pc:+.2f}%</div></div>',
            unsafe_allow_html=True,
        )
    else:
        c1.error("No data")
    c2.markdown(
        f'<div class="card-compact"><div class="label">Volatility</div>'
        f'<div class="value">{vol_latest:.4f}</div></div>' if vol_latest is not None
        else '<div class="card-compact"><div class="label">Volatility</div><div class="value">--</div></div>',
        unsafe_allow_html=True,
    )
    c3.markdown(
        f'<div class="card-compact"><div class="label">Volume 24h</div>'
        f'<div class="value">{total_vol:,.0f}</div></div>',
        unsafe_allow_html=True,
    )
    c4.markdown(
        f'<div class="card-compact"><div class="label">Trades 24h</div>'
        f'<div class="value">{total_trades:,}</div></div>',
        unsafe_allow_html=True,
    )
    if sent_val is not None:
        sc = "#00d4aa" if sent_val > 0.05 else "#ffc107" if sent_val > -0.05 else "#ff4b4b"
        c5.markdown(
            f'<div class="card-compact"><div class="label">Sentiment</div>'
            f'<div class="value" style="color:{sc}">{sent_val:+.3f}</div></div>',
            unsafe_allow_html=True,
        )
    else:
        c5.markdown('<div class="card-compact"><div class="label">Sentiment</div><div class="value">--</div></div>', unsafe_allow_html=True)
    c6.markdown(
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
                paper_bgcolor="#0e1117", font_color="#9ba3af", font_size=11,
                margin=dict(l=10, r=10, t=10, b=10),
                showlegend=False,
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
                paper_bgcolor="#0e1117", font_color="#9ba3af", font_size=11,
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

    with tab_b:
        try:
            audit = query_pg(
                "SELECT id, table_name, operation, changed_by, changed_at "
                "FROM audit_log ORDER BY changed_at DESC LIMIT 100"
            )
            if not audit.empty:
                st.dataframe(audit, use_container_width=True, hide_index=True)
                ops = audit["operation"].value_counts()
                fig = go.Figure()
                fig.add_trace(go.Pie(
                    labels=ops.index.tolist(), values=ops.values.tolist(),
                    marker_colors=["#f7931a", "#00d4aa", "#ffc107"],
                    textinfo="label+percent",
                ))
                fig.update_layout(
                    paper_bgcolor="#0e1117", font_color="#9ba3af", font_size=11,
                    margin=dict(l=10, r=10, t=10, b=10),
                    legend=dict(orientation="h", y=-0.15, font_size=10),
                )
                st.plotly_chart(fig, use_container_width=True, key="audit_chart")
            else:
                st.info("No audit data available.")
        except Exception as e:
            st.error(f"Audit: {e}")


# ─── Routing ─────────────────────────────────────────────────
pages = {
    "Market Overview": page_overview,
    "Volatility Analytics": page_model,
    "Sentiment Analytics": page_cross_source,
    "Pipeline Operations": page_lineage,
}
pages[page]()

# ─── Auto-refresh ────────────────────────────────────────────
placeholder = st.empty()
with placeholder:
    sleep(DASHBOARD_REFRESH_SECONDS)
    st.rerun()
