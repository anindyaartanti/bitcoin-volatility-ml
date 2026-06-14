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
</style>
""", unsafe_allow_html=True)

# ─── Sidebar ─────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f'<div class="sidebar-section"><span style="color:#9ba3af;font-size:0.75rem">PIPELINE</span><br><span style="color:#f0f2f6;font-weight:600">{datetime.now():%H:%M:%S} UTC</span></div>', unsafe_allow_html=True)

    if st.button("Refresh", use_container_width=True):
        st.rerun()

    page = st.radio(
        "Menu",
        ["Pipeline Overview", "Model Analytics", "Cross-Source Analytics", "Data Lineage"],
        label_visibility="collapsed",
    )

    st.markdown("""<div style="margin-top:1.5rem;font-size:0.75rem;color:#5b616e;line-height:1.4">
PostgreSQL &mdash; real-time data<br>
Trino &mdash; cross-source analytics
</div>""", unsafe_allow_html=True)

st.title(DASHBOARD_TITLE)


# ═══════════════════════════════════════════════════════════════
# PAGE 1: Pipeline Overview
# ═══════════════════════════════════════════════════════════════
def page_overview():
    try:
        ohlc_24h = query_pg(
            "SELECT window_start, close, volume, volatility "
            "FROM btc_ohlc_1m "
            "WHERE window_start > NOW() - INTERVAL '24 hours' "
            "ORDER BY window_start"
        )
    except Exception:
        ohlc_24h = pd.DataFrame()

    try:
        sent = query_pg(
            "SELECT compound_score FROM sentiment_30m ORDER BY window_start DESC LIMIT 1"
        )
        sent_val = sent["compound_score"].iloc[0] if not sent.empty else None
    except Exception:
        sent_val = None

    try:
        pred = query_pg(
            "SELECT COUNT(*) AS cnt FROM volatility_pred "
            "WHERE window_start > NOW() - INTERVAL '1 hour'"
        )
        pred_cnt = int(pred["cnt"].iloc[0]) if not pred.empty else 0
    except Exception:
        pred_cnt = 0

    # ── Metric cards ───────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)

    if not ohlc_24h.empty:
        last_price = ohlc_24h["close"].iloc[-1]
        price_change = ohlc_24h["close"].iloc[-1] - ohlc_24h["close"].iloc[0]
        pct_change = (price_change / ohlc_24h["close"].iloc[0]) * 100
        vol_latest = ohlc_24h["volatility"].iloc[-1]
        delta_str = f"{pct_change:+.2f}%" if len(ohlc_24h) > 1 else None
        delta_col = "normal"

        c1.markdown(f"""<div class="card">
<div class="label">BTC/USDT</div>
<div class="value">${last_price:,.2f}</div>
<div class="delta" style="color:{"#00d4aa" if pct_change >= 0 else "#ff4b4b"}">{delta_str if delta_str else ""}</div>
</div>""", unsafe_allow_html=True)

        c2.markdown(f"""<div class="card">
<div class="label">Volatility (1m)</div>
<div class="value">{vol_latest:.4f}</div>
</div>""", unsafe_allow_html=True)
    else:
        c1.error("OHLC not available")

    if sent_val is not None:
        sent_color = "#00d4aa" if sent_val > 0.05 else "#ffc107" if sent_val > -0.05 else "#ff4b4b"
        sent_label = "Positif" if sent_val > 0.05 else "Netral" if sent_val > -0.05 else "Negatif"
        c3.markdown(f"""<div class="card">
<div class="label">Sentiment (last)</div>
<div class="value">{sent_val:+.3f}</div>
<div class="delta" style="color:{sent_color}">{sent_label}</div>
</div>""", unsafe_allow_html=True)
    else:
        c3.markdown(f"""<div class="card"><div class="label">Sentiment (last)</div><div class="value" style="color:#5b616e">--</div></div>""", unsafe_allow_html=True)

    c4.markdown(f"""<div class="card">
<div class="label">Predictions (1h)</div>
<div class="value">{pred_cnt}</div>
</div>""", unsafe_allow_html=True)

    # ── Charts ─────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs(["Harga BTC", "Prediksi Volatilitas", "Pipeline Health"])

    with tab1:
        if not ohlc_24h.empty:
            fig = px.line(
                ohlc_24h, x="window_start", y="close",
                title=None,
            )
            fig.update_traces(line_color="#f7931a", line_width=2)
            fig.update_layout(
                margin=dict(l=10, r=10, t=20, b=10),
                paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                font_color="#9ba3af", font_size=11,
                xaxis=dict(showgrid=False, showline=False, title=None),
                yaxis=dict(gridcolor="#2d3139", title=dict(text="USD", font_size=10)),
                hovermode="x unified",
            )
            st.plotly_chart(fig, use_container_width=True, key="price_chart")
        else:
            st.info("No OHLC data available.")

    with tab2:
        try:
            preds = query_pg(
                "SELECT window_start, predicted_vol_5m AS predicted_vol "
                "FROM volatility_pred "
                "WHERE window_start > NOW() - INTERVAL '24 hours' "
                "ORDER BY window_start"
            )
            if not preds.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=preds["window_start"], y=preds["predicted_vol"],
                    mode="lines", name="Predicted", line=dict(color="#f7931a", width=2),
                ))
                fig.add_trace(go.Scatter(
                    x=preds["window_start"], y=preds["predicted_vol"],
                    mode="lines", name="Predicted (copy)", line=dict(color="#00d4aa", width=2, dash="dot"),
                ))
                fig.update_layout(
                    title=None, yaxis_title="Volatility",
                    margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                    font_color="#9ba3af", font_size=11,
                    xaxis=dict(showgrid=False, showline=False),
                    yaxis=dict(gridcolor="#2d3139"),
                    legend=dict(orientation="h", y=1.08, font_size=11),
                )
                st.plotly_chart(fig, use_container_width=True, key="vol_chart")
            else:
                st.info("No predictions yet.")
        except Exception as e:
            st.error(f"Predictions: {e}")

    with tab3:
        hc1, hc2, hc3, hc4 = st.columns(4)
        try:
            r = query_pg(
                "SELECT "
                "(SELECT COUNT(*) FROM btc_ohlc_1m WHERE window_start > NOW() - INTERVAL '24 hours') AS ohlc_24h, "
                "(SELECT COUNT(*) FROM sentiment_30m WHERE window_start > NOW() - INTERVAL '24 hours') AS sent_24h, "
                "(SELECT COUNT(*) FROM volatility_pred WHERE window_start > NOW() - INTERVAL '24 hours') AS pred_24h, "
                "(SELECT MAX(window_start) FROM btc_ohlc_1m) AS last_ohlc"
            )
            if not r.empty:
                last = r["last_ohlc"].iloc[0]
                lag = (datetime.now(last.tzinfo) - last).total_seconds() if last.tzinfo else (datetime.now() - last).total_seconds()

                hc1.markdown(f"""<div class="card"><div class="label">OHLC (24h)</div><div class="value">{int(r['ohlc_24h'].iloc[0])}</div></div>""", unsafe_allow_html=True)
                hc2.markdown(f"""<div class="card"><div class="label">Sentiment (24h)</div><div class="value">{int(r['sent_24h'].iloc[0])}</div></div>""", unsafe_allow_html=True)
                hc3.markdown(f"""<div class="card"><div class="label">Predictions (24h)</div><div class="value">{int(r['pred_24h'].iloc[0])}</div></div>""", unsafe_allow_html=True)
                hc4.markdown(f"""<div class="card"><div class="label">Last OHLC</div><div class="value">{int(lag)}s</div></div>""", unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Health: {e}")


# ═══════════════════════════════════════════════════════════════
# PAGE 2: Model Analytics
# ═══════════════════════════════════════════════════════════════
def page_model():
    st.markdown('<h2>Model Analytics</h2>', unsafe_allow_html=True)

    col1, col2 = st.columns([2, 1])
    with col1:
        try:
            runs = query_pg(
                "SELECT model_version, "
                "MIN(window_start) AS first_run, MAX(window_start) AS last_run, COUNT(*) AS n_preds "
                "FROM volatility_pred WHERE model_version IS NOT NULL "
                "GROUP BY model_version ORDER BY first_run"
            )
            if not runs.empty:
                st.dataframe(runs, use_container_width=True, hide_index=True)
                fig = px.bar(
                    runs, x="model_version", y="n_preds",
                    title=None,
                    labels={"model_version": "Model", "n_preds": "Predictions"},
                    color="n_preds", color_continuous_scale="Blues",
                )
                fig.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                    font_color="#9ba3af", font_size=11,
                    xaxis=dict(showgrid=False), yaxis=dict(gridcolor="#2d3139"),
                    coloraxis_showscale=False,
                )
                st.plotly_chart(fig, use_container_width=True, key="mae_chart")
            else:
                st.info("No training data available.")
        except Exception as e:
            st.error(f"Training data: {e}")

    with col2:
        try:
            preds = query_pg(
                "SELECT predicted_vol_5m AS predicted_vol "
                "FROM volatility_pred "
                "WHERE window_start > NOW() - INTERVAL '7 days'"
            )
            if not preds.empty:
                preds["error"] = 0  # placeholder tanpa actual_vol
                fig = px.histogram(preds, x="predicted_vol", nbins=40, title=None,
                                   labels={"predicted_vol": "Predicted Volatility"})
                fig.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                    font_color="#9ba3af", font_size=11,
                    xaxis=dict(showgrid=False, title=dict(text="Error", font_size=10)),
                    yaxis=dict(gridcolor="#2d3139", title=dict(text="Count", font_size=10)),
                    bargap=0.05,
                )
                fig.update_traces(marker_color="#f7931a", marker_line_color="#f7931a")
                st.plotly_chart(fig, use_container_width=True, key="residual_chart")

                mean_pred = preds["predicted_vol"].mean()
                st.markdown(f"""<div class="card"><div class="label">Mean Pred (7d)</div><div class="value">{mean_pred:.6f}</div></div>""", unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Residuals: {e}")


# ═══════════════════════════════════════════════════════════════
# PAGE 3: Cross-Source Analytics (Trino)
# ═══════════════════════════════════════════════════════════════
def page_cross_source():
    st.markdown('<h2>Cross-Source Analytics</h2>', unsafe_allow_html=True)
    st.markdown(
        '<div style="color:#9ba3af;font-size:0.85rem;margin-bottom:1rem">'
        "Combines PostgreSQL (OHLC) with MinIO Parquet (raw tweets) via Trino.</div>",
        unsafe_allow_html=True,
    )

    if st.button("Jalankan Query Federasi", use_container_width=True):
        with st.spinner("Querying Trino..."):
            try:
                sql = """
                SELECT
                    o.window_start,
                    o.close,
                    o.volatility,
                    t.compound         AS avg_tweet_sentiment,
                    t.tweet_count
                FROM postgresql.btcdb.btc_ohlc_1m o
                LEFT JOIN (
                    SELECT
                        date_trunc('minute', created_at) AS tweet_minute,
                        AVG(compound) AS compound,
                        COUNT(*)      AS tweet_count
                    FROM hive.twitter_raw.tweets
                    WHERE created_at > NOW() - INTERVAL '1' HOUR
                    GROUP BY 1
                ) t ON o.window_start = t.tweet_minute
                WHERE o.window_start > NOW() - INTERVAL '1' HOUR
                ORDER BY o.window_start
                """
                df = query_trino(sql)

                if df.empty:
                    st.info(
                        "Query succeeded but no data returned. "
                        "The Parquet folder may be empty — run the sentiment pipeline first."
                    )
                else:
                    st.markdown(
                        f'<div style="color:#00d4aa;font-size:0.9rem">{len(df)} rows returned.</div>',
                        unsafe_allow_html=True,
                    )
                    st.dataframe(df, use_container_width=True, hide_index=True)

                    if "close" in df.columns and "avg_tweet_sentiment" in df.columns:
                        valid = df.dropna(subset=["avg_tweet_sentiment"])
                        if len(valid) > 2:
                            fig = px.scatter(
                                valid, x="avg_tweet_sentiment", y="close",
                                title=None,
                                labels={"avg_tweet_sentiment": "Avg Tweet Sentiment", "close": "BTC Close (USD)"},
                                trendline="ols",
                            )
                            fig.update_traces(marker_color="#f7931a", marker_size=6)
                            fig.update_layout(
                                margin=dict(l=10, r=10, t=10, b=10),
                                paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                                font_color="#9ba3af", font_size=11,
                                xaxis=dict(showgrid=False), yaxis=dict(gridcolor="#2d3139"),
                            )
                            st.plotly_chart(fig, use_container_width=True, key="cross_scatter")

                            corr = valid["avg_tweet_sentiment"].corr(valid["close"])
                            st.markdown(
                                f'<div class="card"><div class="label">Sentiment vs Price Correlation</div>'
                                f'<div class="value">{corr:.4f}</div></div>',
                                unsafe_allow_html=True,
                            )
            except Exception as e:
                st.error(f"Trino query failed: {e}")
                st.markdown(
                    '<div style="color:#5b616e;font-size:0.8rem">'
                    "Check: (1) Trino container is running, "
                    "(2) Hive schema registered, "
                    "(3) Parquet files exist in MinIO bucket.</div>",
                    unsafe_allow_html=True,
                )

    st.divider()
    st.markdown('<h3>SQL Explorer</h3>', unsafe_allow_html=True)
    sql_input = st.text_area(
        "Write any Trino SQL query",
        height=100,
        value="SELECT * FROM hive.twitter_raw.tweets LIMIT 10",
    )
    if st.button("Jalankan", key="run_sql"):
        with st.spinner("Running..."):
            try:
                df = query_trino(sql_input)
                st.dataframe(df, use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(f"Query error: {e}")


# ═══════════════════════════════════════════════════════════════
# PAGE 4: Data Lineage
# ═══════════════════════════════════════════════════════════════
def page_lineage():
    st.markdown('<h2>Data Lineage</h2>', unsafe_allow_html=True)

    tab_a, tab_b = st.tabs(["Pipeline Lineage", "Audit Log"])

    with tab_a:
        try:
            lineage = query_pg(
                "SELECT run_id, pipeline_name, source, target_table, "
                "rows_processed, rows_rejected, quality_status, "
                "started_at, finished_at "
                "FROM pipeline_lineage "
                "ORDER BY finished_at DESC NULLS LAST LIMIT 100"
            )
            if not lineage.empty:
                st.dataframe(lineage, use_container_width=True, hide_index=True)
                fig = px.timeline(
                    lineage.dropna(subset=["started_at", "finished_at"]),
                    x_start="started_at", x_end="finished_at",
                    y="pipeline_name", color="quality_status",
                    title=None,
                    labels={"pipeline_name": "Pipeline", "quality_status": "Status"},
                )
                fig.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                    font_color="#9ba3af", font_size=11,
                    xaxis=dict(showgrid=False), yaxis=dict(showgrid=False),
                    legend=dict(orientation="h", y=1.08, font_size=10),
                )
                st.plotly_chart(fig, use_container_width=True, key="lineage_chart")
            else:
                st.info("No lineage data available.")
        except Exception as e:
            st.error(f"Lineage: {e}")

    with tab_b:
        try:
            audit = query_pg(
                "SELECT id, table_name, operation, changed_by, changed_at "
                "FROM audit_log ORDER BY changed_at DESC LIMIT 100"
            )
            if not audit.empty:
                st.dataframe(audit, use_container_width=True, hide_index=True)
                ops = audit["operation"].value_counts()
                fig = px.pie(
                    values=ops.values, names=ops.index,
                    title=None,
                )
                fig.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                    font_color="#9ba3af", font_size=11,
                    legend=dict(orientation="h", y=-0.15, font_size=10),
                )
                fig.update_traces(marker=dict(colors=["#f7931a", "#00d4aa", "#ffc107"]))
                st.plotly_chart(fig, use_container_width=True, key="audit_chart")
            else:
                st.info("No audit data available.")
        except Exception as e:
            st.error(f"Audit: {e}")


# ─── Routing ─────────────────────────────────────────────────
pages = {
    "Pipeline Overview": page_overview,
    "Model Analytics": page_model,
    "Cross-Source Analytics": page_cross_source,
    "Data Lineage": page_lineage,
}
pages[page]()

# ─── Auto-refresh ────────────────────────────────────────────
placeholder = st.empty()
with placeholder:
    sleep(DASHBOARD_REFRESH_SECONDS)
    st.rerun()
