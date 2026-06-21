# Rencana ML Training & Progress

> **Target:** Nilai maksimal di rubrik poin 4 (Integrasi ML, 15 poin) + poin 5 (Dashboard, 10 poin).

---

## Arsitektur ML Pipeline

```
                       ┌─────────────────────────┐
                       │   PREFECT (orchestrator) │
                       │                          │
          ┌────────────┤ • sentiment_pipeline     │
          │            │   (30 min)               │
          │            │ • clustering_flow   ← NEW│
          │            │   (weekly / on-demand)   │
          │            │ • training_flow          │
          │            │   (weekly)               │
          │            └────────────┬─────────────┘
          │                         │ train + log
          ▼                         ▼
   ┌──────────┐            ┌─────────────┐
   │ MinIO    │            │   MLflow     │
   │ (data    │            │ ┌──────────┐ │
   │  lake)   │            │ │Tracking  │ │
   └──────────┘            │ │Registry  │ │
                           │ └──────────┘ │
                           │ ┌──────────┐ │
                           │ │XGBoost   │ │
                           │ │KMeans ←NE│W│
                           │ └──────────┘ │
                           └──────┬───────┘
                                  │ load model
                                  ▼
                    ┌─────────────────────────┐
                    │ SPARK STRUCTURED STREAM  │
                    │  (real-time inference)   │
                    │                          │
                    │ 1. Read Kafka (ticker)   │
                    │ 2. 1-min OHLC window     │
                    │ 3. Feature engineering   │
                    │ 4. Assign cluster ← NEW  │
                    │ 5. XGBoost predict       │
                    │ 6. Write → PostgreSQL    │
                    └─────────────┬───────────┘
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │      CONSUMERS           │
                    │ • Streamlit (dashboard)  │
                    │ • Grafana (monitoring)   │
                    │ • Telegram (alerting)    │
                    └─────────────────────────┘
```

---

## Status Saat Ini

### ✅ Sudah Implementasi

| Komponen | File | Status |
|----------|------|--------|
| **XGBoost Training** | `prefect/flows/training_flow.py` | ✅ 386 baris, TimeSeriesSplit 5-fold, MAE/RMSE |
| **MLflow Tracking** | `prefect/flows/training_flow.py` (log_to_mlflow) | ✅ Experiment `btc_volatility_prediction` |
| **Model Registry** | `prefect/flows/training_flow.py` | ✅ Auto-promote ke Production jika MAE < 0.0015 |
| **Real-time Inference** | `processing/stream_processor.py` | ✅ Dalam Spark Streaming, per micro-batch (30 detik) |
| **Feature Engineering** | SQL view `v_ml_features` + Python inline | ✅ 7 fitur: 3 OHLC + 3 sentiment + `minutes_since_sentiment` |
| **Sentiment Pipeline** | `prefect/flows/sentiment_flow.py` | ✅ FinVADER scoring every 30 min |
| **Prefect Orchestration** | `prefect/flows/deploy_all.py` | ✅ 2 deployment: sentiment (30m) + training (weekly) |
| **Dashboard** | `dashboard/app.py` | ✅ 4 halaman: Overview, Model Analytics, Cross-Source, Lineage |
| **Audit Trail** | `security/postgres/init.sql` | ✅ Trigger pada 4 tabel |
| **Data Quality** | Inline di `sentiment_flow.py` | ✅ Null check + range [-1,1] |
| **Alerting** | Grafana + Telegram | ✅ 4 rules: no predictions, high volatility, stale, data gap |

### ❌ Belum / In Progress

| Komponen | File | Status |
|----------|------|--------|
| **K-Means Clustering** | `ml/clustering.py` (belum ada) | 🔴 Fase 2 — belum dibuat |
| **Clustering Flow** | `prefect/flows/clustering_flow.py` (belum ada) | 🔴 Fase 2 — belum dibuat |
| **Enrich Inference** | `processing/stream_processor.py` | 🔴 Fase 3 — tambah cluster label |
| **Dashboard Regime** | `dashboard/app.py` | 🔴 Fase 4 — embed regime gauge |
| **Notebook Eksplorasi** | `notebooks/exploration.ipynb` | 🟡 Fase 1 — selesai dibuat, perlu data >100 sampel |

---

## Fitur ML

### 7 Fitur untuk XGBoost (existing)

```
1. rolling_vol_5m          — Std dev return 5 menit rolling
2. price_range_ratio        — (high - low) / close
3. vol_ratio                — volume / avg_volume_10min
4. compound_score           — FinVADER compound sentiment
5. positive_ratio           — Proporsi tweet positif
6. tweet_count              — Jumlah tweet di window
7. minutes_since_sentiment  — Staleness sentimen
```

### Fitur ke-8: Cluster Label (NEW — Fase 2-3)

```
8. cluster_label            — Market regime cluster (0/1/2)
```

### Target

```
target_vol_5m               — Std dev return 5 menit ke depan (supervised)
```

---

## Rencana Selanjutnya

### Fase 1: Eksplorasi Clustering ✅
- [x] Buat `notebooks/exploration.ipynb`
- [ ] Tunggu data OHLC >100 baris + sentiment multi-window
- [ ] Konfirmasi K optimal (target: K=3)
- [ ] Tentukan label: Stabil / Volatilitas Sedang / Volatilitas Tinggi
- [ ] Cek stabilitas dengan multiple seeds

### Fase 2: Implementasi Clustering Flow
- [ ] Buat `ml/clustering.py`
  - Load `v_ml_features` via SQLAlchemy
  - StandardScaler + KMeans(n_clusters=3)
  - Log ke MLflow: `btc_market_regime_kmeans`
  - Simpan scaler + centroids sebagai artifact
  - Tulis cluster assignment ke tabel baru `btc_market_regime` (opsional)
- [ ] Buat `prefect/flows/clustering_flow.py`
  - Prefect flow `clustering_pipeline`
  - Schedule: weekly (bareng training) atau on-demand
  - Record lineage ke `pipeline_lineage`
- [ ] Daftarkan deployment di `deploy_all.py`

### Fase 3: Enrich Inference
- [ ] Modifikasi `processing/stream_processor.py`
  - Load K-Means model dari MLflow (bareng XGBoost)
  - Di `write_batch()`: assign cluster label ke setiap data point baru
  - Tambahkan `cluster_label` sebagai fitur ke-8 ke XGBoost
  - Simpan `cluster_label` di `volatility_pred`
  - Perbarui `FEATURE_COLS` → 8 fitur
- [ ] Retrain XGBoost dengan 8 fitur
- [ ] Update `v_ml_features` view jika perlu

### Fase 4: Dashboard Enrichment
- [ ] Tambahkan panel di halaman **Pipeline Overview**:
  - Regime gauge (hijau/kuning/merah)
  - Current regime label
- [ ] Tambahkan di halaman **Model Analytics**:
  - Historical regime transitions chart
  - Boxplot prediksi per cluster
  - Tabel statistik per regime

### Fase 5: Demonstrasi & Bukti
- [ ] Trigger training 3x berturut-turut (untuk screenshot)
- [ ] Screenshot MLflow experiments page (3+ runs dengan metrik berbeda)
- [ ] Screenshot Prefect flow runs
- [ ] Screenshot log execution (INFO/WARNING level)
- [ ] Screenshot dashboard dengan regime visible
- [ ] Pastikan semua pipeline bisa dijalankan ulang (reproducibility)

---

## Mapping ke Rubrik Penilaian

| Rubrik | Poin | Evidence |
|--------|------|----------|
| **1. Arsitektur** | 10 | Diagram di atas + arsitektur.md |
| **2. Batch Processing** | 10 | sentiment_pipeline (30 min), clustering_flow, training_flow |
| **3. Stream Processing** | 15 | Spark Structured Streaming real-time inference |
| **4. Integrasi ML** | 15 | XGBoost training + K-Means clustering + inference enrichment |
| **5. Dashboard** | 10 | 4 halaman + regime gauge & insights |
| **6. Monitoring** | 10 | Grafana dashboard + Prefect logs |
| **7. Alerting** | 10 | Grafana 4 rules → Telegram |
| **8. Keamanan** | 10 | .env, RBAC, Nginx HTTPS + Basic Auth |
| **9. Governance** | 5 | Audit trail, data quality, metadata |
| **10. Dokumentasi** | 5 | Laporan.md + README + kode rapi |

---

## Komando Cepat

```bash
# Cek data tersedia untuk clustering
docker exec postgres psql -U btcadmin -d btcdb \
  -c "SELECT COUNT(*) ohlc, (SELECT COUNT(*) FROM sentiment_30m) sentiment FROM btc_ohlc_1m;"

# Trigger training manual (via Prefect)
docker exec prefect-worker prefect deployment run 'model-training-daily/model-training-daily'

# Lihat MLflow experiments
# Buka http://localhost:5001 → experiment: btc_volatility_prediction

# Jalankan notebook eksplorasi
jupyter notebook notebooks/exploration.ipynb
# atau langsung:
python3 -c "
... (lihat script di atas)
"
```
