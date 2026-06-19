#!/usr/bin/env python3
"""Generate exploration.ipynb for market regime clustering."""

import nbformat as nbf
from pathlib import Path
import os

os.chdir(Path(__file__).parent.parent)

nb = nbf.v4.new_notebook()
nb.metadata = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python", "version": "3.11.0"},
}

cells = []


def md(src):
    cells.append(nbf.v4.new_markdown_cell(src))


def code(src):
    cells.append(nbf.v4.new_code_cell(src))


# ════════════════════════════════════════════════════════
# CELLS
# ════════════════════════════════════════════════════════

md("""# Eksplorasi Market Regime Clustering

**Tujuan:** Menentukan jumlah cluster optimal (K) untuk market regime clustering pada data volatilitas Bitcoin + sentimen Twitter/X.

**Pendekatan:**
1. Ambil fitur dari view `v_ml_features` (7 fitur: 3 OHLC + 3 sentiment + 1 timestamp feature)
2. Hapus fitur dengan variance = 0, standardisasi sisanya
3. Uji K-Means K=2,3,4 — evaluasi dengan **Elbow Method** + **Silhouette Score**
4. Analisis centroid → label deskriptif (Stabil / Sedang / Tinggi)
5. Tentukan K optimal

**Hasil eksplorasi ini menentukan:**
- Jumlah cluster K yang akan digunakan di `ml/clustering.py`
- Label tiap cluster untuk dashboard
- Apakah fitur sentimen perlu treatment khusus sebelum clustering
""")

# ── 1. SETUP ──
md("## 1. Setup & Load Data")
code("""import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from sqlalchemy import create_engine

# Style
plt.rcParams.update({
    "figure.figsize": (12, 5),
    "figure.dpi": 120,
    "axes.titlesize": 13,
    "axes.labelsize": 11,
})

# ─── DB Connection ───
engine = create_engine(
    "postgresql+psycopg2://btcadmin:gantiPasswordAman123@localhost:5434/btcdb"
)

# Load view
df = pd.read_sql("SELECT * FROM v_ml_features ORDER BY window_start", engine)
df["window_start"] = pd.to_datetime(df["window_start"])
df.set_index("window_start", inplace=True)

print(f"Dataset shape:  {df.shape}")
print(f"Time range:     {df.index.min()} → {df.index.max()}")
print(f"Duration:       {df.index.max() - df.index.min()}")
print(f"Target mean:    {df['target_vol_5m'].mean():.8f}")
print(f"Target std:     {df['target_vol_5m'].std():.8f}")
df.head(10)
""")

# ── 2. EDA ──
md("## 2. Exploratory Data Analysis")

code("""# 7 fitur clustering + target
all_feat = [
    "rolling_vol_5m", "price_range_ratio", "vol_ratio",
    "compound_score", "positive_ratio", "tweet_count",
    "minutes_since_sentiment",
]
target = "target_vol_5m"

# Variance per fitur — deteksi fitur useless (variance ≈ 0)
print("═══ Feature Variance Check ═══")
for col in all_feat:
    var = df[col].var()
    flag = " ⚠ ZERO VARIANCE" if var < 1e-15 else ""
    print(f"  {col:30s}  var={var:.10f}  mean={df[col].mean():.6f}{flag}")

print()
print(f"═══ Target Stats ═══")
print(f"  {target:30s}  mean={df[target].mean():.8f}  std={df[target].std():.8f}")
""")

code("""# Distribusi fitur (hanya yang punya variance)
feat_cols = ["rolling_vol_5m", "price_range_ratio", "vol_ratio",
             "compound_score", "positive_ratio", "tweet_count"]

# Filter ke fitur yang var > 0
active_features = [c for c in feat_cols if df[c].var() > 1e-15]
print(f"Active features (var > 0): {active_features}")
print(f"Dropped (zero var):        {[c for c in feat_cols if c not in active_features]}")

n = len(active_features) + 1  # +1 for target
rows = (n + 2) // 3
fig, axes = plt.subplots(rows, 3, figsize=(16, 4 * rows))
axes = axes.flatten()

for i, col in enumerate(active_features):
    ax = axes[i]
    vals = df[col].dropna()
    ax.hist(vals, bins=min(30, len(vals)), edgecolor="white", color="steelblue")
    ax.set_title(col, fontsize=11)
    ax.axvline(vals.median(), color="red", linestyle="--", label=f"median={vals.median():.6f}")
    ax.legend(fontsize=7)

# Target
axes[len(active_features)].hist(df[target].dropna(), bins=min(30, len(df)),
                                 edgecolor="white", color="darkorange")
axes[len(active_features)].set_title(f"{target} (target)", fontsize=11)
axes[len(active_features)].axvline(df[target].median(), color="red", linestyle="--",
                                   label=f"median={df[target].median():.6f}")
axes[len(active_features)].legend(fontsize=7)

# Hide unused
for j in range(len(active_features) + 1, len(axes)):
    axes[j].axis("off")

plt.suptitle("Distribusi Fitur + Target", fontsize=14, y=1.02)
plt.tight_layout()
plt.show()
""")

code("""# Matriks korelasi (termasuk target)
plot_cols = active_features + [target]
corr = df[plot_cols].corr()

fig, ax = plt.subplots(figsize=(10, 8))
mask = np.triu(np.ones_like(corr, dtype=bool), k=1)
sns.heatmap(corr, mask=mask, annot=True, fmt=".3f", cmap="RdBu_r",
            center=0, square=True, linewidths=0.5, vmin=-1, vmax=1,
            ax=ax, cbar_kws={"shrink": 0.8})
ax.set_title("Matriks Korelasi Fitur + Target", fontsize=14)
plt.tight_layout()
plt.show()
""")

code("""# Pair plot untuk fitur utama yang aktif
if len(active_features) >= 2:
    plot_cols = active_features[:5] + [target]
    sns.pairplot(df[plot_cols].dropna(), diag_kind="kde",
                 plot_kws={"alpha": 0.6, "s": 30})
    plt.suptitle(f"Pair Plot: {len(active_features)} Fitur Aktif + Target", y=1.02, fontsize=14)
    plt.show()
else:
    print("⚠️  Kurang dari 2 fitur aktif — skip pair plot.")
""")

# ── 3. K-MEANS ──
md("## 3. K-Means Clustering Evaluation")

md("""### Fitur untuk Clustering

Digunakan **6 fitur** (OHLC + sentiment, tanpa `minutes_since_sentiment`):
- `rolling_vol_5m` — volatilitas historis 5 menit
- `price_range_ratio` — rentang harga relatif vs close
- `vol_ratio` — volume relatif vs rata-rata 10 menit
- `compound_score` — skor sentimen FinVADER
- `positive_ratio` — proporsi tweet positif
- `tweet_count` — jumlah tweet

**Fitur dengan variance = 0 akan di-drop terlebih dahulu** (biasanya terjadi saat semua data point berasal dari 1 window sentimen yang sama).
""")

code("""# ─── Pilih fitur + drop zero-variance ───
cluster_features_all = [
    "rolling_vol_5m", "price_range_ratio", "vol_ratio",
    "compound_score", "positive_ratio", "tweet_count",
]

# Filter fitur dengan varian > 0
cluster_features = [c for c in cluster_features_all if df[c].var() > 1e-15]
dropped = [c for c in cluster_features_all if c not in cluster_features]

print(f"Fitur digunakan:  {cluster_features}")
print(f"Fitur di-drop:    {dropped if dropped else '(none)'}")

X = df[cluster_features].dropna().values
print(f"\\nSamples (after dropna): {X.shape[0]}")
print(f"Features used:           {X.shape[1]}")

# Standardisasi
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

print("\\nScaled stats (should be mean≈0, std≈1):")
for i, col in enumerate(cluster_features):
    print(f"  {col:25s}  mean={X_scaled[:, i].mean():+.4f}  std={X_scaled[:, i].std():.4f}")
""")

code("""# ─── Elbow + Silhouette ───
n_samples = X_scaled.shape[0]
max_k = min(n_samples, 10)  # jangan lebih dari n_samples

print(f"Testing K=2..{max_k-1} (max allowed = n_samples-1 = {n_samples-1})\\n")
print(f"{'K':<6} {'Inertia':>12} {'Silhouette':>14} {'Cluster Sizes'}")
print("-" * 60)

K_range = list(range(2, max_k))
inertias = []
silhouettes = []
cluster_sizes_list = []

for k in K_range:
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = km.fit_predict(X_scaled)
    inert = km.inertia_
    sil = silhouette_score(X_scaled, labels)
    sizes = [int(s) for s in np.bincount(labels)]

    inertias.append(inert)
    silhouettes.append(sil)
    cluster_sizes_list.append(sizes)

    marker = ""
    if sil == max(silhouettes):
        marker = " ← best silhouette"
    elif k <= 4:
        marker = " ← candidate"
    print(f"K={k:<4} {inert:>12.3f} {sil:>14.4f}   {sizes}{marker}")

# Plot
if len(K_range) > 1:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    ax1.plot(K_range, inertias, "bo-", markersize=8)
    ax1.set_xlabel("K (Number of Clusters)")
    ax1.set_ylabel("Inertia (WCSS)")
    ax1.set_title("Elbow Method")
    ax1.grid(True, alpha=0.3)
    # Anotasi K=2,3,4
    for kh in [2, 3, 4]:
        if kh in K_range:
            idx = K_range.index(kh)
            ax1.annotate(f"K={kh}", (kh, inertias[idx]),
                         textcoords="offset points", xytext=(0, 15),
                         fontsize=11, ha="center", color="red", fontweight="bold")

    ax2.plot(K_range, silhouettes, "go-", markersize=8)
    ax2.set_xlabel("K (Number of Clusters)")
    ax2.set_ylabel("Silhouette Score")
    ax2.set_title("Silhouette Analysis")
    ax2.grid(True, alpha=0.3)
    for kh in [2, 3, 4]:
        if kh in K_range:
            idx = K_range.index(kh)
            ax2.annotate(f"K={kh}", (kh, silhouettes[idx]),
                         textcoords="offset points", xytext=(0, 15),
                         fontsize=11, ha="center", color="red", fontweight="bold")

    plt.tight_layout()
    plt.show()
else:
    print("\\n⚠️  Not enough K values to plot (need >1).")
""")

code("""# ─── Sample size warning ───
if n_samples < 30:
    print("═" * 55)
    print("⚠️  DATA AWAL — hanya", n_samples, "sample.")
    print("   Biarkan pipeline stream + batch jalan 30-60 menit")
    print("   agar data OHLC dan sentimen terakumulasi.")
    print("   Setelah itu: restart kernel & run ulang semua sel.")
    print("═" * 55)
    print()
    print("   Cara cek data sudah cukup:")
    print("     docker exec postgres psql -U btcadmin -d btcdb \\\\")
    print("       -c \"SELECT COUNT(*) FROM btc_ohlc_1m;\"")
    print("     → Target: > 100 baris untuk clustering yang meaningful")
""")

# ── 4. PCA ──
md("## 4. Visualisasi PCA 2D")

code("""if n_samples >= 6:
    pca = PCA(n_components=2, random_state=42)
    X_pca = pca.fit_transform(X_scaled)

    K_CANDIDATES = [k for k in [2, 3, 4] if k < n_samples]
    palette = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]

    fig, axes = plt.subplots(1, len(K_CANDIDATES), figsize=(6 * len(K_CANDIDATES), 5))
    if len(K_CANDIDATES) == 1:
        axes = [axes]

    for idx, k in enumerate(K_CANDIDATES):
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X_scaled)
        sil = silhouette_score(X_scaled, labels)

        ax = axes[idx]
        for c in range(k):
            mask = labels == c
            ax.scatter(X_pca[mask, 0], X_pca[mask, 1],
                       c=palette[c % len(palette)], label=f"Cluster {c}",
                       s=80, alpha=0.85, edgecolors="white", linewidth=0.5)
        ax.set_title(f"K={k}  (sil={sil:.4f})", fontsize=13)
        ax.set_xlabel(f"PC1 ({pca.explained_variance_ratio_[0]:.1%})")
        ax.set_ylabel(f"PC2 ({pca.explained_variance_ratio_[1]:.1%})")
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.2)

    fig.suptitle("K-Means — PCA 2D Projection", fontsize=14, y=1.02)
    plt.tight_layout()
    plt.show()

    print(f"Total explained variance (2 PCs): {pca.explained_variance_ratio_.sum():.2%}")

    # PC loadings
    loadings = pd.DataFrame(
        pca.components_.T,
        index=cluster_features,
        columns=[f"PC{i+1}" for i in range(2)],
    )
    print("\\n═══ PC Loadings ═══")
    print(loadings.round(4))
else:
    print(f"⚠️  Butuh ≥6 sample untuk PCA, hanya ada {n_samples}.")
""")

# ── 5. CENTROID ──
md("## 5. Analisis Centroid & Interpretasi Cluster")

code("""# ─── Pilih K terbaik ───
# Gunakan silhouette score tertinggi, dengan preferensi K=3 untuk interpretability
sil_arr = np.array(silhouettes)
best_k_by_sil = K_range[int(np.argmax(sil_arr))]
BEST_K = 3 if 3 < n_samples else best_k_by_sil

print(f"K terbaik by silhouette: {best_k_by_sil}")
print(f"K digunakan:             {BEST_K} (prefer K=3 untuk interpretability)\\n")

km_final = KMeans(n_clusters=BEST_K, random_state=42, n_init=10)
labels_final = km_final.fit_predict(X_scaled)
centroids_original = scaler.inverse_transform(km_final.cluster_centers_)

# Centroid DataFrame
centroid_df = pd.DataFrame(
    centroids_original,
    columns=cluster_features,
    index=[f"Cluster {i}" for i in range(BEST_K)],
).T

print("═══ Centroid per Cluster (skala asli) ═══")
print(centroid_df.round(8))
print()
sizes = np.bincount(labels_final).tolist()
print(f"Cluster sizes: {dict(enumerate(sizes))}")
""")

code("""# ─── Bar chart centroid ───
n_feat = len(cluster_features)
rows = (n_feat + 2) // 3
fig, axes = plt.subplots(rows, 3, figsize=(16, 4.5 * rows))
axes = axes.flatten()

for i, col in enumerate(cluster_features):
    ax = axes[i]
    values = centroid_df.loc[col].values
    x_labels = centroid_df.columns
    bar_colors = [palette[j % len(palette)] for j in range(BEST_K)]

    bars = ax.bar(x_labels, values, color=bar_colors, edgecolor="white", linewidth=0.8)
    ax.set_title(col, fontsize=12)
    ax.axhline(y=df[col].mean(), color="gray", linestyle="--",
               linewidth=1.5, label=f"Global mean={df[col].mean():.4f}")
    ax.legend(fontsize=8)
    ax.grid(True, axis="y", alpha=0.3)

    for bar, v in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                f"{v:.4f}", ha="center", va="bottom", fontsize=7, fontweight="bold")

# Hide unused subplots
for j in range(n_feat, len(axes)):
    axes[j].axis("off")

fig.suptitle(f"Centroid Analysis — K={BEST_K} Clusters", fontsize=14, y=1.01)
plt.tight_layout()
plt.show()
""")

code("""# ─── Label Cluster berdasarkan Volatilitas ───
vol_col = "rolling_vol_5m"
vol_means = centroid_df.loc[vol_col].sort_values()
cluster_order = vol_means.index.tolist()  # sorted low → high volatility

# Naming scheme
if BEST_K == 2:
    label_names = {0: "Volatilitas Rendah", 1: "Volatilitas Tinggi"}
elif BEST_K == 3:
    label_names = {0: "Stabil", 1: "Volatilitas Sedang", 2: "Volatilitas Tinggi"}
elif BEST_K == 4:
    label_names = {0: "Stabil", 1: "Volatilitas Rendah",
                   2: "Volatilitas Sedang", 3: "Volatilitas Tinggi"}
else:
    label_names = {i: f"Regime {i+1}" for i in range(BEST_K)}

# Map cluster_id → label
label_map = {}
for rank, cluster_id in enumerate(cluster_order):
    label_map[cluster_id] = label_names[rank]

# ─── Print Interpretation ───
global_mean = df[cluster_features].mean()
global_std = df[cluster_features].std().replace(0, 1)

print("═══ Cluster Label Mapping ═══")
for cluster_id, label in label_map.items():
    vol = centroids_original[cluster_id][0]
    z_scores = (centroids_original[cluster_id] - global_mean.values) / global_std.values
    desc_parts = []
    for col, zval in zip(cluster_features, z_scores):
        if abs(zval) > 0.2:
            arrow = "↑" if zval > 0 else "↓"
            desc_parts.append(f"{col} {arrow} ({zval:+.2f}σ)")
    desc = " | ".join(desc_parts) if desc_parts else "near average on all features"

    print(f"  Cluster {cluster_id} → \\"{label}\\" (n={sizes[cluster_id]})")
    print(f"    rolling_vol_5m = {vol:.8f}")
    print(f"    Characteristics: {desc}")

print()
print("═══ Ringkasan untuk Dashboard ═══")
color_map = {
    "Stabil": "🟢 Hijau",
    "Volatilitas Rendah": "🟢 Hijau",
    "Volatilitas Sedang": "🟡 Kuning",
    "Volatilitas Tinggi": "🔴 Merah",
}
for cluster_id, label in label_map.items():
    color = color_map.get(label, "⚪ Abu-abu")
    print(f"  Regime {cluster_id}: {color} — {label}")
""")

# ── 6. TIMELINE ──
md("## 6. Timeline Cluster Assignment")
code("""# Plot cluster assignment over time + BTC close price
if n_samples > 3:
    df_labeled = df[cluster_features].dropna().copy()
    df_labeled["cluster_id"] = labels_final
    df_labeled["cluster_label"] = df_labeled["cluster_id"].map(label_map)

    # Fetch close price from original table
    close_df = pd.read_sql(
        "SELECT window_start, close FROM btc_ohlc_1m ORDER BY window_start",
        engine,
    )
    close_df["window_start"] = pd.to_datetime(close_df["window_start"])
    close_df.set_index("window_start", inplace=True)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True,
                                     gridspec_kw={"height_ratios": [1, 3]})

    # BTC Close price
    ax1.plot(close_df.index, close_df["close"], color="black", linewidth=1.5)
    ax1.set_ylabel("BTC Close (USDT)", fontsize=11)
    ax1.grid(True, alpha=0.3)
    ax1.legend(["BTC/USDT"], loc="upper left")

    # Cluster timeline
    label_to_color = {
        "Stabil": "#4C72B0",
        "Volatilitas Rendah": "#55A868",
        "Volatilitas Sedang": "#DD8452",
        "Volatilitas Tinggi": "#C44E52",
    }
    for label_name in label_map.values():
        mask = df_labeled["cluster_label"] == label_name
        y_pos = [list(label_map.values()).index(label_name)] * mask.sum()
        ax2.scatter(df_labeled.index[mask], y_pos,
                    c=label_to_color.get(label_name, "gray"),
                    s=120, alpha=0.85, edgecolors="white", linewidth=0.5,
                    label=label_name, zorder=5)

    ax2.set_yticks(range(len(label_map)))
    ax2.set_yticklabels(list(label_map.values()))
    ax2.set_ylabel("Market Regime", fontsize=11)
    ax2.set_xlabel("Time (UTC)", fontsize=11)
    ax2.grid(True, axis="x", alpha=0.3)
    ax2.legend(fontsize=9, loc="upper left")

    fig.suptitle("BTC Price & Market Regime Over Time", fontsize=14)
    plt.tight_layout()
    plt.show()
else:
    print("⚠️  Butuh >3 sampel untuk timeline, hanya ada", n_samples)
""")

# ── 7. STABILITY ──
md("## 7. Stabilitas Cluster — Multiple Runs")
code("""# Cek konsistensi K-Means dengan beberapa seed berbeda
if n_samples >= 6:
    seeds = [0, 42, 123, 999]
    print(f"K={BEST_K}, {len(seeds)} seeds: checking stability...\\n")

    best_inertia = float("inf")
    best_labels = None
    best_seed = None
    for seed in seeds:
        km = KMeans(n_clusters=BEST_K, random_state=seed, n_init=10)
        labels = km.fit_predict(X_scaled)
        sil = silhouette_score(X_scaled, labels)
        sizes = [int(s) for s in np.bincount(labels)]
        marker = " ← best" if km.inertia_ < best_inertia else ""
        if km.inertia_ < best_inertia:
            best_inertia = km.inertia_
            best_labels = labels
            best_seed = seed
        print(f"  seed={seed:<5} inertia={km.inertia_:.3f}  sil={sil:.4f}  sizes={sizes}{marker}")

    print(f"\\n→ Best seed = {best_seed} (lowest inertia, akan dipakai di produksi)")
    # Simpan best seed
    BEST_SEED = best_seed
else:
    print("⚠️  Butuh ≥6 sample untuk stability check.")
    BEST_SEED = 42
""")

# ── 8. KESIMPULAN ──
md("""## 8. Kesimpulan & Rekomendasi Final

### Hasil Eksplorasi

| Parameter | Nilai |
|-----------|-------|
| K optimal | **3** (Stabil, Volatilitas Sedang, Volatilitas Tinggi) |
| Alasan | 3 cluster memberikan interpretability terbaik untuk dashboard |
| Fitur dominan | `vol_ratio`, `rolling_vol_5m`, `price_range_ratio` |
| Fitur sentimen | Berguna saat data mencakup >2 window sentimen (variance > 0) |
| Random seed | `BEST_SEED` (dari stability check) |

### Caveat
- Dengan data < 100 sampel: hasil bisa tidak stabil, validasi ulang setelah pipeline berjalan lebih lama
- Fitur sentimen (`compound_score`, `positive_ratio`, `tweet_count`) punya variance = 0 saat data hanya mencakup 1 window 30 menit
- Rekomendasi: jalankan notebook ini ulang setiap kali mau training model untuk validasi K

### Next Steps → Fase 2-4
1. **`ml/clustering.py`** — Train K-Means dengan K=3, persist ke MLflow Registry sebagai `btc_market_regime_kmeans`
2. **`prefect/flows/clustering_flow.py`** — Prefect flow untuk training + labeling otomatis
3. **`processing/stream_processor.py`** — Load K-Means model saat inference, assign `cluster_label` → enrich fitur XGBoost
4. **`dashboard/app.py`** — Embed regime gauge + timeline ke Pipeline Overview
""")

nb.cells = cells
nbf.write(nb, "notebooks/exploration.ipynb")
print(f"✅ notebooks/exploration.ipynb written — {len(cells)} cells")
