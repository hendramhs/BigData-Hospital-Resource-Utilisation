"""
SEGMEN 5 (Data Analyst) - Hasil analitik & interpretasi kesehatan  [M7]
Studi Kasus 2: Hospital Resource Utilisation (data sintetis)

Pertanyaan: bagaimana pola pemanfaatan tempat tidur (BOR) dan lama rawat (LOS) per bangsal
sepanjang 2025, dan apa implikasinya untuk perencanaan kapasitas?

Sumber : data/raw/admissions_full.csv + data/raw/wards.csv  (cleaning minimal dilakukan di sini,
         karena tahap 03_clean_integrate belum ada; ganti READ_FROM_CLEAN bila sudah tersedia)
Keluaran (results/):
  05_quality_impact.json   dampak cleaning (before/after) -> bukti Veracity
  05_bor_monthly.csv       BOR bulanan per bangsal (after cleaning)
  05_bor_ward_summary.csv  ringkasan tahunan per bangsal vs target
  05_los_department.csv    LOS per departemen
  05_summary.json          angka kunci untuk narasi video/PDF
  05_chart_*.png           grafik
"""
import os, time
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from config import *

ensure_dirs()
t0 = time.time()
spark = get_spark("Analytics-Segmen5")

RAINY = [10, 11, 12, 1, 2, 3]  # sama dengan generator
P_START, P_END = "2025-01-01", "2026-01-01"

# ------------------------------------------------------------------ baca data
adm = spark.read.csv(os.path.join(RAW, "admissions_full.csv"), header=True, schema=adm_schema())
wards = spark.read.csv(os.path.join(RAW, "wards.csv"), header=True, schema=wards_schema())
adm = (adm.withColumn("admit_ts", F.to_timestamp("admit_time"))
          .withColumn("disch_ts", F.to_timestamp("discharge_time")))
adm.cache()

# ------------------------------------------------------------------ BOR (Spark SQL)
months_pdf = pd.DataFrame({"month_start": pd.date_range(P_START, periods=12, freq="MS")})
months_pdf["month_end"] = months_pdf.month_start + pd.offsets.MonthBegin(1)
months_pdf["days_in_month"] = (months_pdf.month_end - months_pdf.month_start).dt.days
months_pdf["month"] = months_pdf.month_start.dt.month
months = spark.createDataFrame(months_pdf.astype({"month_start": "datetime64[ns]", "month_end": "datetime64[ns]"}))
months.createOrReplaceTempView("months")
wards.createOrReplaceTempView("wards")


def bor_monthly(df, label):
    """BOR = hari-tempat-tidur terpakai / (total_beds x hari bulan) x 100.
    Hari terpakai = tumpang tindih [admit, discharge] dengan bulan, dalam fraksi hari."""
    df.createOrReplaceTempView(f"adm_{label}")
    q = f"""
    SELECT a.ward_id, m.month, m.days_in_month,
           SUM(GREATEST(0.0, (unix_timestamp(LEAST(a.disch_ts, m.month_end))
                            - unix_timestamp(GREATEST(a.admit_ts, m.month_start))) / 86400.0)) AS bed_days_used,
           COUNT(DISTINCT a.admission_id) AS admissions
    FROM adm_{label} a JOIN months m
      ON a.admit_ts < m.month_end AND a.disch_ts > m.month_start
    GROUP BY a.ward_id, m.month, m.days_in_month
    """
    r = spark.sql(q)
    r.createOrReplaceTempView(f"u_{label}")
    out = spark.sql(f"""
        SELECT u.ward_id, w.ward_name, w.department, u.month, u.admissions,
               ROUND(u.bed_days_used, 1) AS bed_days_used,
               w.total_beds * u.days_in_month AS bed_days_available,
               w.total_beds, w.target_bor_pct,
               ROUND(100.0 * u.bed_days_used / (w.total_beds * u.days_in_month), 1) AS bor_pct
        FROM u_{label} u JOIN wards w ON u.ward_id = w.ward_id
        ORDER BY u.ward_id, u.month""").toPandas()
    for col in ("bed_days_used", "bor_pct", "target_bor_pct", "bed_days_available", "total_beds"):
        out[col] = pd.to_numeric(out[col])
    return out


def ward_year(bor_pdf):
    g = bor_pdf.groupby(["ward_id", "ward_name", "total_beds", "target_bor_pct"], as_index=False).agg(
        bed_days_used=("bed_days_used", "sum"), bed_days_available=("bed_days_available", "sum"))
    g["bor_pct"] = (100 * g.bed_days_used / g.bed_days_available).round(1)
    tot = pd.DataFrame([{"ward_id": "TOTAL", "ward_name": "Seluruh RS", "total_beds": g.total_beds.sum(),
                         "target_bor_pct": None, "bed_days_used": g.bed_days_used.sum(),
                         "bed_days_available": g.bed_days_available.sum()}])
    tot["bor_pct"] = (100 * tot.bed_days_used / tot.bed_days_available).round(1)
    return pd.concat([g, tot], ignore_index=True)


# ------------------------------------------------------------------ BEFORE: data mentah apa adanya
raw_n = adm.count()
raw_valid = adm.filter(F.col("disch_ts").isNotNull())          # tanpa dedup, tanpa koreksi
bor_before = ward_year(bor_monthly(raw_valid, "before"))

# ------------------------------------------------------------------ CLEANING (aturan + catatan keputusan)
canon_dept = [r[0] for r in wards.select("department").collect()]
n_dup = raw_n - adm.dropDuplicates(["admission_id"]).count()
n_null_dis = adm.filter(F.col("disch_ts").isNull()).count()
n_neg = adm.filter(F.col("disch_ts") < F.col("admit_ts")).count()
n_age = adm.filter((F.col("age") < 0) | (F.col("age") > 120)).count()
n_dept_bad = adm.filter(F.col("department").isNull() | (~F.col("department").isin(canon_dept))).count()

c = adm.dropDuplicates(["admission_id"])                       # 1. buang duplikat
c = c.drop("department").join(wards.select("ward_id", "department"), "ward_id", "left")  # 2. dept dari master bangsal
c = c.withColumn("age", F.when((F.col("age") < 0) | (F.col("age") > 120), None).otherwise(F.col("age")))  # 3. umur invalid -> NULL
c = c.withColumn("valid_dis", F.col("disch_ts").isNotNull() & (F.col("disch_ts") >= F.col("admit_ts")))
med = (c.filter("valid_dis")                                   # 4. median LOS (jam) per bangsal dari data valid
         .withColumn("los_h", (F.unix_timestamp("disch_ts") - F.unix_timestamp("admit_ts")) / 3600.0)
         .groupBy("ward_id").agg(F.expr("percentile_approx(los_h, 0.5)").alias("med_los_h")))
c = c.join(med, "ward_id", "left")
c = (c.withColumn("discharge_imputed", ~F.col("valid_dis"))
       .withColumn("disch_ts", F.when(F.col("valid_dis"), F.col("disch_ts"))
                   .otherwise(F.expr("admit_ts + make_interval(0,0,0,0,0,0,med_los_h*3600)")))
       .withColumn("los_days", (F.unix_timestamp("disch_ts") - F.unix_timestamp("admit_ts")) / 86400.0)
       .drop("valid_dis", "med_los_h"))
c.cache()
clean_n = c.count()

bor_m_after = bor_monthly(c, "after")
bor_after = ward_year(bor_m_after)

quality = {
    "rows_before": raw_n, "rows_after": clean_n,
    "issues": {
        "duplikat admission_id": {"before": n_dup, "after": 0, "keputusan": "dropDuplicates(admission_id)"},
        "discharge_time kosong": {"before": n_null_dis, "after": 0,
                                  "keputusan": "diisi admit + median LOS bangsal (flag discharge_imputed)"},
        "discharge < admit": {"before": n_neg, "after": 0,
                              "keputusan": "diperlakukan seperti kosong: diisi median LOS bangsal (flag)"},
        "umur tidak valid (<0 / >120)": {"before": n_age, "after": 0, "keputusan": "diubah NULL (tidak dipakai di BOR/LOS)"},
        "department kosong/format tidak baku": {"before": n_dept_bad, "after": 0,
                                                "keputusan": "department diambil dari wards.csv via ward_id"},
    },
    "discharge_imputed_rows": int(c.filter("discharge_imputed").count()),
    "bor_total_before_pct": float(bor_before.loc[bor_before.ward_id == "TOTAL", "bor_pct"].iloc[0]),
    "bor_total_after_pct": float(bor_after.loc[bor_after.ward_id == "TOTAL", "bor_pct"].iloc[0]),
    "bor_ward_before_vs_after": {
        r.ward_id: {"before": float(bor_before.set_index("ward_id").loc[r.ward_id, "bor_pct"]), "after": float(r.bor_pct)}
        for r in bor_after.itertuples() if r.ward_id != "TOTAL"},
}
save_json("05_quality_impact", quality)

# ------------------------------------------------------------------ LOS per departemen (hanya discharge asli)
c.createOrReplaceTempView("clean_adm")
los = spark.sql("""
    SELECT department, COUNT(*) AS admissions,
           ROUND(AVG(los_days), 2) AS avg_los_days,
           ROUND(percentile_approx(los_days, 0.5), 2) AS median_los_days,
           ROUND(MAX(los_days), 1) AS max_los_days
    FROM clean_adm WHERE NOT discharge_imputed
    GROUP BY department ORDER BY avg_los_days DESC""").toPandas()
los_season = spark.sql(f"""
    SELECT department,
           ROUND(AVG(CASE WHEN month(admit_ts) IN ({','.join(map(str, RAINY))}) THEN los_days END), 2) AS los_rainy,
           ROUND(AVG(CASE WHEN month(admit_ts) NOT IN ({','.join(map(str, RAINY))}) THEN los_days END), 2) AS los_dry
    FROM clean_adm WHERE NOT discharge_imputed GROUP BY department""").toPandas()
los = los.merge(los_season, on="department")
adm_month = spark.sql("SELECT month(admit_ts) AS month, COUNT(*) AS admissions FROM clean_adm GROUP BY 1 ORDER BY 1").toPandas()

# ------------------------------------------------------------------ simpan tabel
bor_m_after.to_csv(os.path.join(RESULTS, "05_bor_monthly.csv"), index=False)
bor_after.to_csv(os.path.join(RESULTS, "05_bor_ward_summary.csv"), index=False)
los.to_csv(os.path.join(RESULTS, "05_los_department.csv"), index=False)

# musiman: rata-rata BOR bulanan (hujan vs kemarau) per bangsal; Jan = bulan awal tanpa pasien bawaan 2024
bm = bor_m_after.copy()
bm["season"] = bm.month.isin(RAINY).map({True: "hujan", False: "kemarau"})
seas = bm[bm.month != 1].groupby(["ward_id", "season"]).bor_pct.mean().unstack().round(1)
adm_rain = adm_month[adm_month.month.isin(RAINY)].admissions.mean()
adm_dry = adm_month[~adm_month.month.isin(RAINY)].admissions.mean()

summary = {
    "bor_year_after": bor_after[["ward_id", "ward_name", "total_beds", "target_bor_pct", "bor_pct"]].to_dict("records"),
    "wards_above_target": [r.ward_id for r in bor_after.itertuples() if r.ward_id != "TOTAL" and r.bor_pct > r.target_bor_pct],
    "bor_seasonal_excl_january": seas.to_dict("index"),
    "avg_monthly_admissions_rainy": round(float(adm_rain)),
    "avg_monthly_admissions_dry": round(float(adm_dry)),
    "peak_month_total_bor": int(bm.groupby("month").bed_days_used.sum().idxmax()),
    "los_by_department": los.to_dict("records"),
    "runtime_seconds": None,
}

# ------------------------------------------------------------------ grafik
plt.rcParams.update({"font.size": 11, "axes.spines.top": False, "axes.spines.right": False})
names = bor_after[bor_after.ward_id != "TOTAL"].set_index("ward_id")

fig, ax = plt.subplots(figsize=(9, 4.8))
x = range(len(names)); w = 0.38
ax.bar([i - w / 2 for i in x], names.bor_pct, w, label="Rasio demand/kapasitas (BOR) aktual", color="#2563eb")
ax.bar([i + w / 2 for i in x], names.target_bor_pct, w, label="Target (wards.csv)", color="#9ca3af")
ax.axhline(100, color="#dc2626", ls="--", lw=1.2); ax.text(len(names) - 0.5, 102, "100% = kapasitas fisik", color="#dc2626", ha="right", fontsize=9)
for i, v in enumerate(names.bor_pct): ax.text(i - w / 2, v + 3, f"{v:.0f}%", ha="center", fontsize=9)
ax.set_xticks(list(x)); ax.set_xticklabels(names.ward_name.str.replace(" Ward", "").str.replace("Intensive Care Unit", "ICU"), rotation=15)
ax.set_ylabel("%"); ax.set_title("BOR tahunan 2025 per bangsal vs target (data sintetis, setelah cleaning)")
ax.legend(frameon=False); fig.tight_layout(); fig.savefig(os.path.join(RESULTS, "05_chart_bor_vs_target.png"), dpi=150); plt.close(fig)

fig, ax = plt.subplots(figsize=(9, 4.8))
for wid, g in bor_m_after.groupby("ward_id"):
    ax.plot(g.month, g.bor_pct, marker="o", ms=3.5, label=g.ward_name.iloc[0].replace(" Ward", "").replace("Intensive Care Unit", "ICU"))
ax.axhline(100, color="#dc2626", ls="--", lw=1)
ax.set_xticks(range(1, 13)); ax.set_xlabel("Bulan 2025"); ax.set_ylabel("BOR (%)")
ax.set_title("BOR bulanan per bangsal 2025 (tanpa pasien bawaan dari 2024)")
ax.legend(frameon=False, ncol=3, fontsize=9); fig.tight_layout(); fig.savefig(os.path.join(RESULTS, "05_chart_bor_monthly.png"), dpi=150); plt.close(fig)

fig, ax = plt.subplots(figsize=(9, 4.4))
l = los.sort_values("avg_los_days")
ax.barh(l.department, l.avg_los_days, color="#0f766e")
for i, v in enumerate(l.avg_los_days): ax.text(v + 0.1, i, f"{v:.1f} hari", va="center", fontsize=9)
ax.set_xlabel("Rata-rata LOS (hari)"); ax.set_title("Lama rawat rata-rata per departemen (discharge asli saja)")
fig.tight_layout(); fig.savefig(os.path.join(RESULTS, "05_chart_los.png"), dpi=150); plt.close(fig)

fig, ax = plt.subplots(figsize=(8, 4.2))
b = pd.DataFrame({"Sebelum cleaning": bor_before.set_index("ward_id").bor_pct,
                  "Sesudah cleaning": bor_after.set_index("ward_id").bor_pct})
b.plot.bar(ax=ax, color=["#9ca3af", "#2563eb"]); ax.set_ylabel("BOR (%)"); ax.set_xlabel("")
ax.set_title("Dampak cleaning terhadap BOR (Veracity)"); plt.xticks(rotation=0, fontsize=8); ax.legend(frameon=False)
fig.tight_layout(); fig.savefig(os.path.join(RESULTS, "05_chart_veracity_impact.png"), dpi=150); plt.close(fig)

summary["runtime_seconds"] = round(time.time() - t0, 1)
save_json("05_summary", summary)

# ------------------------------------------------------------------ cetak ringkas
pd.set_option("display.width", 200)
print("\n=== VERACITY: dampak cleaning ===")
for k, v in quality["issues"].items(): print(f"  {k:40s} sebelum={v['before']:>5}  sesudah={v['after']}")
print(f"  baris: {quality['rows_before']} -> {quality['rows_after']} | discharge diisi: {quality['discharge_imputed_rows']}")
print("\n=== BOR TAHUNAN: sebelum vs sesudah cleaning ===")
print(pd.DataFrame(quality["bor_ward_before_vs_after"]).T.assign(TOTAL=None).dropna(axis=1, how="all"))
print(f"  TOTAL sebelum={quality['bor_total_before_pct']}%  sesudah={quality['bor_total_after_pct']}%")
print("\n=== BOR TAHUNAN vs TARGET (setelah cleaning) ===")
print(bor_after[["ward_id", "total_beds", "target_bor_pct", "bed_days_used", "bed_days_available", "bor_pct"]].to_string(index=False))
print("\n=== BOR BULANAN (pivot) ===")
print(bor_m_after.pivot(index="ward_id", columns="month", values="bor_pct").to_string())
print("\n=== LOS per departemen ===")
print(los.to_string(index=False))
print("\n=== Admisi per bulan ===")
print(adm_month.set_index("month").admissions.to_dict())
print(f"\nSelesai dalam {summary['runtime_seconds']} detik")