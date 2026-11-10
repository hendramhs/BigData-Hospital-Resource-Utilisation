"""
05_analytics.py - SEGMEN 5 (Data Analyst) | M7: Spark SQL & DataFrames | Fokus: Veracity & Value
Studi Kasus 2: Hospital Resource Utilisation (BOR & LOS)

Jalankan dari root repo:   python 05_analytics.py

ATURAN
  * Skrip ini TIDAK membuat / membersihkan data. Hanya membaca keluaran tahap sebelumnya:
      data/clean/admissions, data/clean/wards, data/clean/integrated_resource_records   (02_storage_integration_quality.py)
      results/02_quality_report.json                                                    (02_storage_integration_quality.py)
    Jika ada yang hilang, skrip berhenti dengan pesan error.
  * Metode hunian (BOR) MENGIKUTI 04_processing.py (Operation B):
      - discharge kosong  -> diimputasi: admit + median LOS (hari) dari admisi yang discharge-nya valid
      - 1 baris per hari rawat (explode(sequence(start_day, end_day)), hari masuk & hari pulang dihitung)
      - occupied_beds = jumlah admisi yang hadir pada hari itu per bangsal; BOR = occupied / total_beds
    Bedanya: dihitung di sini dari data/clean (bukan dari data/lake/bor_daily), dibatasi tahun 2025, dan hari tanpa
    pasien tetap dihitung (kalender lengkap). bor_daily dipakai hanya untuk pengecekan silang bila ada.

PERTANYAAN
  Q1  OPSI A - BOR TERLAYANI (dibatasi kapasitas) vs target + permintaan tak terlayani   [Value]
  Q2  Pola musim hujan vs kemarau: admisi, ALOS, permintaan tak terlayani, surge         [Value]
  Q4  Hari kritis: % hari melebihi kapasitas / target                                    [Value]
  (BOR berbasis permintaan, mis. 297%, hanya kolom pendukung; tidak dipakai sebagai angka utama)
  V   Veracity: before (02_quality_report.json) vs after (diukur ulang di data clean) + sensitivitas imputasi
Keluaran: results/05_*.csv, results/05_summary.json, results/05_dashboard.png (+ panel terpisah)
"""
import os, time, warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from config import *

ensure_dirs()
RAINY = [1, 2, 3, 10, 11, 12]
Y0, Y1 = "2025-01-01", "2025-12-31"
WARMUP_END = "2025-01-15"     # data mulai 1 Jan 2025 tanpa pasien dari 2024 -> hunian awal Januari terlalu rendah
timings = {}


def require(path, label):
    if not os.path.exists(path):
        parent = os.path.dirname(path)
        raise SystemExit(f"[ERROR] {label} tidak ditemukan: {path}\n"
                         f"        Isi folder induk: {os.listdir(parent) if os.path.isdir(parent) else 'folder tidak ada'}\n"
                         f"        Jalankan tahap sebelumnya (02_storage_integration_quality.py) lebih dulu.")


def timed(name, df):
    """toPandas() + catat waktu eksekusi; Decimal (hasil ROUND Spark SQL) -> float."""
    t = time.time(); pdf = df.toPandas(); timings[name] = round(time.time() - t, 2)
    for c in pdf.columns:
        if pdf[c].dtype == object and len(pdf) and pdf[c].map(lambda x: type(x).__name__ == "Decimal").any():
            pdf[c] = pdf[c].astype(float)
    print(f"[{timings[name]:>5}s] {name}")
    return pdf


# ------------------------------------------------------------------ LOAD (hanya data clean hasil 02)
paths = {"admissions": f"{CLEAN}/admissions", "wards": f"{CLEAN}/wards",
         "integrated": f"{CLEAN}/integrated_resource_records"}
for k, p in paths.items():
    require(p, f"Data CLEAN '{k}'")
require(os.path.join(RESULTS, "02_quality_report.json"), "Laporan kualitas 02")

spark = get_spark("M7-DataAnalyst")
adm = spark.read.parquet(paths["admissions"])
wards = spark.read.parquet(paths["wards"])
integ = spark.read.parquet(paths["integrated"])
print("Skema admissions (CLEAN):"); adm.printSchema()

if "admit_ts" not in adm.columns and {"admit_time", "discharge_time"} <= set(adm.columns):
    adm = (adm.withColumn("admit_ts", F.to_timestamp("admit_time"))
              .withColumn("discharge_ts", F.to_timestamp("discharge_time")))
missing = {"admission_id", "ward_id", "admit_ts", "discharge_ts"} - set(adm.columns)
if missing:
    raise SystemExit(f"[ERROR] Kolom wajib tidak ada di admissions CLEAN: {sorted(missing)}\n        Tersedia: {adm.columns}")
wards = wards.select("ward_id", "ward_name", "department", "total_beds", "target_bor_pct")
wards.createOrReplaceTempView("wards")
n_adm = adm.count()

# ------------------------------------------------------------------ HUNIAN HARIAN (metode 04, Operation B)
# median LOS (hari) dari admisi yang discharge-nya valid, persis seperti 04_processing.py
median_los = (adm.filter(F.col("discharge_ts").isNotNull())
                 .agg(F.percentile_approx(F.datediff(F.to_date("discharge_ts"), F.to_date("admit_ts")), 0.5))
                 .first()[0]) or 1
n_imputed = adm.filter(F.col("discharge_ts").isNull()).count()
print(f"Admisi CLEAN: {n_adm:,} | discharge kosong: {n_imputed:,} -> diimputasi admit + {median_los} hari (median LOS)")

stays = (adm.withColumn("start_day", F.to_date("admit_ts"))
            .withColumn("imputed", F.col("discharge_ts").isNull())
            .withColumn("end_day", F.coalesce(F.to_date("discharge_ts"), F.date_add(F.to_date("admit_ts"), int(median_los))))
            .withColumn("end_day", F.greatest("end_day", "start_day"))
            .withColumn("day", F.explode(F.sequence("start_day", "end_day"))))
stays_day = stays.filter(F.col("day").between(Y0, Y1))

calendar = spark.sql(f"SELECT explode(sequence(DATE'{Y0}', DATE'{Y1}')) AS day")
occ_raw = stays_day.groupBy("ward_id", "day").agg(
    F.count("*").alias("occupied"),
    F.sum(F.when(~F.col("imputed"), 1).otherwise(0)).alias("occupied_observed"))
occ = (calendar.crossJoin(wards).join(occ_raw, ["ward_id", "day"], "left")
       .na.fill({"occupied": 0, "occupied_observed": 0})
       .withColumn("month", F.month("day"))
       .withColumn("season", F.when(F.col("month").isin(RAINY), "Rainy").otherwise("Dry"))
       .cache())
occ.createOrReplaceTempView("occ_daily")

# pengecekan silang dengan keluaran 04 (jika ada)
xcheck = None
bd = f"{LAKE}/bor_daily"
if os.path.isdir(bd):
    try:
        b04 = (spark.read.parquet(bd).filter(F.col("day").between(Y0, Y1))
                    .groupBy("ward_id").agg(F.avg("occupied_beds").alias("avg_occ_04")))
        mine = occ.groupBy("ward_id").agg(F.avg("occupied").alias("avg_occ_05"))
        xcheck = b04.join(mine, "ward_id").withColumn("selisih", F.round(F.col("avg_occ_05") - F.col("avg_occ_04"), 2)).toPandas()
        print("\n[CEK SILANG vs data/lake/bor_daily (04)]"); print(xcheck.to_string(index=False))
        if xcheck.selisih.abs().max() > 0.2:   # metode identik + data sama -> selisih seharusnya ~0
            print("[PERINGATAN] Hunian 05 dan bor_daily (04) berbeda. Kemungkinan 04 masih membaca data raw / kolom string discharge_time. "
                  "Minta Big Data Developer memakai data/clean + discharge_ts lalu jalankan ulang 04.")
    except Exception as e:
        print(f"[INFO] bor_daily tidak dapat dibaca untuk cek silang: {e}")
else:
    print("[INFO] data/lake/bor_daily belum ada -> cek silang dengan 04 dilewati")

# ------------------------------------------------------------------ Q1 + Q4: BOR, terlayani, tak terlayani, hari kritis
q1 = f"""
SELECT ward_name, total_beds, target_bor_pct,
       ROUND(AVG(occupied), 1)                                                            AS avg_occupied,
       MAX(occupied)                                                                      AS peak_occupied,
       ROUND(100 * SUM(occupied) / (total_beds * COUNT(*)), 1)                            AS demand_bor_pct,
       ROUND(100 * SUM(CASE WHEN day >= DATE'{WARMUP_END}' THEN occupied END)
             / (total_beds * SUM(CASE WHEN day >= DATE'{WARMUP_END}' THEN 1 END)), 1)    AS demand_bor_excl_warmup_pct,
       ROUND(100 * SUM(occupied_observed) / (total_beds * COUNT(*)), 1)                   AS bor_excl_imputed_pct,
       ROUND(100 * SUM(LEAST(occupied, total_beds)) / (total_beds * COUNT(*)), 1)         AS served_bor_pct,
       ROUND(100 * SUM(GREATEST(occupied - total_beds, 0)) / SUM(occupied), 1)            AS unmet_demand_pct,
       ROUND(100 * AVG(CASE WHEN occupied > total_beds THEN 1 ELSE 0 END), 1)              AS pct_days_over_capacity,
       ROUND(100 * AVG(CASE WHEN occupied > total_beds * target_bor_pct / 100 THEN 1 ELSE 0 END), 1) AS pct_days_over_target
FROM occ_daily GROUP BY ward_name, total_beds, target_bor_pct
ORDER BY demand_bor_pct DESC"""
bor = timed("Q1_Q4_BOR_ward", spark.sql(q1))
# pembanding: BOR standar = patient-days / (tempat tidur x 365); patient-days kontinu (jam), diimputasi sama seperti 04
pdays = (adm.withColumn("end_ts", F.coalesce("discharge_ts", F.col("admit_ts") + F.expr(f"INTERVAL {int(median_los)} DAYS")))
            .withColumn("pd", F.greatest(F.lit(0.0), (F.unix_timestamp(F.least("end_ts", F.lit("2026-01-01 00:00:00").cast("timestamp")))
                                                      - F.unix_timestamp(F.greatest("admit_ts", F.lit("2025-01-01 00:00:00").cast("timestamp")))) / 86400.0))
            .groupBy("ward_id").agg(F.sum("pd").alias("patient_days"))
            .join(wards, "ward_id").select("ward_name", "patient_days", "total_beds"))
pdays = timed("Q1_patient_days_BOR", pdays)
pdays["bor_patient_days_pct"] = (100 * pdays.patient_days / (pdays.total_beds * 365)).round(1)
bor = bor.merge(pdays[["ward_name", "bor_patient_days_pct"]], on="ward_name")
bor["beds_needed_at_target"] = (bor.avg_occupied * 100 / bor.target_bor_pct).apply(lambda x: int(-(-x // 1)))
bor["bed_gap"] = bor.beds_needed_at_target - bor.total_beds
bor["capacity_multiple_needed"] = (bor.beds_needed_at_target / bor.total_beds).round(1)

# ------------------------------------------------------------------ Q2: musiman
monthly = timed("Q2_monthly_BOR", spark.sql("""
SELECT ward_name, month,
       ROUND(100 * SUM(occupied) / (total_beds * COUNT(*)), 1)                         AS demand_bor_pct,
       ROUND(100 * SUM(LEAST(occupied, total_beds)) / (total_beds * COUNT(*)), 1)      AS served_bor_pct,
       ROUND(100 * SUM(GREATEST(occupied - total_beds, 0)) / SUM(occupied), 1)         AS unmet_demand_pct
FROM occ_daily GROUP BY ward_name, total_beds, month ORDER BY ward_name, month"""))

occ_season = timed("Q2_season_occupancy", spark.sql("""
SELECT ward_name, target_bor_pct, season, ROUND(AVG(occupied), 1) AS avg_occupied
FROM occ_daily GROUP BY ward_name, target_bor_pct, season"""))
sea = occ_season.pivot(index=["ward_name", "target_bor_pct"], columns="season", values="avg_occupied").reset_index()
sea.columns.name = None
sea["beds_needed_rainy"] = (sea.Rainy * 100 / sea.target_bor_pct).apply(lambda x: int(-(-x // 1)))
sea["beds_needed_dry"] = (sea.Dry * 100 / sea.target_bor_pct).apply(lambda x: int(-(-x // 1)))
sea["surge_beds_rainy"] = sea.beds_needed_rainy - sea.beds_needed_dry

adm_v = (adm.filter(F.col("discharge_ts").isNotNull())
            .withColumn("los_days", (F.unix_timestamp("discharge_ts") - F.unix_timestamp("admit_ts")) / 86400.0))
adm_v.createOrReplaceTempView("adm_valid")
adm.withColumn("month", F.month("admit_ts")).createOrReplaceTempView("adm_all")
adm_cnt = timed("Q2_admissions_season", spark.sql(f"""
SELECT w.ward_name, CASE WHEN month IN ({','.join(map(str, RAINY))}) THEN 'Rainy' ELSE 'Dry' END AS season,
       COUNT(*) AS admissions
FROM adm_all a JOIN wards w USING (ward_id) GROUP BY 1, 2"""))
alos = timed("Q2_alos_season", spark.sql(f"""
SELECT w.ward_name, CASE WHEN month(admit_ts) IN ({','.join(map(str, RAINY))}) THEN 'Rainy' ELSE 'Dry' END AS season,
       ROUND(AVG(los_days), 2) AS alos_days
FROM adm_valid a JOIN wards w USING (ward_id) GROUP BY 1, 2"""))
a1 = adm_cnt.pivot(index="ward_name", columns="season", values="admissions")
a1 = (a1.assign(adm_per_month_rainy=(a1.Rainy / 6).round(0), adm_per_month_dry=(a1.Dry / 6).round(0))
        .assign(admission_uplift_pct=lambda d: (100 * (d.adm_per_month_rainy / d.adm_per_month_dry - 1)).round(1))
        [["adm_per_month_rainy", "adm_per_month_dry", "admission_uplift_pct"]].reset_index())
a2 = alos.pivot(index="ward_name", columns="season", values="alos_days")
a2 = (a2.assign(alos_uplift_pct=lambda d: (100 * (d.Rainy / d.Dry - 1)).round(1))
        .rename(columns={"Rainy": "alos_rainy", "Dry": "alos_dry"}).reset_index())
unmet_s = timed("Q2_season_unmet", spark.sql("""
SELECT ward_name, season, ROUND(100 * SUM(GREATEST(occupied - total_beds, 0)) / SUM(occupied), 1) AS unmet_demand_pct
FROM occ_daily GROUP BY ward_name, season"""))
u = unmet_s.pivot(index="ward_name", columns="season", values="unmet_demand_pct")
u = u.rename(columns={"Rainy": "unmet_pct_rainy", "Dry": "unmet_pct_dry"}).reset_index(); u.columns.name = None
season = sea.merge(a1, on="ward_name").merge(a2, on="ward_name").merge(u, on="ward_name")
alos_ward = timed("Q2_alos_ward", spark.sql("""
SELECT w.ward_name, ROUND(AVG(los_days), 2) AS alos_days, COUNT(*) AS admissions_with_valid_los
FROM adm_valid a JOIN wards w USING (ward_id) GROUP BY 1"""))

# ------------------------------------------------------------------ V: Veracity (before = laporan 02; after = diukur ulang)
q02 = load_json("02_quality_report"); qb, qa = q02["before"], q02["after"]
age_after = adm.filter((F.col("age") < 0) | (F.col("age") > 120)).count()
temporal_after = adm.filter(F.col("discharge_ts") < F.col("admit_ts")).count()
ver = pd.DataFrame([
    ["Baris admisi duplikat",            qb["duplicate_admission_rows"], n_adm - adm.select("admission_id").distinct().count(), "dropDuplicates(admission_id)"],
    ["Discharge kosong",                 qb["missing_discharge_rows"],   0,                f"{n_imputed:,} admisi ber-discharge NULL di clean (kosong + invalid) diimputasi admit + {median_los} hari saat analisis (metode 04)"],
    ["Discharge < admit",                qb["invalid_temporal_rows"],    temporal_after,   "di-NULL-kan (lalu diimputasi)"],
    ["Umur tidak valid (<0 / >120)",     qb["invalid_age_rows"],         age_after,        "di-NULL-kan"],
    ["Tarif harian negatif",             qb["negative_daily_rate_rows"], qa["negative_daily_rate_values_remaining"], "di-NULL-kan"],
    ["Log bed yatim (orphan)",           qa["orphan_bed_rows_removed"],  0,                "left_semi join dengan admisi valid (dibuang)"],
], columns=["masalah", "sebelum", "sesudah", "penanganan"])
print("\n[VERACITY] before (02_quality_report.json) vs after (diukur ulang dari data clean)"); print(ver.to_string(index=False))

sens = bor[["ward_name", "demand_bor_pct", "bor_excl_imputed_pct"]].copy()
sens["selisih_pts"] = (sens.demand_bor_pct - sens.bor_excl_imputed_pct).round(1)
sens["selisih_pct"] = (100 * sens.selisih_pts / sens.demand_bor_pct).round(1)
print("\n[SENSITIVITAS IMPUTASI] BOR dengan vs tanpa admisi ber-discharge imputasi"); print(sens.to_string(index=False))

# ------------------------------------------------------------------ Q5: dataset terintegrasi (bed log + admisi + wards)
integ.createOrReplaceTempView("integrated")
beds_q = timed("Q5_integrated_bed_status", spark.sql("""
SELECT ward_name, COUNT(*) AS bed_log_rows, COUNT(DISTINCT bed_id) AS distinct_beds_logged, MAX(total_beds) AS total_beds,
       ROUND(100 * AVG(CASE WHEN occupancy_status = 'Under_Review' THEN 1 ELSE 0 END), 1) AS pct_under_review,
       ROUND(100 * AVG(CASE WHEN occupancy_status = 'Reserved' THEN 1 ELSE 0 END), 1)     AS pct_reserved,
       ROUND(AVG(daily_rate), 0)                                                          AS avg_daily_rate_idr,
       SUM(CASE WHEN daily_rate IS NULL THEN 1 ELSE 0 END)                                AS null_rate_rows
FROM integrated GROUP BY ward_name ORDER BY ward_name"""))

# ------------------------------------------------------------------ SIMPAN HASIL
tables = {"05_bor_ward": bor, "05_season": season, "05_monthly_bor": monthly, "05_veracity_before_after": ver, "05_imputation_sensitivity": sens, "05_alos_ward": alos_ward,
          "05_bed_log_integrated": beds_q}
if xcheck is not None: tables["05_crosscheck_vs_04"] = xcheck
for name, df in tables.items(): df.to_csv(f"{RESULTS}/{name}.csv", index=False)
save_json("05_summary", {"method": "ikut 04_processing.py (imputasi median LOS, sensus harian inklusif)",
                         "median_los_days": int(median_los), "admissions_clean": n_adm, "imputed_discharge": n_imputed,
                         "timings_sec": timings, "bor": bor.to_dict("records"), "season": season.to_dict("records"),
                         "veracity": ver.to_dict("records"), "imputation_sensitivity": sens.to_dict("records")})

# ------------------------------------------------------------------ VISUALISASI
plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False})
SHORT = {"Intensive Care Unit": "ICU", "Internal Medicine Ward": "Penyakit Dalam", "Cardiology Ward": "Kardiologi",
         "Surgery Ward": "Bedah", "Pediatrics Ward": "Anak", "Emergency Ward": "IGD"}


def p_bor(ax):
    b = bor.sort_values("served_bor_pct")
    y = list(range(len(b))); lab = [SHORT.get(w, w) for w in b.ward_name]
    ax.barh(y, b.served_bor_pct, color="#2E86C1", label="BOR terlayani (dibatasi kapasitas)")
    ax.scatter(b.target_bor_pct, y, marker="|", color="black", s=350, zorder=3, label="Target BOR")
    ax.axvline(100, ls="--", color="grey", lw=1)
    for i, (v, t) in enumerate(zip(b.served_bor_pct, b.target_bor_pct)):
        ax.text(v + 1, i, f"{v:.1f}% (target {t:.0f}%)", va="center", fontsize=8)
    ax.set_yticks(y); ax.set_yticklabels(lab); ax.set_xlim(0, 135); ax.set_xlabel("%")
    ax.set_title("BOR terlayani 2025 vs target (opsi A)"); ax.legend(fontsize=7, loc="upper center", bbox_to_anchor=(0.5, -0.14), ncol=2, frameon=False)


def p_month(ax):
    for w, g in monthly.groupby("ward_name"): ax.plot(g.month, g.unmet_demand_pct, marker="o", ms=3, label=SHORT.get(w, w))
    for lo, hi in [(0.5, 3.5), (9.5, 12.5)]: ax.axvspan(lo, hi, color="#AED6F1", alpha=.35)
    ax.set_xticks(range(1, 13)); ax.set_xlabel("Bulan 2025 (biru = musim hujan)"); ax.set_ylabel("Permintaan tak terlayani (%)")
    ax.set_title("Pola musiman: permintaan tak terlayani"); ax.legend(fontsize=7, ncol=2)


def p_unmet(ax):
    b = bor.sort_values("unmet_demand_pct")
    y = list(range(len(b))); lab = [SHORT.get(w, w) for w in b.ward_name]
    ax.barh(y, b.unmet_demand_pct, color="#C0392B")
    for i, (v, d) in enumerate(zip(b.unmet_demand_pct, b.pct_days_over_capacity)):
        ax.text(v + 1, i, f"{v:.0f}%  |  {d:.0f}% hari > kapasitas", va="center", fontsize=8)
    ax.set_yticks(y); ax.set_yticklabels(lab); ax.set_xlim(0, 110); ax.set_xlabel("% dari total permintaan hari-tempat-tidur")
    ax.set_title("Permintaan tak terlayani per bangsal")


def p_ver(ax):
    v = ver.iloc[::-1]; y = range(len(v))
    ax.barh([i + .2 for i in y], v.sebelum, height=.38, color="#E67E22", label="Sebelum")
    ax.barh([i - .2 for i in y], v.sesudah, height=.38, color="#27AE60", label="Sesudah")
    for i, (s, t) in enumerate(zip(v.sebelum, v.sesudah)):
        ax.text(s + 10, i + .2, f"{s:,}", va="center", fontsize=7); ax.text(t + 10, i - .2, f"{t:,}", va="center", fontsize=7)
    ax.set_yticks(list(y)); ax.set_yticklabels(v.masalah, fontsize=7); ax.set_xlabel("Jumlah baris")
    ax.set_title("Veracity: sebelum vs sesudah cleaning"); ax.legend(fontsize=7, loc="lower right")


panels = {"bor": p_bor, "monthly": p_month, "unmet": p_unmet, "veracity": p_ver}
fig, axs = plt.subplots(2, 2, figsize=(15, 9))
for ax, fn in zip(axs.flat, panels.values()): fn(ax)
fig.suptitle("Hospital Resource Utilisation 2025 - Ringkasan Analitik (data sintetis)", fontsize=13, fontweight="bold")
plt.tight_layout(rect=[0, 0, 1, .96]); plt.savefig(f"{RESULTS}/05_dashboard.png", dpi=160); plt.close()
for name, fn in panels.items():
    f, a = plt.subplots(figsize=(8, 4.6)); fn(a); plt.tight_layout(); plt.savefig(f"{RESULTS}/05_chart_{name}.png", dpi=160); plt.close()

print("\n=== SELESAI. Hasil di folder results/ (05_*.csv, 05_summary.json, 05_dashboard.png) ===")
print("\n[Q1+Q4] BOR per bangsal"); print(bor.to_string(index=False))
print("\n[Q2] Musiman"); print(season.to_string(index=False))
spark.stop()