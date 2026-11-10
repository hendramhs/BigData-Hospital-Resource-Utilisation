"""
04_processing.py  -  Segmen 4: Data Processing & Computational Requirements
Big Data Developer (M5-M6) | Fokus: Volume & Velocity
Studi kasus: Hospital Resource Utilisation (BOR & LOS)

Jalankan dari root repo:   python 04_processing.py

Output:
  results/volume_profile.csv      -> jumlah record tiap tabel
  results/benchmark_A_vs_B.csv    -> waktu Operation A vs B + jumlah shuffle
  results/velocity_log.csv        -> arrival, latency ingest/proses, throughput
  data/lake/bor_daily/            -> BOR harian per bangsal (untuk Data Analyst)
"""
import os
import sys
import time
import shutil
import datetime
import pandas as pd

# Windows: pastikan worker Spark memakai Python yang sama (hilangkan warning python3)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
# Windows: jika winutils sudah dipasang di C:\hadoop\bin, pakai otomatis
if os.name == "nt" and not os.environ.get("HADOOP_HOME") and os.path.exists(r"C:\hadoop\bin\winutils.exe"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, ArrayType

# =====================================================================
# 1. KONFIGURASI
# =====================================================================
RAW_DIR = "data/raw"
CLEAN_DIR = "data/lake/clean"          # dipakai otomatis jika ada (dari Data Engineer)
LAKE_DIR = "data/lake"
RESULTS_DIR = "results"
RUNS = 3                               # jumlah run benchmark (di luar warm-up)
RUN_MAPREDUCE_DEMO = False             # contoh RDD map/reduceByKey (M5); lambat di Windows
SHOW_EXPLAIN = False                   # True = cetak explain() penuh (panjang)

os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(LAKE_DIR, exist_ok=True)

spark = (SparkSession.builder
         .appName("Hospital-Processing-Segment4")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "8")    # data kecil -> partisi kecil
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("FATAL")   # sembunyikan stacktrace berulang; error Python tetap muncul


# =====================================================================
# 2. HELPER
# =====================================================================
def flatten(df):
    """Ratakan struct bersarang (JSON); array of struct di-explode."""
    changed = True
    while changed:
        changed = False
        for f in df.schema.fields:
            if isinstance(f.dataType, StructType):
                inner = [F.col(f"{f.name}.{s.name}").alias(f"{f.name}_{s.name}")
                         for s in f.dataType.fields]
                df = df.select([c for c in df.columns if c != f.name] + inner)
                changed = True
                break
            if isinstance(f.dataType, ArrayType) and isinstance(f.dataType.elementType, StructType):
                df = df.withColumn(f.name, F.explode_outer(F.col(f.name)))
                changed = True
                break
    return df


def count_shuffles(df):
    """Hitung operator Exchange (shuffle) pada physical plan."""
    try:
        return df._jdf.queryExecution().executedPlan().toString().count("Exchange")
    except Exception:
        return -1


def run_action(df):
    """Eksekusi penuh (noop sink) agar semua join/agregasi benar-benar berjalan."""
    df.write.format("noop").mode("overwrite").save()


WRITER = "spark"   # diputuskan sekali oleh detect_writer()


def detect_writer():
    """Uji tulis Parquet via Spark sekali. Gagal (mis. Windows tanpa winutils) -> pakai pandas."""
    global WRITER
    if os.environ.get("FORCE_PANDAS_WRITE") == "1":
        WRITER = "pandas"
    else:
        test_dir = f"{LAKE_DIR}/_write_test"
        shutil.rmtree(test_dir, ignore_errors=True)
        try:
            spark.range(5).write.mode("overwrite").parquet(test_dir)
            WRITER = "spark"
        except Exception:
            WRITER = "pandas"
        shutil.rmtree(test_dir, ignore_errors=True)
    if WRITER == "pandas":
        print("[INFO] Penulisan Parquet via Spark tidak tersedia (Windows tanpa winutils/HADOOP_HOME).\n"
              "       Ingestion memakai pandas; latency TIDAK sebanding dengan Spark native "
              "(sebutkan ini di video, atau pasang winutils / jalankan di Colab).")
    else:
        print("[INFO] Penulisan Parquet via Spark: OK")


def write_parquet(df, path, mode="overwrite"):
    if WRITER == "spark":
        df.write.mode(mode).parquet(path)
        return
    os.makedirs(path, exist_ok=True)
    pdf = df.toPandas()
    fname = f"part-{int(time.time() * 1000)}"
    try:
        pdf.to_parquet(f"{path}/{fname}.parquet", index=False)
    except Exception:
        pdf.to_csv(f"{path}/{fname}.csv", index=False)


def benchmark(label, df, runs=RUNS):
    run_action(df)                                   # warm-up (tidak dihitung)
    times = []
    for _ in range(runs):
        t = time.perf_counter()
        run_action(df)
        times.append(time.perf_counter() - t)
    avg = sum(times) / len(times)
    sh = count_shuffles(df)
    print(f"  {label:<26} avg={avg:.3f}s  runs={[round(x, 3) for x in times]}  shuffle(Exchange)={sh}")
    return {"operation": label, "avg_sec": round(avg, 4),
            "min_sec": round(min(times), 4), "max_sec": round(max(times), 4),
            "shuffle_exchanges": sh}


# =====================================================================
# 3. LOAD DATA & PROFIL VOLUME
# =====================================================================
detect_writer()
print("\n=== [1] LOAD DATA & PROFIL VOLUME ===")
use_clean = os.path.isdir(os.path.join(CLEAN_DIR, "admissions"))

if use_clean:
    print(f"Memakai data bersih dari {CLEAN_DIR}")
    adm = spark.read.parquet(f"{CLEAN_DIR}/admissions")
    wards = spark.read.parquet(f"{CLEAN_DIR}/wards")
    bed = flatten(spark.read.parquet(f"{CLEAN_DIR}/bed_capacity"))
else:
    print(f"Memakai data raw dari {RAW_DIR} (+ cleaning ringan)")
    adm = spark.read.csv(f"{RAW_DIR}/admissions_full.csv", header=True, inferSchema=True)
    wards = spark.read.csv(f"{RAW_DIR}/wards.csv", header=True, inferSchema=True)
    bed_raw = spark.read.json(f"{RAW_DIR}/bed_capacity.json")
    if "_corrupt_record" in bed_raw.columns and len(bed_raw.columns) == 1:   # JSON multi-line
        bed_raw = spark.read.option("multiLine", True).json(f"{RAW_DIR}/bed_capacity.json")
    bed = flatten(bed_raw)

profile = []
for name, df in [("admissions", adm), ("wards", wards), ("bed_capacity", bed)]:
    n = df.count()
    profile.append({"table": name, "rows": n, "columns": len(df.columns)})
    print(f"[{name}] rows={n:,}  cols={len(df.columns)}")
pd.DataFrame(profile).to_csv(f"{RESULTS_DIR}/volume_profile.csv", index=False)
print("Relasi: admissions.admission_id = bed_capacity.admission_id ; "
      "admissions.ward_id = wards.ward_id = bed_capacity.ward_id")

# --- Cleaning ringan (dilewati jika sudah ada data bersih dari Data Engineer) ---
if not use_clean:
    adm = adm.dropDuplicates(["admission_id"]).filter(F.col("admit_time").isNotNull())
    # waktu keluar < waktu masuk dianggap tidak valid -> dibuang
    adm = adm.filter(F.col("discharge_time").isNull() |
                     (F.col("discharge_time") >= F.col("admit_time")))
    bed = bed.dropDuplicates(["record_id"])

adm = adm.cache()
n_adm = adm.count()
print(f"Admisi siap diproses: {n_adm:,}")

# Discharge kosong = pasien belum pulang / data hilang. Jika diperpanjang sampai akhir tahun,
# BOR akan membengkak. Keputusan: imputasi dengan median LOS data valid.
median_los = (adm.filter(F.col("discharge_time").isNotNull())
                 .agg(F.percentile_approx(F.datediff("discharge_time", "admit_time"), 0.5))
                 .first()[0]) or 1
n_missing_dis = adm.filter(F.col("discharge_time").isNull()).count()
print(f"Discharge kosong: {n_missing_dis:,} -> diimputasi admit + median LOS ({median_los} hari)")

# =====================================================================
# 4. OPERATION A  &  OPERATION B
# =====================================================================
print("\n=== [2] OPERATION A vs B (Volume / Computational Complexity) ===")

# --- Operation A: agregasi sederhana, 1 tabel --------------------------------
op_a = adm.groupBy("ward_id").count().withColumnRenamed("count", "admissions")

# --- Operation B: relasional + temporal --------------------------------------
stays = (adm
         .withColumn("start_day", F.to_date("admit_time"))
         .withColumn("end_day",
                     F.coalesce(F.to_date("discharge_time"),
                                F.date_add(F.to_date("admit_time"), int(median_los))))
         .withColumn("end_day", F.greatest("end_day", "start_day"))
         .withColumn("los_days", F.greatest(F.datediff("end_day", "start_day"), F.lit(1)))
         .withColumn("day", F.explode(F.sequence("start_day", "end_day"))))   # 1 baris / hari rawat

# join 1: master bangsal (kapasitas) - kunci ward_id
ward_ref = wards.select("ward_id", "ward_name", "total_beds").dropDuplicates(["ward_id"])
joined = stays.join(ward_ref, on="ward_id", how="left")

# join 2: log bed (diagregasi per admisi agar tidak fan-out; orphan otomatis diabaikan)
bed_agg = (bed.groupBy("admission_id").agg(F.count("*").alias("bed_events"))
              .withColumnRenamed("admission_id", "bed_adm_id"))
joined = (joined.join(bed_agg, joined["admission_id"] == bed_agg["bed_adm_id"], "left")
                .drop("bed_adm_id"))

# agregasi temporal: BOR harian per bangsal
op_b = (joined.groupBy("ward_id", "ward_name", "day")
        .agg(F.countDistinct("admission_id").alias("occupied_beds"),
             F.first("total_beds").alias("total_beds"),
             F.avg("los_days").alias("avg_los_days"),
             F.sum(F.coalesce("bed_events", F.lit(0))).alias("bed_log_events"))
        .withColumn("bor_pct", F.round(F.col("occupied_beds") / F.col("total_beds") * 100, 2)))

print("\nHasil Operation A (admisi per bangsal):")
op_a.join(ward_ref.select("ward_id", "ward_name"), "ward_id", "left") \
    .orderBy(F.desc("admissions")).show(truncate=False)
print("Hasil Operation B (BOR harian, 10 tertinggi):")
op_b.orderBy(F.desc("bor_pct")).show(10, truncate=False)

print(f"Benchmark (warm-up dibuang, rata-rata {RUNS} run):")
res_a = benchmark("A: simple aggregation", op_a)
res_b = benchmark("B: relational+temporal", op_b)
res_a["rows_in"] = res_b["rows_in"] = n_adm
res_a["rows_out"] = op_a.count()
res_b["rows_out"] = op_b.count()
res_a["intermediate_rows"] = n_adm
res_b["intermediate_rows"] = stays.count()          # baris setelah explode per hari
bench = pd.DataFrame([res_a, res_b])
bench["slowdown_vs_A"] = (bench["avg_sec"] / bench.loc[0, "avg_sec"]).round(2)
bench.to_csv(f"{RESULTS_DIR}/benchmark_A_vs_B.csv", index=False)
print("\n" + bench.to_string(index=False))

if SHOW_EXPLAIN:
    print("\n--- explain() Operation A ---")
    op_a.explain()
    print("--- explain() Operation B ---")
    op_b.explain()

ratio = res_b["avg_sec"] / max(res_a["avg_sec"], 1e-9)
print(f"\nKESIMPULAN: Operation B {ratio:.1f}x lebih lambat dari A pada admisi yang sama "
      f"({n_adm:,}). Shuffle A={res_a['shuffle_exchanges']} vs B={res_b['shuffle_exchanges']}; "
      f"data antara membengkak dari {n_adm:,} menjadi {res_b['intermediate_rows']:,} baris "
      f"(explode per hari rawat). Penyebab: struktur relasional (join) dan desain query "
      f"temporal, bukan ukuran data awal.")

write_parquet(op_b, f"{LAKE_DIR}/bor_daily")

# =====================================================================
# 5. (OPSIONAL) MAPREDUCE DEMO - M5
# =====================================================================
if RUN_MAPREDUCE_DEMO:
    print("\n=== [3] MAPREDUCE (RDD) vs DATAFRAME ===")
    t = time.perf_counter()
    rdd_counts = (adm.select("ward_id").rdd
                  .map(lambda r: (r[0], 1))
                  .reduceByKey(lambda a, b: a + b)
                  .collect())
    print(f"RDD map->reduceByKey: {time.perf_counter() - t:.3f}s  hasil={sorted(rdd_counts)}")

# =====================================================================
# 6. VELOCITY: simulasi micro-batch per bulan
# =====================================================================
print("\n=== [4] VELOCITY: SIMULASI MICRO-BATCH (arrival disimulasikan) ===")
stream_dir = f"{LAKE_DIR}/stream_sim"
shutil.rmtree(stream_dir, ignore_errors=True)

adm_m = adm.withColumn("_batch", F.date_format("admit_time", "yyyy-MM"))
batches = [r[0] for r in adm_m.select("_batch").distinct().orderBy("_batch").collect()]
velocity_log, prev_arrival = [], None

for b in batches:
    batch = adm_m.filter(F.col("_batch") == b).drop("_batch").cache()
    n = batch.count()                                   # materialisasi sebelum diukur
    arrival = datetime.datetime.now()

    t0 = time.perf_counter()
    write_parquet(batch, stream_dir, mode="append")     # ingestion ke data lake
    ingest_s = time.perf_counter() - t0

    t1 = time.perf_counter()
    run_action(batch.groupBy("ward_id").count())        # processing batch
    proc_s = time.perf_counter() - t1

    gap = (arrival - prev_arrival).total_seconds() if prev_arrival else None
    prev_arrival = arrival
    total = ingest_s + proc_s
    velocity_log.append({
        "batch": b,
        "writer": WRITER,
        "arrival_ts": arrival.isoformat(timespec="milliseconds"),
        "interval_since_prev_s": round(gap, 3) if gap is not None else None,
        "rows": n,
        "ingest_latency_s": round(ingest_s, 3),
        "process_latency_s": round(proc_s, 3),
        "end_to_end_latency_s": round(total, 3),
        "throughput_rows_s": round(n / total, 1) if total > 0 else None,
    })
    batch.unpersist()

vel = pd.DataFrame(velocity_log)
vel.to_csv(f"{RESULTS_DIR}/velocity_log.csv", index=False)
print(vel.to_string(index=False))
print(f"\nRata-rata: ingest={vel.ingest_latency_s.mean():.3f}s, "
      f"process={vel.process_latency_s.mean():.3f}s, "
      f"throughput={vel.throughput_rows_s.mean():.0f} rows/s")

# =====================================================================
# 7. RINGKASAN SATU LAYAR (untuk direkam di video)
# =====================================================================
os.system("cls" if os.name == "nt" else "clear")
line = "=" * 70
print(line)
print(" SEGMEN 4 - DATA PROCESSING: RINGKASAN HASIL")
print(line)
print("\n[VOLUME] Skala & relasi data")
for r in profile:
    print(f"  {r['table']:<14} {r['rows']:>8,} baris  {r['columns']:>2} kolom")
print(f"  Relasi : admission_id (admissions-bed_capacity), ward_id (admissions-wards)")
print(f"  Diproses: {n_adm:,} admisi bersih")
print("\n[VOLUME] Operation A vs B (rata-rata %d run, warm-up dibuang)" % RUNS)
print(f"  {'Operasi':<26}{'Waktu':>9}{'Shuffle':>9}{'Baris antara':>14}")
print(f"  {'A: agregasi sederhana':<26}{res_a['avg_sec']:>8.3f}s{res_a['shuffle_exchanges']:>9}{res_a['intermediate_rows']:>14,}")
print(f"  {'B: join + temporal':<26}{res_b['avg_sec']:>8.3f}s{res_b['shuffle_exchanges']:>9}{res_b['intermediate_rows']:>14,}")
print(f"  -> B {ratio:.1f}x lebih lambat. Penyebab: struktur (join, explode per hari)")
print("     dan desain query, BUKAN ukuran data (jumlah admisi sama).")
print("\n[VELOCITY] Simulasi micro-batch per bulan (arrival disimulasikan)")
print(f"  Jumlah batch      : {len(vel)}  (writer: {WRITER})")
print(f"  Rows per batch    : rata-rata {vel.rows.mean():,.0f}")
print(f"  Ingestion latency : rata-rata {vel.ingest_latency_s.mean():.3f} s")
print(f"  Process latency   : rata-rata {vel.process_latency_s.mean():.3f} s")
print(f"  Throughput        : rata-rata {vel.throughput_rows_s.mean():,.0f} baris/detik")
print(f"  Interval antar batch: rata-rata {vel.interval_since_prev_s.mean():.2f} s")
print("\n  Per batch:")
print(vel[["batch", "rows", "ingest_latency_s", "process_latency_s", "throughput_rows_s"]]
      .to_string(index=False))
print("\n" + line)
print(" Output tersimpan: results/*.csv dan data/lake/bor_daily")
print(line)
spark.stop()
