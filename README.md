# Hospital Resource & Bed Occupancy Analytics Pipeline
### Scalable Healthcare Data Engineering & Analytics Prototype using the 5Vs Big Data Architecture

[![Engine](https://img.shields.io/badge/Engine-Apache%20Spark%203.5-orange.svg)](https://spark.apache.org/)
[![Language](https://img.shields.io/badge/Language-Python%20%7C%20PySpark-brightgreen.svg)](https://www.python.org/)
[![Storage](https://img.shields.io/badge/Storage-Apache%20Parquet%20Lake-blue.svg)](https://parquet.apache.org/)
[![Analytics](https://img.shields.io/badge/Analytics-Spark%20SQL-yellow.svg)](https://spark.apache.org/sql/)


---

## 🏥 Executive Overview

**Hospital Resource & Bed Occupancy Analytics** adalah prototype data pipeline berskala besar yang dirancang untuk mengatasi tantangan operasional rumah sakit modern: **manajemen kapasitas tempat tidur dan alokasi sumber daya medis secara tepat waktu**.

Dalam operasional layanan kesehatan, ketidakseimbangan antara laju admisi pasien dan kapasitas bangsal (khususnya unit rawat intensif seperti ICU) sering menyebabkan *bottleneck*, waktu tunggu pasien membengkak, dan utilisasi fasilitas yang tidak merata. Proyek ini mengimplementasikan arsitektur pemrosesan data terdistribusi berbasis **Apache Spark** untuk mengintegrasikan data admisi, memvalidasi integritas data, dan menghitung indikator kritis rumah sakit seperti **Bed Occupancy Rate (BOR)** serta **Length of Stay (LOS)**.

---

## 🎯 Problem Statement & Analytics Scope

Proyek ini menjawab kebutuhan komputasi analitik berikut:
> *"Bagaimana mengintegrasikan aliran data admisi multi-format, mendeteksi anomali pencatatan log rumah sakit, dan memproyeksikan tingkat keterisian tempat tidur (BOR) secara efisien guna mencegah overcapacity pada bangsal kritis?"*

### Pertanyaan Analitik Utama:
1. **Capacity Thresholds:** Bangsal mana yang melampaui ambang batas ideal BOR (75%–85%) dan berisiko mengalami kelangkaan tempat tidur?
2. **Seasonal Variance:** Bagaimana pola fluktuasi admisi sepanjang tahun mempengaruhi kebutuhan tempat tidur aktif?
3. **Turnaround & Length of Stay:** Bagaimana korelasi antara jenis tindakan/bangsal rawat inap dengan lama hari rawat pasien?

---

## ⚙️ The 5Vs Big Data Architecture

Sistem ini dirancang dengan menerapkan prinsip **5Vs of Big Data**:

```
                       THE 5Vs ARCHITECTURE
┌──────────────────────────────────────────────────────────────────┐
│  1. VOLUME    │ 30.000+ Rawat Inap & 45.000+ Log Utilisasi       │
│  2. VELOCITY  │ Simulasi Micro-batch Ingestion & Latency Profiling│
│  3. VARIETY   │ Integrasi Multi-Format: Tabular CSV + Nested JSON │
│  4. VERACITY  │ Automated Data Quality Cleansing & Validation    │
│  5. VALUE     │ Bed Occupancy Rate, LOS, & Capacity Forecasting  │
└──────────────────────────────────────────────────────────────────┘
```

### 1. Volume (Scale & Computational Complexity)
* Pemrosesan lebih dari **30.000 data admisi pasien** yang berelasi dengan **45.000+ log audit tempat tidur** di 6 bangsal rumah sakit (total kapasitas 240 tempat tidur terpasang).
* Evaluasi komputasi relasional kompleks (*multi-table shuffle joins* dan agregasi temporal) dibandingkan dengan agregasi sederhana pada berbagai skala data ($x1$, $x5$, $x10$).

### 2. Velocity (Data Arrival & Ingestion Timeliness)
* Simulasi aliran data berkala (*periodic batch ingestion*) per periode bulan/shift.
* Pipeline mencatat metrik latensi penulisan (*ingestion latency*) ke Parquet Data Lake dan menghitung *system throughput* (catatan data per detik).

### 3. Variety (Heterogeneous Data Integration)
* Menangani dan mengintegrasikan dua format data dengan skema berbeda:
  * **Tabular Relational (CSV):** Data demografi dan peristiwa admisi pasien (`admissions_full.csv`).
  * **Semi-structured Hierarchical (Nested JSON Lines):** Spesifikasi alkes dan status keterisian bed (`bed_capacity.json`).
  * **Master Reference (CSV):** Tabel kapasitas resmi bangsal (`wards.csv`).

### 4. Veracity (Data Quality & Integrity Management)
* Pipeline mendeteksi dan melakukan koreksi otomatis terhadap anomali pencatatan:
  * *Missing discharge timestamps* pada admisi aktif.
  * *Temporal inconsistency* (waktu keluar sebelum waktu masuk).
  * Standarisasi format penamaan bangsal dan departemen (*casing & whitespace normalization*).
  * Validasi nilai batas wajar umur pasien dan eliminasi catatan duplikat (*deduplication*).
  * Deteksi rekaman yatim (*orphan records*) pada relasi log bed terhadap ID admisi.

### 5. Value (Actionable Healthcare Intelligence)
* Menghasilkan metrik operasional terukur:
  * Kalkulasi harian **Bed Occupancy Rate (BOR %)** per bangsal.
  * Distribusi rata-rata **Length of Stay (LOS)** per spesialisasi medis.
  * Rekomendasi manajerial untuk alokasi tempat tidur fleksibel (*surge capacity*) saat musim puncak.

---

## 🏛️ System Pipeline Workflow

```
[Raw Data Sources]
  ├── admissions_full.csv  ── (Patient Admissions: CSV)
  ├── bed_capacity.json    ── (Bed Specifications & Logs: Nested JSON)
  └── wards.csv            ── (Ward Capacity Reference: CSV)
               │
               ▼
[Ingestion Layer]
  └── Periodic Micro-batch Simulator (PySpark Batch Streaming + Ingest Timestamps)
               │
               ▼
[Storage Layer - Raw Lake]
  └── Apache Parquet Storage (data/lake/)
               │
               ▼
[Data Cleaning & Integration Layer]
  ├── Automated Veracity Rules (Dedup, Range Checks, Timestamp Audits)
  └── Schema Normalization & Entity Join (CSV + Flattened JSON)
               │
               ▼
[Processing & Analytics Engine]
  ├── PySpark DataFrame API & Spark SQL Optimization
  └── Execution Plan Benchmarking (Catalyst Optimizer & Shuffle Evaluation)
               │
               ▼
[Output & Healthcare Value Delivery]
  ├── Executive Operational Dashboard (PNG / Results)
  ├── Performance Benchmark Logs (CSV / JSON)
  └── Automated System Architecture & Audit Reports
```

---

## 📂 Repository Structure
Belum fixed
```text
├── data/
│   ├── raw/                  # Sumber data mentah heterogen (CSV & JSON)
├── results/                  # Metrik evaluasi, performa query, & grafik analitik
├── 01_generate_data.py       # Generator data sintetis berskala besar & injektor anomali
├── requirements.txt          # Spesifikasi dependensi pustaka Python
└── README.md                 # Dokumentasi proyek
```

---

## 👥 Engineering & Analytics Team

| Role | Responsibility Focus | 5Vs Domain |
|:---|:---|:---:|
| **Project Lead / Solution Analyst** | Problem framing, domain requirements, & dataset architecture | All 5Vs |
| **System Engineer** | Data ingestion flow, lake storage, & velocity tracking | Volume & Velocity |
| **Data Engineer** | Schema transformation, multi-format integration, & data veracity | Variety & Veracity |
| **Big Data Developer** | Distributed processing, query plans, & execution benchmarks | Volume & Velocity |
| **Data Analyst** | SQL query execution, dashboard visualization, & domain insights | Veracity & Value |

---

## 🚀 Quickstart & Execution

### 1. Prerequisites
* **Python**: 3.10 atau versi di atasnya
* **Java**: JRE / JDK 8, 11, 17, atau 21 (diperlukan oleh Apache Spark runtime)
* **Windows**: Spark juga memerlukan Hadoop helper (`winutils.exe`) dan
  environment variable `HADOOP_HOME`. Untuk instalasi PySpark melalui pip,
  tetapkan juga `SPARK_HOME` ke folder `site-packages\pyspark`. Alternatif yang lebih mudah untuk
  demonstrasi adalah menjalankan pipeline di Google Colab/Linux.

### 2. Dependency Setup
Pasang dependensi Python yang dibutuhkan:
```bash
pip install -r requirements.txt
```

### 3. Generate and process the dataset
Generate the synthetic source data:
```powershell
python .\01_generate_data.py
```

Run the storage, integration, and quality pipeline:
```powershell
python .\02_storage_integration_quality.py
```

> **Note for Windows:** `config.py` (`get_spark()`) automatically configures `SPARK_HOME`, `JAVA_HOME`, `HADOOP_HOME`, `TEMP`/`TMP` (`C:\spark-tmp`), and clears `TERM_PROGRAM` so the pipeline runs directly in VS Code / PowerShell without manual environment variable setup.

The second script writes the source datasets to `data/lake/` as Parquet,
flattens the nested `bed_specs` JSON object, validates and cleans the data,
joins admissions with bed logs and ward capacity, and writes the integrated
dataset to `data/clean/integrated_resource_records/`. The before-and-after
quality evidence is saved in `results/02_quality_report.json`.
