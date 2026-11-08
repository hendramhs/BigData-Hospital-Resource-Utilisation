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
