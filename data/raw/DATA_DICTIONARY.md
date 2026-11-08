# Kamus Data: Studi Kasus 2 - Hospital Resource Utilisation
*(Data sintetis untuk demonstrasi Big Data 5Vs - tidak mengandung data medis riil)*

## 1. admissions_full.csv (Format CSV - Tabular)
Satu baris merepresentasikan satu peristiwa admisi pasien rawat inap ke bangsal rumah sakit.
- `admission_id`: Identitas unik kunjungan rawat inap (contoh: ADM000001).
- `patient_id`: Identitas unik pasien (relasi 1 pasien ke banyak admisi).
- `ward_id`: Kode bangsal rumah sakit (W_EMERG, W_INTMED, W_ICU, W_CARD, W_PED, W_SURG).
- `department`: Nama departemen / spesialisasi layanan rawat inap.
- `admission_type`: Jenis admisi (Emergency, Elective, Urgent).
- `age`: Umur pasien saat masuk rawat inap.
- `gender`: Jenis kelamin (M / F).
- `admit_time`: Waktu penerimaan masuk bangsal (YYYY-MM-DD HH:MM:SS).
- `discharge_time`: Waktu pemulangan dari bangsal (YYYY-MM-DD HH:MM:SS).

## 2. bed_capacity.json (Format JSON Lines - Bersarang / Nested)
Satu baris merepresentasikan satu catatan audit alokasi & status utilisasi tempat tidur.
- `record_id`: ID unik pencatatan log.
- `admission_id`: Relasi ke tabel admisi pasien (FK).
- `ward_id`: Kode bangsal.
- `bed_id`: Nomor spesifik tempat tidur di bangsal (contoh: W_ICU_B005).
- `record_time`: Waktu perekaman status tempat tidur.
- `source`: Sumber sistem (bed_management_system / iot_smart_bed).
- `bed_specs` (Nested Object):
  - `bed_type`: Tipe tempat tidur (ICU Bed, Standard Ward Bed, Pediatric Bed, dsb.).
  - `daily_rate`: Tarif operasional per hari (IDR).
  - `occupancy_status`: Status keterisian (Occupied, Under_Review, Reserved).
  - `has_ventilator`: Fasilitas bantuan ventilator (true / false).

## 3. wards.csv (Format CSV - Tabel Referensi Master)
Kapasitas terpasang dan target operasional per bangsal.
- `ward_id`: Kode unik bangsal (PK).
- `ward_name`: Nama lengkap bangsal rawat inap.
- `department`: Departemen pengelola.
- `total_beds`: Kapasitas resmi total tempat tidur bangsal.
- `target_bor_pct`: Target persentase keterisian optimal (Bed Occupancy Rate ideal: 75-85%).

### Relasi & Struktur Volume:
`admissions` (1) --- (N) `bed_capacity / bed_records` melalui `admission_id`.
`wards` (1) --- (N) `admissions` melalui `ward_id`.
