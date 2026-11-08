"""
SEGMEN 1 (Project Lead / Analyst) - Masalah kesehatan & karakteristik data  [M1]
Studi Kasus 2: Hospital Resource Utilisation (Data Sintetis / Tanpa Data Pasien Nyata)

Pertanyaan kesehatan:
  "Bagaimana pola pemanfaatan tempat tidur (Bed Occupancy Rate / BOR) dan lama rawat inap (Length of Stay / LOS)
   di berbagai bangsal rumah sakit sepanjang 2025, dan apa implikasinya untuk perencanaan kapasitas tempat tidur?"

Keluaran:
  - data/raw/admissions_full.csv (dan encounters_full.csv)
  - data/raw/bed_capacity.json (dan observations.json)
  - data/raw/wards.csv (dan diagnoses.csv)
  - data/raw/DATA_DICTIONARY.md
  - results/01_profile.json
"""
import os, json, shutil
import numpy as np, pandas as pd
from config import *

ensure_dirs(reset_data=True)
rng = np.random.default_rng(SEED)

N_ADM, N_PAT = 30_000, 9_000
RAINY = [1, 2, 3, 10, 11, 12]  # Musim hujan (peningkatan kunjungan IGD, infeksi, dan anak)

# Konfigurasi Bangsal Rumah Sakit (Ward & Bed Capacity)
WARDS_CONFIG = {
    "W_EMERG": {
        "ward_name": "Emergency Ward",
        "department": "Emergency",
        "total_beds": 30,
        "p": 0.22,
        "age": (1, 90),
        "los": (1, 3),          # Length of Stay dalam hari
        "bed_type": "Observation Bed",
        "daily_rate": 500_000,
        "has_ventilator": False,
        "target_bor": 75.0,
    },
    "W_INTMED": {
        "ward_name": "Internal Medicine Ward",
        "department": "Internal Medicine",
        "total_beds": 65,
        "p": 0.24,
        "age": (25, 90),
        "los": (3, 10),
        "bed_type": "Standard Ward Bed",
        "daily_rate": 750_000,
        "has_ventilator": False,
        "target_bor": 85.0,
    },
    "W_ICU": {
        "ward_name": "Intensive Care Unit",
        "department": "Critical Care",
        "total_beds": 25,
        "p": 0.10,
        "age": (30, 85),
        "los": (4, 12),
        "bed_type": "ICU Bed",
        "daily_rate": 2_500_000,
        "has_ventilator": True,
        "target_bor": 80.0,
    },
    "W_CARD": {
        "ward_name": "Cardiology Ward",
        "department": "Cardiology",
        "total_beds": 35,
        "p": 0.12,
        "age": (35, 90),
        "los": (3, 9),
        "bed_type": "Cardiac Care Bed",
        "daily_rate": 1_200_000,
        "has_ventilator": False,
        "target_bor": 80.0,
    },
    "W_PED": {
        "ward_name": "Pediatrics Ward",
        "department": "Pediatrics",
        "total_beds": 40,
        "p": 0.16,
        "age": (0, 17),
        "los": (2, 6),
        "bed_type": "Pediatric Bed",
        "daily_rate": 700_000,
        "has_ventilator": False,
        "target_bor": 80.0,
    },
    "W_SURG": {
        "ward_name": "Surgery Ward",
        "department": "Surgery",
        "total_beds": 45,
        "p": 0.16,
        "age": (12, 80),
        "los": (3, 8),
        "bed_type": "Surgical Bed",
        "daily_rate": 1_000_000,
        "has_ventilator": False,
        "target_bor": 85.0,
    },
}

# Tabel Master Bangsal (Reference Dataset)
ward_list = list(WARDS_CONFIG.keys())
WARDS_REF = pd.DataFrame([
    {
        "ward_id": wid,
        "ward_name": cfg["ward_name"],
        "department": cfg["department"],
        "total_beds": cfg["total_beds"],
        "bed_type": cfg["bed_type"],
        "daily_rate": cfg["daily_rate"],
        "target_bor_pct": cfg["target_bor"],
    }
    for wid, cfg in WARDS_CONFIG.items()
])

p_ward = np.array([WARDS_CONFIG[w]["p"] for w in ward_list])
p_ward /= p_ward.sum()

# ---------- 1. DATA ADMISI (CSV) ----------
cand = 45_000
admit_all = pd.to_datetime("2025-01-01") + pd.to_timedelta(rng.integers(0, 365 * 86400, cand), unit="s")
# Pola musiman: musim hujan (Okt-Mar) lonjakan admisi IGD, Anak, dan Rawat Inap ~30%
keep = rng.random(cand) < np.where(np.isin(admit_all.month, RAINY), 1.0, 0.72)
admit = admit_all[keep][:N_ADM]
n = len(admit)
rainy = np.isin(admit.month, RAINY)

selected_wards = rng.choice(ward_list, n, p=p_ward)
age = np.zeros(n, dtype=int)
los_h = np.zeros(n, dtype=int)
dept_names = np.empty(n, dtype=object)
adm_type = rng.choice(["Emergency", "Elective", "Urgent"], n, p=[0.50, 0.30, 0.20])

for wid in ward_list:
    m = selected_wards == wid
    cfg = WARDS_CONFIG[wid]
    dept_names[m] = cfg["department"]
    lo_age, hi_age = cfg["age"]
    age[m] = rng.integers(lo_age, hi_age + 1, m.sum())
    
    # Lama rawat inap (Length of Stay) dalam jam
    lo_los, hi_los = cfg["los"]
    base_los = rng.integers(lo_los * 24, hi_los * 24 + 1, m.sum())
    # Di musim hujan, pasien anak & penyakit dalam cenderung dirawat lebih lama
    if wid in ["W_PED", "W_INTMED"]:
        base_los = np.where(rainy[m], (base_los * 1.25).astype(int), base_los)
    los_h[m] = base_los

gender = rng.choice(["F", "M"], n)
fmt = "%Y-%m-%d %H:%M:%S"

adm = pd.DataFrame({
    "admission_id": [f"ADM{i:06d}" for i in range(n)],
    "patient_id": [f"P{p:05d}" for p in rng.integers(0, N_PAT, n)],
    "ward_id": selected_wards,
    "department": dept_names,
    "admission_type": adm_type,
    "age": age,
    "gender": gender,
    "admit_time": admit.strftime(fmt),
    "discharge_time": (admit + pd.to_timedelta(los_h, unit="h")).strftime(fmt),
})
# Alias kompatibilitas
adm["encounter_id"] = adm["admission_id"]
adm["dx_code"] = adm["ward_id"]

# ---------- 2. DATA UTILISASI TEMPAT TIDUR (JSON BERSARANG) ----------
# Setiap admisi memiliki catatan alokasi & monitoring utilisasi bed (1-3 catatan per admisi)
admit_np = admit.values
parts = []
for wid in ward_list:
    m = selected_wards == wid
    cfg = WARDS_CONFIG[wid]
    idx_ward = np.where(m)[0]
    # 1 sampai 2 pembacaan status bed per pasien
    idx_repeat = np.repeat(idx_ward, rng.integers(1, 3, len(idx_ward)))
    n_recs = len(idx_repeat)
    
    # Generate bed ID sesuai kapasitas bangsal
    bed_num = rng.integers(1, cfg["total_beds"] + 1, n_recs)
    bed_ids = [f"{wid}_B{b:03d}" for b in bed_num]
    
    # Timestamp pemantauan tempat tidur selama periode rawat
    ts = admit_np[idx_repeat] + (rng.random(n_recs) * los_h[idx_repeat] * 3600).astype("timedelta64[s]")
    
    # Variasi tarif harian (termasuk biaya alkes/tindakan)
    rate_val = np.round(rng.normal(cfg["daily_rate"], cfg["daily_rate"] * 0.08), -3)
    
    # Status keterisian bed
    status_choice = rng.choice(["Occupied", "Under_Review", "Reserved"], n_recs, p=[0.92, 0.05, 0.03])
    
    df_part = pd.DataFrame({
        "admission_id": adm.admission_id.values[idx_repeat],
        "encounter_id": adm.admission_id.values[idx_repeat],
        "ward_id": wid,
        "bed_id": bed_ids,
        "record_time": pd.to_datetime(ts).strftime(fmt),
        "obs_time": pd.to_datetime(ts).strftime(fmt),
        "source": rng.choice(["bed_management_system", "iot_smart_bed"], n_recs, p=[0.7, 0.3]),
        "bed_type": cfg["bed_type"],
        "daily_rate": rate_val,
        "occupancy_status": status_choice,
        "has_ventilator": cfg["has_ventilator"],
    })
    parts.append(df_part)

bed_recs = pd.concat(parts, ignore_index=True).sample(frac=1, random_state=SEED).reset_index(drop=True)
bed_recs.insert(0, "record_id", [f"REC{i:07d}" for i in range(len(bed_recs))])
bed_recs["obs_id"] = bed_recs["record_id"]

# ---------- 3. INJEKSI MASALAH KUALITAS DATA (VERACITY) ----------
issues = {}
def pick(df, frac, seed):
    return df.sample(frac=frac, random_state=seed).index

# Isu 1: Department / Ward kosong (NULL)
i = pick(adm, 0.02, 3)
adm.loc[i, "department"] = None
issues["admisi: department kosong"] = len(i)

# Isu 2: Format penulisan department/bangsal tidak konsisten
i = pick(adm[adm.department.notna()], 0.04, 9)
adm.loc[i, "department"] = [(lambda s, k: [s.lower(), s.upper(), f" {s} "][k])(s, k)
                            for s, k in zip(adm.loc[i, "department"], rng.integers(0, 3, len(i)))]
issues["admisi: format department tidak konsisten"] = len(i)

# Isu 3: Waktu discharge hilang (pasien masih aktif rawat inap atau data hilang)
i = pick(adm, 0.03, 4)
adm.loc[i, "discharge_time"] = None
issues["admisi: discharge_time kosong"] = len(i)

# Isu 4: Anomali waktu: discharge lebih awal dari admit
i = pick(adm[adm.discharge_time.notna()], 0.01, 5)
adm.loc[i, "discharge_time"] = (pd.to_datetime(adm.loc[i, "admit_time"]) - pd.Timedelta(days=1)).dt.strftime(fmt)
issues["admisi: discharge lebih awal dari admit"] = len(i)

# Isu 5: Umur pasien tidak valid (< 0 atau > 120 tahun)
i = pick(adm, 0.005, 10)
adm.loc[i, "age"] = rng.choice([-1, 150], len(i))
issues["admisi: umur tidak valid"] = len(i)

# Isu 6: Baris duplikat (duplicate admission records)
dup = adm.sample(frac=0.03, random_state=1)
adm = pd.concat([adm, dup], ignore_index=True).sample(frac=1, random_state=2).reset_index(drop=True)
issues["admisi: baris duplikat"] = len(dup)

# Isu 7: Nilai biaya bed kosong (NULL)
i = pick(bed_recs, 0.03, 7)
bed_recs.loc[i, "daily_rate"] = np.nan
issues["bed_records: biaya harian kosong"] = len(i)

# Isu 8: Nilai tidak masuk akal / outlier ekstrem pada tarif kamar
i = pick(bed_recs[bed_recs.daily_rate.notna()], 0.005, 11)
bed_recs.loc[i, "daily_rate"] = -500_000.0
issues["bed_records: tarif negatif tidak logis"] = len(i)

# Isu 9: Orphan admission_id (ID admisi di data log bed tidak terdaftar di data admisi)
i = pick(bed_recs, 0.003, 13)
bed_recs.loc[i, "admission_id"] = [f"ADM9{x:05d}" for x in rng.integers(0, 99999, len(i))]
bed_recs.loc[i, "encounter_id"] = bed_recs.loc[i, "admission_id"]
issues["bed_records: admission_id yatim (orphan)"] = len(i)

# ---------- 4. PENYIMPANAN FILE SUMBER ----------
# Simpan CSV admisi (nama utama dan alias)
adm.to_csv(f"{RAW}/admissions_full.csv", index=False)
adm.to_csv(f"{RAW}/encounters_full.csv", index=False)

# Simpan CSV referensi bangsal
WARDS_REF.to_csv(f"{RAW}/wards.csv", index=False)
# Buat tabel diagnoses.csv kompatibel (memetakan ward_id ke deskripsi)
dx_compat = pd.DataFrame({
    "dx_code": WARDS_REF["ward_id"],
    "dx_description": WARDS_REF["ward_name"] + " (" + WARDS_REF["department"] + ")",
})
dx_compat.to_csv(f"{RAW}/diagnoses.csv", index=False)

# Simpan JSON bersarang untuk data utilisasi tempat tidur
def write_nested_json(filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        for r in bed_recs.itertuples(index=False):
            rate_val = None if pd.isna(r.daily_rate) else float(r.daily_rate)
            obj = {
                "record_id": r.record_id,
                "admission_id": r.admission_id,
                "encounter_id": r.admission_id,
                "ward_id": r.ward_id,
                "bed_id": r.bed_id,
                "record_time": r.record_time,
                "obs_time": r.record_time,
                "source": r.source,
                "bed_specs": {
                    "bed_type": r.bed_type,
                    "daily_rate": rate_val,
                    "occupancy_status": r.occupancy_status,
                    "has_ventilator": bool(r.has_ventilator),
                },
                # Format kompatibel dengan observasi
                "observation": {
                    "type": r.bed_type,
                    "value": rate_val,
                    "unit": "IDR",
                }
            }
            f.write(json.dumps(obj, separators=(",", ":")) + "\n")

write_nested_json(f"{RAW}/bed_capacity.json")
write_nested_json(f"{RAW}/observations.json")

# Dokumentasi Kamus Data
with open(f"{RAW}/DATA_DICTIONARY.md", "w", encoding="utf-8") as f:
    f.write("""# Kamus Data: Studi Kasus 2 - Hospital Resource Utilisation
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
""")

profile = {
    "case_study": "Hospital Resource Utilisation",
    "case_study_no": 2,
    "central_question": (
        "Bagaimana pola pemanfaatan tempat tidur (Bed Occupancy Rate / BOR) dan lama rawat inap "
        "(Length of Stay / LOS) di berbagai bangsal sepanjang 2025, dan apa implikasinya untuk "
        "perencanaan kapasitas dan alokasi tempat tidur rumah sakit?"
    ),
    "admission_rows": len(adm),
    "unique_admissions": int(adm.admission_id.nunique()),
    "unique_patients": int(adm.patient_id.nunique()),
    "bed_record_rows": len(bed_recs),
    "ward_rows": len(WARDS_REF),
    "total_hospital_beds": int(WARDS_REF.total_beds.sum()),
    "records_per_admission": round(len(bed_recs) / n, 2),
    "size_admissions_mb": round(os.path.getsize(f"{RAW}/admissions_full.csv") / 1e6, 2),
    "size_bed_capacity_mb": round(os.path.getsize(f"{RAW}/bed_capacity.json") / 1e6, 2),
    "period": f"{adm.admit_time.min()[:10]} s/d {adm.admit_time.max()[:10]}",
    "wards": [w["ward_name"] for w in WARDS_CONFIG.values()],
    "injected_issues": issues,
}
save_json("01_profile", profile)

print("=== DATASET SINTETIS STUDI KASUS 2 (HOSPITAL RESOURCE UTILISATION) DIBUAT ===")
for k, v in profile.items():
    print(f"{k:26s}: {v}")
