"""Storage, integration, and data-quality pipeline for the hospital dataset."""
from config import CLEAN, INCOMING, LAKE, RAW, RESULTS, ensure_dirs, get_spark


def main():
    from pyspark.sql import functions as F

    ensure_dirs()
    spark = get_spark("HospitalResource-StorageIntegrationQuality")

    admissions = (
        spark.read.schema(adm_schema()).option("header", True).csv(
            f"{RAW}/admissions_full.csv"
        )
        .withColumn("_source_file", F.lit("admissions_full.csv"))
    )
    beds = (
        spark.read.schema(bed_schema()).json(f"{RAW}/bed_capacity.json")
        .withColumn("_source_file", F.lit("bed_capacity.json"))
    )
    wards = (
        spark.read.schema(wards_schema()).option("header", True).csv(
            f"{RAW}/wards.csv"
        )
        .withColumn("_source_file", F.lit("wards.csv"))
    )

    write_parquet(admissions, f"{LAKE}/admissions")
    write_parquet(beds, f"{LAKE}/bed_capacity")
    write_parquet(wards, f"{LAKE}/wards")

    admissions_raw = admissions.cache()
    beds_raw = beds.cache()

    admissions_prepared = (
        admissions_raw
        .withColumn("admit_ts", F.to_timestamp("admit_time"))
        .withColumn("discharge_ts", F.to_timestamp("discharge_time"))
        .withColumn("department_normalized", normalize_department("department"))
        .withColumn(
            "age_valid",
            F.col("age").between(0, 120),
        )
        .withColumn(
            "temporal_valid",
            F.col("discharge_ts").isNull()
            | (F.col("discharge_ts") >= F.col("admit_ts")),
        )
    )

    quality_before = {
        "admission_rows": admissions_raw.count(),
        "duplicate_admission_rows": duplicate_rows(admissions_raw, "admission_id"),
        "missing_department_rows": admissions_raw.filter(
            F.col("department").isNull() | (F.trim("department") == "")
        ).count(),
        "missing_discharge_rows": admissions_raw.filter(
            F.col("discharge_time").isNull() | (F.trim("discharge_time") == "")
        ).count(),
        "invalid_age_rows": admissions_prepared.filter(~F.col("age_valid")).count(),
        "invalid_temporal_rows": admissions_prepared.filter(
            ~F.col("temporal_valid")
        ).count(),
        "bed_rows": beds_raw.count(),
        "missing_daily_rate_rows": beds_raw.filter(
            F.col("bed_specs.daily_rate").isNull()
        ).count(),
        "negative_daily_rate_rows": beds_raw.filter(
            F.col("bed_specs.daily_rate") < 0
        ).count(),
    }

    admissions_clean = (
        admissions_prepared
        .withColumn(
            "department",
            F.coalesce(F.col("department_normalized"), F.lit("UNKNOWN")),
        )
        .withColumn("age", F.when(F.col("age_valid"), F.col("age")))
        .withColumn(
            "discharge_ts",
            F.when(F.col("temporal_valid"), F.col("discharge_ts")),
        )
        .dropDuplicates(["admission_id"])
        .drop(
            "department_normalized",
            "age_valid",
            "temporal_valid",
            "_source_file",
        )
    )

    beds_flat = beds_raw.select(
        "record_id",
        "admission_id",
        "ward_id",
        "bed_id",
        "record_time",
        "source",
        F.col("bed_specs.bed_type").alias("bed_type"),
        F.col("bed_specs.daily_rate").alias("daily_rate"),
        F.col("bed_specs.occupancy_status").alias("occupancy_status"),
        F.col("bed_specs.has_ventilator").alias("has_ventilator"),
    )
    valid_admission_ids = admissions_clean.select("admission_id").distinct()
    orphan_bed_rows = beds_flat.join(
        valid_admission_ids, "admission_id", "left_anti"
    ).count()
    beds_clean = (
        beds_flat
        .withColumn(
            "daily_rate",
            F.when(F.col("daily_rate") >= 0, F.col("daily_rate")),
        )
        .join(valid_admission_ids, "admission_id", "left_semi")
    )

    write_parquet(admissions_clean, f"{CLEAN}/admissions")
    write_parquet(beds_clean, f"{CLEAN}/bed_capacity")
    write_parquet(wards, f"{CLEAN}/wards")

    wards_for_join = wards.select(
        "ward_id",
        "ward_name",
        F.col("department").alias("ward_department"),
        "total_beds",
        F.col("bed_type").alias("standard_bed_type"),
        F.col("daily_rate").alias("standard_daily_rate"),
        "target_bor_pct",
    )
    integrated = (
        beds_clean.drop("ward_id").join(admissions_clean, "admission_id", "inner")
        .join(wards_for_join, "ward_id", "inner")
        .withColumn(
            "los_days",
            F.when(
                F.col("discharge_ts").isNotNull(),
                (F.unix_timestamp("discharge_ts") - F.unix_timestamp("admit_ts"))
                / F.lit(86400),
            ),
        )
    )
    write_parquet(integrated, f"{CLEAN}/integrated_resource_records")

    quality_after = {
        "clean_admission_rows": admissions_clean.count(),
        "clean_unique_admissions": admissions_clean.select("admission_id").distinct().count(),
        "clean_bed_rows": beds_clean.count(),
        "orphan_bed_rows_removed": orphan_bed_rows,
        "invalid_temporal_values_remaining": admissions_clean.filter(
            F.col("discharge_ts").isNotNull()
            & (F.col("discharge_ts") < F.col("admit_ts"))
        ).count(),
        "negative_daily_rate_values_remaining": beds_clean.filter(
            F.col("daily_rate") < 0
        ).count(),
        "integrated_rows": integrated.count(),
    }

    save_json("02_quality_report", {"before": quality_before, "after": quality_after})
    print("=== SEGMEN 3: DATA STORAGE, INTEGRATION & QUALITY (M3-M4) ===")
    print("\n[1] VARIETY - Skema Sumber Heterogen (CSV Tabular vs Nested JSON):")
    print("  - admissions_full.csv (Tabular CSV):")
    admissions.drop("_source_file").printSchema()
    print("  - bed_capacity.json (Nested JSON dengan struct bed_specs):")
    beds.drop("_source_file").printSchema()

    print("[2] VERACITY - Bukti Kualitas Data (Before vs After Cleaning):")
    print("  --- BEFORE CLEANING ---")
    for k, v in quality_before.items():
        print(f"    {k:32s}: {v:,}")
    print("  --- AFTER CLEANING ---")
    for k, v in quality_after.items():
        print(f"    {k:36s}: {v:,}")

    print("\n[3] INTEGRATION - Contoh Hasil Join (Admissions + Flattened Beds + Wards):")
    integrated.select(
        "record_id",
        "admission_id",
        "ward_id",
        "department",
        "bed_type",
        "daily_rate",
        "occupancy_status",
        F.round("los_days", 2).alias("los_days"),
    ).show(5, truncate=False)

    print("=== STORAGE, INTEGRATION AND QUALITY PIPELINE COMPLETE ===")
    print(f"Raw Parquet  : {LAKE}")
    print(f"Clean Parquet: {CLEAN}")
    print(f"Quality JSON : {RESULTS}/02_quality_report.json")
    spark.stop()


def normalize_department(column):
    from pyspark.sql import functions as F

    value = F.upper(F.trim(column))
    return (
        F.when(value == "EMERGENCY", "Emergency")
        .when(value == "INTERNAL MEDICINE", "Internal Medicine")
        .when(value == "CRITICAL CARE", "Critical Care")
        .when(value == "CARDIOLOGY", "Cardiology")
        .when(value == "PEDIATRICS", "Pediatrics")
        .when(value == "SURGERY", "Surgery")
    )


def duplicate_rows(frame, key):
    return frame.count() - frame.select(key).distinct().count()


def write_parquet(frame, path):
    frame.write.mode("overwrite").parquet(path)


def adm_schema():
    from config import adm_schema as schema

    return schema()


def bed_schema():
    from config import bed_schema as schema

    return schema()


def wards_schema():
    from config import wards_schema as schema

    return schema()


def save_json(name, value):
    from config import save_json as writer

    writer(name, value)


if __name__ == "__main__":
    main()