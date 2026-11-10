"""Konfigurasi bersama untuk seluruh pipeline IFB-451 (ETS Big Data) - Studi Kasus 2: Hospital Resource Utilisation."""
import os, json, shutil

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
RAW = os.path.join(DATA, "raw")            # sumber: CSV + JSON (data sintetis)
INCOMING = os.path.join(DATA, "incoming")  # folder kedatangan batch (simulasi Velocity)
LAKE = os.path.join(DATA, "lake")          # hasil ingestion: Parquet (mentah)
CLEAN = os.path.join(DATA, "clean")        # hasil cleaning + integrasi: Parquet
RESULTS = os.path.join(ROOT, "results")
DOCS = os.path.join(ROOT, "docs")
SEED = 42


def ensure_dirs(reset_data=False):
    if reset_data and os.path.exists(DATA):
        shutil.rmtree(DATA)
    for d in (RAW, INCOMING, LAKE, CLEAN, RESULTS, DOCS):
        os.makedirs(d, exist_ok=True)


def get_spark(name="HospitalResource-5Vs"):
    import sys
    import pyspark
    from pyspark.sql import SparkSession

    os.environ.pop("TERM_PROGRAM", None)
    spark_home = os.path.dirname(pyspark.__file__)
    os.environ["SPARK_HOME"] = spark_home
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if os.name == "nt":
        if os.path.isdir(r"C:\PROGRA~1\Java\jdk-21"):
            os.environ["JAVA_HOME"] = r"C:\PROGRA~1\Java\jdk-21"
        elif os.path.isdir(r"C:\Program Files\Java\jdk-21"):
            os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-21"
        if os.path.isdir(r"C:\hadoop"):
            os.environ["HADOOP_HOME"] = r"C:\hadoop"
        spark_tmp = r"C:\spark-tmp"
        os.makedirs(spark_tmp, exist_ok=True)
        os.environ["TEMP"] = spark_tmp
        os.environ["TMP"] = spark_tmp

        required_paths = [
            os.path.join(spark_home, "bin"),
            os.path.join(os.environ.get("JAVA_HOME", ""), "bin"),
            os.path.join(os.environ.get("HADOOP_HOME", ""), "bin"),
            r"C:\Windows\System32",
            r"C:\Windows\System32\WindowsPowerShell\v1.0",
        ]
        current_path = os.environ.get("PATH", "")
        os.environ["PATH"] = ";".join(
            [p for p in required_paths if os.path.isdir(p)] + [current_path]
        )

        # Silence PySpark's noisy Windows taskkill atexit message after spark.stop()
        import atexit
        _orig_register = atexit.register

        def _quiet_register(func, *args, **kwargs):
            if getattr(func, "__name__", "") == "killChild":
                return func
            return _orig_register(func, *args, **kwargs)

        atexit.register = _quiet_register

    spark = (SparkSession.builder.master("local[*]").appName(name)
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.driver.memory", "3g")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def adm_schema():
    """Skema data admisi pasien rawat inap (CSV)"""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    return StructType([
        StructField("admission_id", StringType()), StructField("patient_id", StringType()),
        StructField("ward_id", StringType()), StructField("department", StringType()),
        StructField("diagnosis", StringType()),
        StructField("admission_type", StringType()), StructField("age", IntegerType()),
        StructField("gender", StringType()), StructField("admit_time", StringType()),
        StructField("discharge_time", StringType()),
    ])

def enc_schema():
    """Alias kompatibilitas untuk modul yang memanggil enc_schema"""
    return adm_schema()


def bed_schema():
    """JSON bersarang (nested): bed_capacity / utilisasi tempat tidur"""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
    return StructType([
        StructField("record_id", StringType()), StructField("admission_id", StringType()),
        StructField("ward_id", StringType()), StructField("bed_id", StringType()),
        StructField("record_time", StringType()), StructField("source", StringType()),
        StructField("bed_specs", StructType([
            StructField("bed_type", StringType()), StructField("daily_rate", DoubleType()),
            StructField("occupancy_status", StringType()), StructField("has_ventilator", BooleanType())])),
    ])

def obs_schema():
    """Alias kompatibilitas untuk modul yang memanggil obs_schema"""
    return bed_schema()


def wards_schema():
    """Skema master data kapasitas bangsal (CSV)"""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    return StructType([
        StructField("ward_id", StringType()), StructField("ward_name", StringType()),
        StructField("department", StringType()), StructField("total_beds", IntegerType()),
        StructField("bed_type", StringType()), StructField("daily_rate", DoubleType()),
        StructField("target_bor_pct", DoubleType()),
    ])


def dir_size(path):
    total = 0
    for dp, _, fs in os.walk(path):
        for f in fs:
            total += os.path.getsize(os.path.join(dp, f))
    return total if os.path.isdir(path) else os.path.getsize(path)


def save_json(name, obj):
    os.makedirs(RESULTS, exist_ok=True)
    with open(os.path.join(RESULTS, name + ".json"), "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=str)


def load_json(name):
    with open(os.path.join(RESULTS, name + ".json"), encoding="utf-8") as f:
        return json.load(f)
