"""
Configuration commune des SparkSession du projet SahlPay.

Dans Docker (cas normal) : Driver dans le conteneur Jupyter, Master spark://spark-master:7077,
lac de donnees dans MinIO (s3a://lake).
En local (tests sans Docker) : export SAHLPAY_LOCAL=1 -> master local[*] et lac dans ./lake_local.
"""
import os
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession

LOCAL = os.environ.get("SAHLPAY_LOCAL") == "1"
LAKE = os.environ.get("SAHLPAY_LAKE", os.path.abspath("lake_local") if LOCAL else "s3a://lake")
MASTER = "local[*]" if LOCAL else os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")


def get_spark(app_name: str) -> SparkSession:
    builder = (SparkSession.builder
               .appName(app_name)
               .master(MASTER)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "8"))      # 4 coeurs dans le cluster -> 8 partitions suffisent
    if not LOCAL:
        builder = (builder
                   # le Driver tourne dans le conteneur jupyter : les Executors doivent pouvoir le joindre
                   .config("spark.driver.host", "jupyter")
                   .config("spark.driver.bindAddress", "0.0.0.0")
                   .config("spark.executor.memory", "1g")
                   .config("spark.executor.cores", "2")
                   # acces S3A a MinIO
                   .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000"))
                   .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin"))
                   .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"))
                   .config("spark.hadoop.fs.s3a.path.style.access", "true")
                   .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                   .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                   .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                           "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if not LOCAL:
        # Les Executors démarrent sur les Workers quelques secondes après la création de la session.
        # On attend qu'ils soient enregistrés, sinon defaultParallelism vaut 2 au lieu de 4.
        attendus = int(os.environ.get("SAHLPAY_EXPECTED_EXECUTORS", "2"))
        statut = spark.sparkContext._jsc.sc()
        for _ in range(30):
            if statut.getExecutorMemoryStatus().size() - 1 >= attendus:   # -1 : on retire le Driver
                break
            time.sleep(1)
    return spark


@contextmanager
def chrono(label: str, results: dict = None):
    """Mesure le temps d'un bloc : with chrono("RDD total par ville", temps): ..."""
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    if results is not None:
        results[label] = round(elapsed, 3)
    print(f"[temps] {label} : {elapsed:.3f} s")


def list_lake(spark, path: str = None, depth: int = 3):
    """Affiche l'arborescence du lac (equivalent de mc ls --recursive, depuis Spark)."""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    root = jvm.org.apache.hadoop.fs.Path(path or LAKE)
    fs = root.getFileSystem(conf)

    def walk(p, level):
        for st in fs.listStatus(p):
            name = st.getPath().getName()
            kind = "/" if st.isDirectory() else f"  ({st.getLen()} octets)"
            print("  " * level + name + kind)
            if st.isDirectory() and level < depth:
                walk(st.getPath(), level + 1)

    print(str(root))
    walk(root, 1)


def mediane(fn, repetitions: int = 3, label: str = None, results: dict = None) -> float:
    """Execute fn() plusieurs fois et renvoie la mediane du temps (plus stable qu'une seule mesure)."""
    durees = []
    for _ in range(repetitions):
        start = time.perf_counter()
        fn()
        durees.append(time.perf_counter() - start)
    m = round(sorted(durees)[len(durees) // 2], 3)
    if label:
        if results is not None:
            results[label] = m
        print(f"[temps] {label} : mediane {m:.3f} s sur {repetitions} executions")
    return m
