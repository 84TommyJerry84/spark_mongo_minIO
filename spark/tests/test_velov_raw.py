"""Test de lecture du référentiel Vélo'v Raw depuis MinIO."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

minio_access_key = os.environ["MINIO_ACCESS_KEY"]
minio_secret_key = os.environ["MINIO_SECRET_KEY"]


spark = (
    SparkSession.builder.appName("TestVelovRaw")
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://minio:9000",
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        minio_access_key,
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        minio_secret_key,
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true",
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false",
    )
    .getOrCreate()
)

raw = spark.read.json("s3a://raw/velov/stations/current/stations.json")

stations = raw.select(F.explode("values").alias("station")).select("station.*")

print("NB STATIONS =", stations.count())

stations.select(
    "idstation",
    "nom",
    "commune",
).show(10, truncate=False)

spark.stop()
