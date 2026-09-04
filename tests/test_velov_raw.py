"""Test de lecture du référentiel Vélo'v Raw depuis MinIO."""

from pyspark.sql import functions as F

from src.config.settings import RAW_STATIONS_PATH
from src.utils.spark_session import create_spark_session

spark = create_spark_session("TestVelovRaw")


raw = spark.read.json(RAW_STATIONS_PATH)

stations = raw.select(F.explode("values").alias("station")).select("station.*")


print("NB STATIONS =", stations.count())

stations.select(
    "idstation",
    "nom",
    "commune",
).show(10, truncate=False)

spark.stop()
