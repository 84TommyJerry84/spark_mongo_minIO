"""Test de lecture MongoDB depuis Spark."""

from src.config.settings import MONGO_DATABASE, MONGO_URI
from src.utils.spark_session import create_spark_session

spark = create_spark_session("TestRealMongoDB")


stations = (
    spark.read.format("mongodb")
    .option("connection.uri", MONGO_URI)
    .option("database", MONGO_DATABASE)
    .option("collection", "velov_stations")
    .load()
)


print("====================================")
print("NB STATIONS =", stations.count())
print("====================================")

stations.select(
    "idstation",
    "nom",
    "commune",
    "lat",
    "lon",
    "nbbornettes",
).show(5, truncate=False)

stations.printSchema()

spark.stop()
