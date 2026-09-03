import os
from urllib.parse import quote_plus

from pyspark.sql import SparkSession

mongo_username = quote_plus(os.environ["MONGO_USERNAME"])
mongo_password = quote_plus(os.environ["MONGO_PASSWORD"])

mongo_uri = (
    f"mongodb://{mongo_username}:{mongo_password}@mongodb:27017/?authSource=admin"
)


spark = SparkSession.builder.appName("TestRealMongoDB").getOrCreate()


stations = (
    spark.read.format("mongodb")
    .option("connection.uri", mongo_uri)
    .option("database", "velov_weather")
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
