import os

from pyspark.sql import SparkSession


mongo_username = os.environ["MONGO_USERNAME"]
mongo_password = os.environ["MONGO_PASSWORD"]

mongo_uri = (
    f"mongodb://{mongo_username}:{mongo_password}"
    "@mongodb:27017/datalake"
    "?authSource=admin"
)


spark = (
    SparkSession.builder.appName("TestMongoDB")
    .config("spark.mongodb.read.connection.uri", mongo_uri)
    .getOrCreate()
)


print("=== LECTURE MONGODB ===")

df = (
    spark.read.format("mongodb")
    .option("database", "datalake")
    .option("collection", "stations")
    .load()
)


print("=== SCHEMA ===")
df.printSchema()


print("=== DONNEES ===")
df.show(truncate=False)


spark.stop()
