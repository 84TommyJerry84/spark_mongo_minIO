import os

from pyspark.sql import SparkSession


# --------------------------------------------------
# 1. Récupération des variables d'environnement
# --------------------------------------------------

minio_access_key = os.environ["MINIO_ACCESS_KEY"]
minio_secret_key = os.environ["MINIO_SECRET_KEY"]

mongo_username = os.environ["MONGO_USERNAME"]
mongo_password = os.environ["MONGO_PASSWORD"]


# --------------------------------------------------
# 2. URI MongoDB
# --------------------------------------------------

mongo_uri = (
    f"mongodb://{mongo_username}:{mongo_password}"
    "@mongodb:27017/datalake"
    "?authSource=admin"
)


# --------------------------------------------------
# 3. Création de la SparkSession
# --------------------------------------------------

spark = (
    SparkSession.builder.appName("DataLakePipeline")
    # Configuration MinIO / S3A
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # Configuration MongoDB
    .config("spark.mongodb.read.connection.uri", mongo_uri)
    .getOrCreate()
)


# --------------------------------------------------
# 4. Lecture des données MinIO
# --------------------------------------------------

print("=== LECTURE MINIO ===")

print("=== CONTENU BRUT DU FICHIER MINIO ===")

df_debug = spark.read.text("s3a://raw/test/test_minio.json")

df_debug.show(100, truncate=False)


df_minio = spark.read.option("multiLine", "true").json("s3a://raw/test/test_minio.json")

df_minio.show()

# --------------------------------------------------
# 5. Lecture des données MongoDB
# --------------------------------------------------

print("=== LECTURE MONGODB ===")

df_mongo = (
    spark.read.format("mongodb")
    .option("database", "datalake")
    .option("collection", "stations")
    .load()
    .select("station", "capacite")
)

df_mongo.show()


# --------------------------------------------------
# 6. Jointure des deux sources
# --------------------------------------------------

print("=== JOINTURE MINIO + MONGODB ===")

df_final = df_minio.join(df_mongo, on="station", how="inner")

df_final.show()


# --------------------------------------------------
# 7. Arrêt de Spark
# --------------------------------------------------

spark.stop()
