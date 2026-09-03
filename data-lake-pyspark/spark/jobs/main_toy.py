import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

df_final = (
    df_minio.join(df_mongo, on="station", how="inner")
    # Typage explicite
    .withColumn("velos", F.col("velos").cast("integer"))
    .withColumn("capacite", F.col("capacite").cast("integer"))
    .withColumn("temperature", F.col("temperature").cast("double"))
    # Nombre de places libres
    .withColumn("places_libres", F.col("capacite") - F.col("velos"))
    # Pourcentage de vélos disponibles
    .withColumn(
        "taux_disponibilite", F.round((F.col("velos") / F.col("capacite")) * 100, 1)
    )
)

df_final = df_final.withColumn(
    "niveau_disponibilite",
    F.when(F.col("taux_disponibilite") < 30, "faible").otherwise("correcte"),
)

df_final.show()

print("=== AGREGATIONS ===")

df_stats = df_final.agg(
    F.avg("temperature").alias("temperature_moyenne"),
    F.sum("velos").alias("total_velos"),
    F.sum("capacite").alias("capacite_totale"),
    F.avg("taux_disponibilite").alias("taux_moyen"),
)

df_stats.show()

print("=== STATS PAR NIVEAU DE DISPONIBILITE ===")

df_par_niveau = df_final.groupBy("niveau_disponibilite").agg(
    F.count("*").alias("nombre_stations"),
    F.avg("velos").alias("moyenne_velos"),
    F.avg("temperature").alias("temperature_moyenne"),
)

df_par_niveau.show()

# --------------------------------------------------
# 7. Ecriture dans MinIO Analytics
# --------------------------------------------------

print("=== ECRITURE PARQUET DANS MINIO ANALYTICS ===")

df_final.write.mode("overwrite").parquet("s3a://analytics/stations")


print("=== RELECTURE PARQUET ANALYTICS ===")

df_check = spark.read.parquet("s3a://analytics/stations")

df_check.show()
df_check.printSchema()


# --------------------------------------------------
# 8. Arrêt de Spark
# --------------------------------------------------

spark.stop()
