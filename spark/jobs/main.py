import os
from urllib.parse import quote_plus

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --------------------------------------------------
# 1. Variables d'environnement
# --------------------------------------------------

minio_access_key = os.environ["MINIO_ACCESS_KEY"]
minio_secret_key = os.environ["MINIO_SECRET_KEY"]

mongo_username = quote_plus(os.environ["MONGO_USERNAME"])
mongo_password = quote_plus(os.environ["MONGO_PASSWORD"])

mongo_database = os.environ["MONGO_DATABASE"]


# --------------------------------------------------
# 2. URI MongoDB
# --------------------------------------------------

mongo_uri = f"mongodb://{mongo_username}:{mongo_password}@mongodb:27017/?authSource=admin"


# --------------------------------------------------
# 3. SparkSession
# --------------------------------------------------

spark = (
    SparkSession.builder.appName("DataLakePipeline")
    # Les timestamps seront manipulés en UTC
    .config("spark.sql.session.timeZone", "UTC")
    # MinIO / S3A
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false",
    )
    # MongoDB
    .config(
        "spark.mongodb.read.connection.uri",
        mongo_uri,
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 4. Lecture des vraies collections MongoDB
# --------------------------------------------------

# stations = (
#     spark.read.format("mongodb")
#     .option("database", mongo_database)
#     .option("collection", "velov_stations")
#     .load()
# )

# Stations Vélo'v depuis MinIO Raw
raw_stations = spark.read.json("s3a://raw/velov/stations/current/stations.json")

stations = raw_stations.select(F.explode("values").alias("station")).select("station.*")

availabilities = (
    spark.read.format("mongodb")
    .option("database", mongo_database)
    .option("collection", "velov_availabilities")
    .load()
)

meteo = (
    spark.read.format("mongodb")
    .option("database", mongo_database)
    .option("collection", "lyon_meteo")
    .load()
)


# --------------------------------------------------
# 5. Typage des disponibilités Vélo'v
# --------------------------------------------------

availabilities = availabilities.withColumn(
    "horodate_ts",
    F.to_timestamp(
        F.col("horodate"),
        "yyyy-MM-dd HH:mm:ssXXX",
    ),
)

availabilities = availabilities.dropDuplicates(["station_id", "horodate"])
# --------------------------------------------------
# 6. Vérification
# --------------------------------------------------

# print("=== CONVERSION HORODATE ===")

# availabilities.select(
#     "horodate",
#     "horodate_ts",
#     "station_id",
# ).show(10, truncate=False)

# availabilities.printSchema()

# --------------------------------------------------
# 7. jointure
# --------------------------------------------------

df_velov = (
    availabilities.alias("a")
    .join(
        stations.alias("s"),
        F.col("a.station_id") == F.col("s.idstation"),
        "left",
    )
    .select(
        F.col("a.station_id"),
        F.col("a.horodate"),
        F.col("a.horodate_ts"),
        F.col("a.status"),
        F.col("a.capacity"),
        F.col("a.bikes_available"),
        F.col("a.stands_available"),
        F.col("s.nom").alias("station_nom"),
        F.col("s.commune"),
        F.col("s.lat"),
        F.col("s.lon"),
        F.col("s.nbbornettes"),
    )
)

# anomalie de la commune => Saint Fons - Rochette / Crest
df_velov = df_velov.withColumn(
    "commune",
    F.when(
        (F.col("station_id") == 17006) & F.col("commune").isNull(),
        F.lit("Saint-Fons"),
    ).otherwise(F.col("commune")),
)

# print("=== JOINTURE DISPONIBILITES + STATIONS ===")

# df_velov.show(10, truncate=False)

# print("NB LIGNES =", df_velov.count())

# print(
#     "STATIONS NON TROUVEES =",
#     df_velov.filter(F.col("station_nom").isNull()).count(),
# )


# --------------------------------------------------
# 8. separtion des observations sans station
# --------------------------------------------------

df_velov_ok = df_velov.filter(F.col("station_nom").isNotNull())

df_velov_orphelines = df_velov.filter(F.col("station_nom").isNull())

print("=== QUALITE JOINTURE STATIONS ===")

print(
    "LIGNES EXPLOITABLES =",
    df_velov_ok.count(),
)

print(
    "LIGNES ORPHELINES =",
    df_velov_orphelines.count(),
)

# --------------------------------------------------
# 9. les stations manquantes aprés la jointures
# --------------------------------------------------

# df_stations_manquantes = df_velov.filter(F.col("station_nom").isNull())

# print("=== STATIONS NON RATTACHEES ===")

# print(
#     "NB STATION_ID DISTINCTS =",
#     df_stations_manquantes.select("station_id").distinct().count(),
# )

# df_stations_manquantes.groupBy("station_id").agg(
#     F.count("*").alias("nb_observations"),
#     F.min("horodate_ts").alias("premiere_observation"),
#     F.max("horodate_ts").alias("derniere_observation"),
# ).orderBy(F.desc("nb_observations")).show(50, truncate=False)

# --------------------------------------------------
# 10. commune/météo
# --------------------------------------------------
print("=== VERIFICATION COMMUNES METEO ===")

communes_velov = df_velov_ok.select("commune").distinct()

communes_meteo = meteo.select("commune").distinct()

communes_sans_meteo = communes_velov.join(
    communes_meteo,
    on="commune",
    how="left_anti",
)

print(
    "NB COMMUNES VELOV =",
    communes_velov.count(),
)

print(
    "NB COMMUNES SANS METEO =",
    communes_sans_meteo.count(),
)

communes_sans_meteo.show(50, truncate=False)

# --------------------------------------------------
# 11. les communes sans météo
# --------------------------------------------------
df_commune_null = df_velov_ok.filter(F.col("commune").isNull())

print("=== STATIONS SANS COMMUNE ===")

print(
    "NB OBSERVATIONS =",
    df_commune_null.count(),
)

print(
    "NB STATIONS DISTINCTES =",
    df_commune_null.select("station_id").distinct().count(),
)

df_commune_null.select(
    "station_id",
    "station_nom",
    "lat",
    "lon",
).distinct().show(20, truncate=False)
# --------------------------------------------------
# 12. Alignement des créneau (15min)
# --------------------------------------------------

df_velov_ok = df_velov_ok.withColumn(
    "creneau_15min",
    F.from_unixtime(F.floor(F.unix_timestamp("horodate_ts") / 900) * 900).cast("timestamp"),
)

print("=== ALIGNEMENT TEMPOREL ===")

df_velov_ok.select(
    "horodate_ts",
    "creneau_15min",
    "commune",
).show(15, truncate=False)

# --------------------------------------------------
# 14. real jointure
# --------------------------------------------------
df_final = (
    df_velov_ok.alias("v")
    .join(
        meteo.alias("m"),
        (
            (F.col("v.commune") == F.col("m.commune"))
            & (F.col("v.creneau_15min") == F.col("m.datetime"))
        ),
        "left",
    )
    .select(
        # Vélo'v
        F.col("v.station_id"),
        F.col("v.station_nom"),
        F.col("v.commune"),
        F.col("v.lat"),
        F.col("v.lon"),
        F.col("v.horodate_ts"),
        F.col("v.creneau_15min"),
        F.col("v.status"),
        F.col("v.capacity"),
        F.col("v.bikes_available"),
        F.col("v.stands_available"),
        # Météo
        F.col("m.temperature_2m_c"),
        F.col("m.apparent_temperature_c"),
        F.col("m.relative_humidity_2m_pct"),
        F.col("m.precipitation_mm"),
        F.col("m.rain_mm"),
        F.col("m.wind_speed_10m_kmh"),
        F.col("m.weather_code"),
    )
)

print("=== JOINTURE VELOV + METEO ===")

print(
    "NB LIGNES APRES JOINTURE =",
    df_final.count(),
)

print(
    "NB LIGNES SANS METEO =",
    df_final.filter(F.col("temperature_2m_c").isNull()).count(),
)

df_final.select(
    "station_id",
    "station_nom",
    "commune",
    "horodate_ts",
    "creneau_15min",
    "bikes_available",
    "temperature_2m_c",
    "rain_mm",
    "wind_speed_10m_kmh",
).show(15, truncate=False)

# --------------------------------------------------
# Contrôle de cohérence capacité / vélos
# --------------------------------------------------

df_anomalies_capacite = df_final.filter(
    F.col("capacity").isNull()
    | F.col("bikes_available").isNull()
    | (F.col("capacity") <= 0)
    | (F.col("bikes_available") < 0)
    | (F.col("bikes_available") > F.col("capacity"))
)

print(
    "ANOMALIES CAPACITE / VELOS =",
    df_anomalies_capacite.count(),
)

df_final = df_final.filter(
    F.col("capacity").isNotNull()
    & F.col("bikes_available").isNotNull()
    & (F.col("capacity") > 0)
    & (F.col("bikes_available") >= 0)
    & (F.col("bikes_available") <= F.col("capacity"))
)

# --------------------------------------------------
# 14. indicateur métier
# --------------------------------------------------

df_final = df_final.withColumn(
    "taux_velos_disponibles",
    F.when(
        F.col("capacity") > 0,
        F.round(
            F.col("bikes_available") / F.col("capacity") * 100,
            2,
        ),
    ),
)

print("=== INDICATEUR DISPONIBILITE ===")

df_final.select(
    "station_nom",
    "capacity",
    "bikes_available",
    "stands_available",
    "taux_velos_disponibles",
).show(15, truncate=False)

# --------------------------------------------------
# 15. temporelle
# --------------------------------------------------

df_final = df_final.withColumn(
    "horodate_local",
    F.from_utc_timestamp(
        F.col("horodate_ts"),
        "Europe/Paris",
    ),
)

df_final = (
    df_final.withColumn("annee", F.year("horodate_local"))
    .withColumn("mois", F.month("horodate_local"))
    .withColumn("jour", F.dayofmonth("horodate_local"))
    .withColumn("heure", F.hour("horodate_local"))
)

print("=== COLONNES TEMPORELLES ===")

df_final.select(
    "horodate_ts",
    "horodate_local",
    "annee",
    "mois",
    "jour",
    "heure",
).show(15, truncate=False)

# --------------------------------------------------
# 19. Contrôles qualité finaux
# --------------------------------------------------

print("=== QUALITY GATE FINAL ===")

quality = df_final.agg(
    F.count("*").alias("nb_lignes"),
    F.sum(F.when(F.col("horodate_ts").isNull(), 1).otherwise(0)).alias("horodate_null"),
    F.sum(F.when(F.col("station_nom").isNull(), 1).otherwise(0)).alias("station_null"),
    F.sum(F.when(F.col("commune").isNull(), 1).otherwise(0)).alias("commune_null"),
    F.sum(F.when(F.col("capacity") <= 0, 1).otherwise(0)).alias("capacity_non_positive"),
    F.sum(F.when(F.col("bikes_available") < 0, 1).otherwise(0)).alias("bikes_negatifs"),
    F.sum(F.when(F.col("temperature_2m_c").isNull(), 1).otherwise(0)).alias("meteo_null"),
    F.sum(
        F.when(
            (F.col("taux_velos_disponibles") < 0) | (F.col("taux_velos_disponibles") > 100),
            1,
        ).otherwise(0)
    ).alias("taux_hors_limites"),
)

quality.show(truncate=False)

print("=== ANOMALIES TAUX ===")

df_final.filter(
    (F.col("taux_velos_disponibles") < 0) | (F.col("taux_velos_disponibles") > 100)
).select(
    "station_id",
    "station_nom",
    "horodate_ts",
    "capacity",
    "bikes_available",
    "stands_available",
    "taux_velos_disponibles",
).show(truncate=False)

# --------------------------------------------------
# 17. frequentation
# --------------------------------------------------
df_station_heure = df_final.groupBy(
    "station_id",
    "station_nom",
    "commune",
    "annee",
    "mois",
    "jour",
    "heure",
).agg(
    F.count("*").alias("nb_observations"),
    F.round(
        F.avg("bikes_available"),
        2,
    ).alias("moy_velos_disponibles"),
    F.round(
        F.avg("stands_available"),
        2,
    ).alias("moy_places_libres"),
    F.round(
        F.avg("taux_velos_disponibles"),
        2,
    ).alias("moy_taux_velos_disponibles"),
    F.round(
        F.avg("temperature_2m_c"),
        2,
    ).alias("temperature_moyenne_c"),
)

print("=== AGREGATION STATION / HEURE ===")

df_station_heure.orderBy(
    "station_id",
    "annee",
    "mois",
    "jour",
    "heure",
).show(20, truncate=False)


# ---------------------------------------------------
print("=== ECRITURE AGREGATION HORAIRE ===")

(
    df_station_heure.write.mode("overwrite")
    .partitionBy("annee", "mois")
    .parquet("s3a://analytics/velov_meteo_station_heure")
)

print("ECRITURE AGREGATION TERMINEE")

df_agreg_verification = spark.read.parquet("s3a://analytics/velov_meteo_station_heure")

print("=== VERIFICATION AGREGATION ===")

print(
    "NB LIGNES AGREGATION =",
    df_agreg_verification.count(),
)

df_agreg_verification.orderBy(
    "station_id",
    "annee",
    "mois",
    "jour",
    "heure",
).show(10, truncate=False)

# ----------------------------------------------

print("=== ECRITURE PARQUET ANALYTICS ===")

(
    df_final.write.mode("overwrite")
    .partitionBy("annee", "mois")
    .parquet("s3a://analytics/velov_meteo")
)

print("ECRITURE TERMINEE")

df_verification = spark.read.parquet("s3a://analytics/velov_meteo")

print("=== VERIFICATION PARQUET ===")

print(
    "NB LIGNES PARQUET =",
    df_verification.count(),
)

df_verification.select(
    "station_id",
    "station_nom",
    "commune",
    "horodate_local",
    "bikes_available",
    "temperature_2m_c",
    "taux_velos_disponibles",
    "annee",
    "mois",
).show(10, truncate=False)

# --------------------------------------------------
# 18. Arrêt
# --------------------------------------------------

spark.stop()
