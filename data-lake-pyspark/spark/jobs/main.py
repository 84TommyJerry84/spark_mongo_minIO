from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ajout de l'import pour le schema
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.appName("PremierJobPySpark").getOrCreate()

data = [
    ("Bellecour", "12", "22.5", "2026-09-02 08:15:00"),
    ("Part-Dieu", "4", "18.2", "2026-09-02 09:30:00"),
    ("Confluence", "8", "25.1", "2026-09-02 14:45:00"),
    ("Perrache", "2", "17.8", "2026-09-03 07:10:00"),
]

# ajout du schema
schema = StructType(
    [
        StructField("station", StringType(), True),
        StructField("velos_disponibles", StringType(), True),
        StructField("temperature", StringType(), True),
        StructField("date_mesure", StringType(), True),
    ]
)


columns = [
    "station",
    "velos_disponibles",
    "temperature",
]

# avant l'application du schema
# df = spark.createDataFrame(data, columns)
# nouvelle création du df avec le schema
df = spark.createDataFrame(
    data,
    schema=schema,
)

print("SCHEMA AVANT CAST")
df.printSchema()

# df_filtre = df.filter(F.col("temperature") > 20)


df = (
    df.withColumn("velos_disponibles", F.col("velos_disponibles").cast("integer"))
    .withColumn("temperature", F.col("temperature").cast("double"))
    .withColumn(
        "date_mesure", F.to_timestamp(F.col("date_mesure"), "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn("annee", F.year("date_mesure"))
    .withColumn("mois", F.month("date_mesure"))
    .withColumn("jour", F.dayofmonth("date_mesure"))
    .withColumn("heure", F.hour("date_mesure"))
    .withColumn("jour_semaine", F.dayofweek("date_mesure"))
)

print("SCHEMA APRES CAST")
df.printSchema()

df_filtre = df.filter(F.col("temperature") > 20)

df_filtre.show()

spark.stop()
