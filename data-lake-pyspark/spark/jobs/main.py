from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


spark = SparkSession.builder.appName("PremierJobPySpark").getOrCreate()

schema = StructType(
    [
        StructField("station", StringType(), True),
        StructField("velos_disponibles", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
    ]
)


data = [
    ("Bellecour", 12, 22.5),
    ("Part-Dieu", 4, 18.2),
    ("Confluence", 8, 25.1),
    ("Perrache", 2, 17.8),
]

columns = [
    "station",
    "velos_disponibles",
    "temperature",
]

# avant l'application du schema
# df = spark.createDataFrame(data, columns)
df = spark.createDataFrame(
    data,
    schema=schema,
)


df_filtre = df.filter(F.col("temperature") > 20)


df_filtre.show()


spark.stop()
