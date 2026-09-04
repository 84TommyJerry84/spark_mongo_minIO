"""Configuration du projet à partir des variables d'environnement."""

import os
from urllib.parse import quote_plus

# ============================================================
# MongoDB
# ============================================================

MONGO_HOST = os.environ["MONGO_HOST"]
MONGO_PORT = os.environ["MONGO_PORT"]
MONGO_USERNAME = os.environ["MONGO_USERNAME"]
MONGO_PASSWORD = os.environ["MONGO_PASSWORD"]
MONGO_DATABASE = os.environ["MONGO_DATABASE"]

MONGO_URI = (
    f"mongodb://{quote_plus(MONGO_USERNAME)}:"
    f"{quote_plus(MONGO_PASSWORD)}@"
    f"{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
)


# ============================================================
# MinIO
# ============================================================

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]

MINIO_RAW_BUCKET = os.environ["MINIO_RAW_BUCKET"]
MINIO_ANALYTICS_BUCKET = os.environ["MINIO_ANALYTICS_BUCKET"]


# ============================================================
# Chemins Data Lake
# ============================================================

RAW_STATIONS_PATH = f"s3a://{MINIO_RAW_BUCKET}/velov/stations/current/stations.json"

ANALYTICS_VELOV_METEO_PATH = f"s3a://{MINIO_ANALYTICS_BUCKET}/velov_meteo"

ANALYTICS_STATION_HEURE_PATH = f"s3a://{MINIO_ANALYTICS_BUCKET}/velov_meteo_station_heure"


# ============================================================
# APIs
# ============================================================

VELOV_STATIONS_URL = os.environ["VELOV_STATIONS_URL"]
VELOV_AVAILABILITIES_URL = os.environ["VELOV_AVAILABILITIES_URL"]
OPEN_METEO_URL = os.environ["OPEN_METEO_URL"]


# ============================================================
# Temps
# ============================================================

TIMEZONE = os.environ["TIMEZONE"]
