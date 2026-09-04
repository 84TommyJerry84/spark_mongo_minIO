"""Stockage des données Vélo'v brutes dans MinIO."""

import json
import os
from datetime import datetime, timezone

import boto3

from src.config.settings import MINIO_ENDPOINT, MINIO_RAW_BUCKET


def upload_velov_stations_raw(raw_pages):
    """Stocke les réponses JSON brutes des stations Vélo'v dans MinIO Raw."""

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )

    ingestion_time = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")

    object_key = f"velov/stations/ingested_at={ingestion_time}/stations.json"

    contenu_json = "\n".join(
        json.dumps(page, ensure_ascii=False) for page in raw_pages if page.get("values")
    )

    # Archive historisée
    s3.put_object(
        Bucket=MINIO_RAW_BUCKET,
        Key=object_key,
        Body=contenu_json.encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )

    # Dernière version utilisée par Spark
    current_key = "velov/stations/current/stations.json"

    s3.put_object(
        Bucket=MINIO_RAW_BUCKET,
        Key=current_key,
        Body=contenu_json.encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )

    return object_key
