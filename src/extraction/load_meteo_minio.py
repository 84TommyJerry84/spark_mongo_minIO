"""Stockage des données météo brutes dans MinIO."""

import os
from datetime import datetime, timezone

import boto3

from src.config.settings import MINIO_ENDPOINT, MINIO_RAW_BUCKET


def upload_meteo_raw(
    contenu_csv,
    commune,
    start_date,
    end_date,
):
    """Stocke une réponse CSV brute Open-Meteo dans MinIO Raw."""

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )

    commune_safe = commune.replace(" ", "_").replace("/", "-")

    ingestion_time = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")

    object_key = (
        f"meteo/"
        f"commune={commune_safe}/"
        f"start={start_date}/"
        f"end={end_date}/"
        f"ingested_at={ingestion_time}.csv"
    )

    s3.put_object(
        Bucket=MINIO_RAW_BUCKET,
        Key=object_key,
        Body=contenu_csv.encode("utf-8"),
        ContentType="text/csv; charset=utf-8",
    )

    return object_key
