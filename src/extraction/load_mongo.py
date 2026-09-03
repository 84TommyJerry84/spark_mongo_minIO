"""Insère les données dans une base mongo."""

import os

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()


def insert_data_to_mongodb(data, collection_name):
    """Insère les données dans MongoDB sans créer de doublons."""

    host = os.getenv("MONGO_HOST")
    port = os.getenv("MONGO_PORT")
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PASSWORD")
    database_name = os.getenv("MONGO_DATABASE")

    client = MongoClient(
        f"mongodb://{host}:{port}",
        username=username,
        password=password,
        authSource="admin",
        serverSelectionTimeoutMS=5000,
    )

    try:
        client.admin.command("ping")

        db = client[database_name]
        collection = db[collection_name]

        if not data:
            return 0

        unique_keys = {
            "velov_stations": ["idstation"],
            "velov_availabilities": ["station_id", "horodate"],
            "lyon_meteo": ["commune", "datetime"],
        }

        keys = unique_keys.get(collection_name)

        if not keys:
            result = collection.insert_many(data, ordered=False)
            return len(result.inserted_ids)

        operations = []

        for document in data:
            filtre = {key: document.get(key) for key in keys}

            if any(value is None for value in filtre.values()):
                raise ValueError(
                    f"Clé unique manquante pour {collection_name}: {filtre}"
                )

            operations.append(
                UpdateOne(
                    filtre,
                    {"$set": document},
                    upsert=True,
                )
            )

        result = collection.bulk_write(
            operations,
            ordered=False,
        )

        return result.upserted_count

    finally:
        client.close()


def get_last_date(collection_name):
    """Retourne la dernière date insérée."""
    host = os.getenv("MONGO_HOST")
    port = os.getenv("MONGO_PORT")
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PASSWORD")
    database_name = os.getenv("MONGO_DATABASE")

    client = MongoClient(
        f"mongodb://{host}:{port}",
        username=username,
        password=password,
        authSource="admin",
        serverSelectionTimeoutMS=5000,
    )

    try:
        client.admin.command("ping")

        db = client[database_name]
        collection = db[collection_name]

        last_document = collection.find_one(
            {},
            {"horodate": 1, "_id": 0},
            sort=[("horodate", -1)],
        )

        horodate = last_document["horodate"].split(" ")[0] if last_document else None

        return horodate

    finally:
        client.close()
