"""Collecter la data du meteo à Lyon et les communes voisines."""

import csv
import io
from datetime import datetime
from zoneinfo import ZoneInfo

import requests


def collect_meteo(commune, coordonnees, start_date, end_date):
    """Collecte les données météo pour Lyon et les communes voisines."""

    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"

    variables_meteo = (
        "temperature_2m,"
        "relative_humidity_2m,"
        "apparent_temperature,"
        "precipitation,"
        "rain,"
        "snowfall,"
        "weather_code,"
        "wind_speed_10m,"
        "wind_gusts_10m,"
        "is_day,"
        "visibility"
    )

    params = {
        "latitude": f"{coordonnees[0]}",
        "longitude": f"{coordonnees[1]}",
        "start_date": start_date,
        "end_date": end_date,
        "minutely_15": variables_meteo,
        "timezone": "Europe/Paris",
        "format": "csv",
    }

    response = requests.get(
        url,
        params=params,
        timeout=120,
    )

    response.raise_for_status()

    contenu_csv = response.text
    lignes = contenu_csv.splitlines()

    debut_donnees = None

    for index, ligne in enumerate(lignes):
        if ligne.startswith("time,"):
            debut_donnees = index
            break

    if debut_donnees is None:
        raise ValueError("En-tête météo introuvable dans la réponse CSV.")

    csv_meteo = "\n".join(lignes[debut_donnees:])

    reader = csv.DictReader(io.StringIO(csv_meteo))

    documents = []

    for ligne in reader:
        document = {
            "commune": commune,
            "location": {
                "type": "Point",
                "coordinates": [
                    coordonnees[1],
                    coordonnees[0],
                ],
            },
            "datetime": datetime.fromisoformat(ligne["time"]).replace(
                tzinfo=ZoneInfo("Europe/Paris")
            ),
            "temperature_2m_c": float(ligne["temperature_2m (°C)"]),
            "relative_humidity_2m_pct": int(ligne["relative_humidity_2m (%)"]),
            "apparent_temperature_c": float(ligne["apparent_temperature (°C)"]),
            "precipitation_mm": float(ligne["precipitation (mm)"]),
            "rain_mm": float(ligne["rain (mm)"]),
            "snowfall_cm": float(ligne["snowfall (cm)"]),
            "weather_code": int(ligne["weather_code (wmo code)"]),
            "wind_speed_10m_kmh": float(ligne["wind_speed_10m (km/h)"]),
            "wind_gusts_10m_kmh": float(ligne["wind_gusts_10m (km/h)"]),
            "is_day": bool(int(ligne["is_day ()"])),
            "visibility_m": float(ligne["visibility (m)"]),
        }

        documents.append(document)

    return documents, contenu_csv
