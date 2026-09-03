"""Collection des données Velov."""

import requests


def get_velov_stations(url):
    """Cette fonction récupère les stations Velov."""
    maxfeatures = 100
    start = 1

    stations = []

    while True:
        params = {
            "maxfeatures": maxfeatures,
            "start": start,
        }

        response = requests.get(
            url,
            params=params,
        )

        response.raise_for_status()

        data = response.json()
        values = data.get("values", [])

        # Plus de données -> on arrête
        if not values:
            break

        stations.extend(values)

        # On avance de la taille réellement récupérée
        start += len(values)

    return stations


def get_velov_availabilities(
    url,
    maxfeatures,
    start,
    debut,
    fin,
):
    """Récupère les données Velov et conserve uniquement les informations utiles."""

    params = {
        "maxfeatures": maxfeatures,
        "start": start,
        "horodate__gte": debut,
        "horodate__lte": fin,
    }

    response = requests.get(
        url,
        params=params,
        timeout=300,
    )

    response.raise_for_status()

    data = response.json()
    values = data.get("values", [])

    availabilities = []

    for item in values:
        main_stands = item.get("main_stands", {})
        availabilities_data = main_stands.get(
            "availabilities",
            {},
        )

        availability = {
            "horodate": item.get("horodate"),
            "station_id": item.get("number"),
            "status": item.get("status"),
            "capacity": main_stands.get("capacity"),
            "bikes_available": availabilities_data.get("bikes"),
            "stands_available": availabilities_data.get("stands"),
        }

        availabilities.append(availability)

    return availabilities
