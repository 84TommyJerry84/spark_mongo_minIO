"""Collection des données Velov."""

import requests


def get_velov_stations(url):
    """Cette fonction récupère les stations Velov."""
    maxfeatures = 100
    start = 1

    stations = []
    raw_pages = []

    while True:
        params = {
            "maxfeatures": maxfeatures,
            "start": start,
        }

        response = requests.get(
            url,
            params=params,
            timeout=120,
        )
        response.raise_for_status()

        data = response.json()

        # On conserve la réponse brute de cette page
        raw_pages.append(data)

        values = data.get("values", [])

        if not values:
            break

        stations.extend(values)

        start += len(values)

    return stations, raw_pages


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
            "raw": item,
        }

        availabilities.append(availability)

    return availabilities
