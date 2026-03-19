import logging
import os

import requests

logger = logging.getLogger(__name__)


def get_distance_from_google(
    origin_city: str, origin_uf: str, dest_city: str, dest_uf: str
) -> float | None:
    """Query Google Distance Matrix API and return distance in km, or None on failure."""
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        logger.error("GOOGLE_API_KEY not configured")
        return None

    origin_city = str(origin_city).strip() if origin_city else ""
    origin_uf = str(origin_uf).strip() if origin_uf else ""
    dest_city = str(dest_city).strip() if dest_city else ""
    dest_uf = str(dest_uf).strip() if dest_uf else ""

    if not all([origin_city, origin_uf, dest_city, dest_uf]):
        return None

    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": f"{origin_city},{origin_uf},BR",
        "destinations": f"{dest_city},{dest_uf},BR",
        "key": api_key,
        "language": "pt-BR",
        "units": "metric",
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.ok:
            data = resp.json()
            if data.get("status") == "OK":
                element = data["rows"][0]["elements"][0]
                if element.get("status") == "OK":
                    distance_km = element["distance"]["value"] / 1000
                    return round(distance_km, 1)
    except Exception as e:
        logger.error("Google API error: %s", e)

    return None
