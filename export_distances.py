import csv
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from unicodedata import normalize

import requests
from dotenv import load_dotenv

from googletools import get_distance_from_google

logger = logging.getLogger(__name__)

IBGE_MUNICIPIOS_URL = (
    "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
)

INPUT_CSV_PATH = "distancias.csv"
OUTPUT_CSV_PATH = "distancias_export.csv"

OUTPUT_HEADER = [
    "cidade_origem",
    "uf_origem",
    "cidade_destino",
    "uf_destino",
    "distancia_km",
    "fonte",
]


@dataclass(frozen=True)
class RouteSpec:
    origin_uf: str
    destination_city: str
    destination_uf: str


DEFAULT_ROUTE_SPECS: tuple[RouteSpec, ...] = (
    RouteSpec(origin_uf="MG", destination_city="BETIM", destination_uf="MG"),
    RouteSpec(origin_uf="MG", destination_city="IGARAPE", destination_uf="MG"),
    RouteSpec(origin_uf="RJ", destination_city="RIO DE JANEIRO", destination_uf="RJ"),
    RouteSpec(origin_uf="SP", destination_city="SAO PAULO", destination_uf="SP"),
)


def normalize_city_name(p_city: str) -> str:
    city = str(p_city or "").strip().upper()
    city = normalize("NFKD", city).encode("ascii", "ignore").decode("ascii")
    city = " ".join(city.split())
    return city


def fetch_ibge_municipalities(p_uf: str, p_timeout_s: float = 20.0) -> list[str]:
    uf = str(p_uf).strip().upper()
    url = IBGE_MUNICIPIOS_URL.format(uf=uf)
    resp = requests.get(url, timeout=p_timeout_s)
    resp.raise_for_status()
    data = resp.json()

    municipalities: list[str] = []
    for item in data:
        name = str(item.get("nome", "")).strip()
        if name:
            municipalities.append(normalize_city_name(name))

    municipalities = sorted(set(municipalities))
    if not municipalities:
        raise ValueError(f"No municipalities returned by IBGE for UF={uf}")
    return municipalities


def load_existing_output_keys(p_output_csv_path: str) -> set[tuple[str, str, str, str]]:
    if not os.path.exists(p_output_csv_path):
        return set()

    with open(p_output_csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != OUTPUT_HEADER:
            raise ValueError(
                f"Unexpected header in {p_output_csv_path}: {reader.fieldnames} (expected {OUTPUT_HEADER})"
            )

        keys: set[tuple[str, str, str, str]] = set()
        for row in reader:
            keys.add(
                (
                    normalize_city_name(row["cidade_origem"]),
                    str(row["uf_origem"]).strip().upper(),
                    normalize_city_name(row["cidade_destino"]),
                    str(row["uf_destino"]).strip().upper(),
                )
            )
        return keys


def ensure_output_file(p_output_csv_path: str):
    if os.path.exists(p_output_csv_path):
        return
    with open(p_output_csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_HEADER)
        writer.writeheader()


def append_output_row(
    p_output_csv_path: str, p_row: dict[str, str | float], p_lock: threading.RLock
):
    with p_lock:
        with open(p_output_csv_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_HEADER)
            writer.writerow(p_row)


def build_output_row(
    p_origin_city: str,
    p_origin_uf: str,
    p_destination_city: str,
    p_destination_uf: str,
    p_distance_km: float,
) -> dict[str, str | float]:
    return {
        "cidade_origem": normalize_city_name(p_origin_city),
        "uf_origem": str(p_origin_uf).strip().upper(),
        "cidade_destino": normalize_city_name(p_destination_city),
        "uf_destino": str(p_destination_uf).strip().upper(),
        "distancia_km": float(p_distance_km),
        "fonte": "google",
    }


def run(
    p_output_csv_path: str = OUTPUT_CSV_PATH,
    p_max_workers: int | None = None,
    p_max_attempts: int = 3,
    p_base_backoff_s: float = 0.5,
):
    """Export missing distances to a new CSV with the official header."""
    load_dotenv()

    output_csv_path = str(p_output_csv_path)
    ensure_output_file(output_csv_path)

    if p_max_workers is None:
        max_workers = int(os.environ.get("DISTANCE_UPDATER_MAX_WORKERS", "16"))
    else:
        max_workers = int(p_max_workers)
    max_workers = max(1, max_workers)

    existing_keys = load_existing_output_keys(output_csv_path)

    municipalities_by_uf: dict[str, list[str]] = {}
    for uf in sorted({spec.origin_uf for spec in DEFAULT_ROUTE_SPECS}):
        municipalities_by_uf[uf] = fetch_ibge_municipalities(uf)

    required_routes: list[tuple[str, str, str, str]] = []
    for spec in DEFAULT_ROUTE_SPECS:
        origin_uf = spec.origin_uf.strip().upper()
        dest_city = normalize_city_name(spec.destination_city)
        dest_uf = spec.destination_uf.strip().upper()

        for origin_city in municipalities_by_uf.get(origin_uf, []):
            required_routes.append((origin_city, origin_uf, dest_city, dest_uf))

    missing_routes = [r for r in required_routes if r not in existing_keys]

    logger.info("Existing rows in output: %d", len(existing_keys))
    logger.info("Total required routes: %d", len(required_routes))
    logger.info("Missing routes to fetch: %d", len(missing_routes))

    lock = threading.RLock()
    appended = 0
    failed = 0

    def resolve_distance(route: tuple[str, str, str, str]) -> tuple[tuple[str, str, str, str], float | None]:
        origin_city, origin_uf, dest_city, dest_uf = route
        attempts = max(1, int(p_max_attempts))
        backoff = max(0.0, float(p_base_backoff_s))

        for attempt in range(1, attempts + 1):
            dist_km = get_distance_from_google(origin_city, origin_uf, dest_city, dest_uf)
            if dist_km is not None:
                return route, dist_km
            if attempt < attempts and backoff > 0:
                time.sleep(backoff * (2 ** (attempt - 1)))

        return route, None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(resolve_distance, r) for r in missing_routes]
        for fut in as_completed(futures):
            route, dist_km = fut.result()
            if dist_km is None:
                failed += 1
                logger.warning("Could not get distance for %s", route)
                continue

            with lock:
                if route in existing_keys:
                    continue
                existing_keys.add(route)
                row = build_output_row(route[0], route[1], route[2], route[3], dist_km)
                append_output_row(output_csv_path, row, lock)
                appended += 1

    logger.info("Done. Appended: %d | Failed: %d", appended, failed)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run()

