import pandas as pd

from database import batch_update_distances, fetch_pending, get_connection, get_db_config
from googletools import buscar_google_maps

VERSION = "ßeta"
CSV_PATH = "distancias.csv"


def load_distances_cache(csv_path: str) -> pd.DataFrame:
    return pd.read_csv(csv_path)


def build_route_key(
    cidade_origem: str, uf_origem: str, cidade_destino: str, uf_destino: str
) -> str:
    o = f"{cidade_origem.strip().upper()}/{uf_origem.strip().upper()}"
    d = f"{cidade_destino.strip().upper()}/{uf_destino.strip().upper()}"
    return f"{o}:{d}"


def get_distance_from_cache(df: pd.DataFrame, route_key: str) -> float | None:
    match = df[df["ROTA"] == route_key]
    if not match.empty:
        return float(match.iloc[0]["distancia_km"])
    return None


def calculate_final_distance(raw_km: float) -> int:
    """Distance stored in DB = round(raw_km + 40)."""
    return round(raw_km + 40)


def append_to_cache(
    df: pd.DataFrame,
    route_key: str,
    origem: str,
    destino: str,
    dist_km: float,
    csv_path: str,
) -> pd.DataFrame:
    new_row = pd.DataFrame(
        [
            {
                "ROTA": route_key,
                "origem": origem,
                "destino": destino,
                "distancia_km": dist_km,
                "distancia_texto": f"{round(dist_km)} km",
            }
        ]
    )
    df = pd.concat([df, new_row], ignore_index=True)
    df.to_csv(csv_path, index=False)
    return df


def run():
    print(f"Distance-Updater {VERSION}")

    config = get_db_config()
    conn = get_connection(config)

    df = load_distances_cache(CSV_PATH)
    pending = fetch_pending(conn)
    print(f"Found {len(pending)} pending records")

    updates: list[tuple[int, int]] = []

    for row in pending:
        route_key = build_route_key(
            row["origem"], row["uf_origem"], row["destino"], row["uf_destino"]
        )
        origem_full = f"{row['origem']}/{row['uf_origem']}"
        destino_full = f"{row['destino']}/{row['uf_destino']}"

        dist_km = get_distance_from_cache(df, route_key)

        if dist_km is None:
            dist_km = buscar_google_maps(
                row["origem"], row["uf_origem"], row["destino"], row["uf_destino"]
            )
            if dist_km is not None:
                df = append_to_cache(
                    df, route_key, origem_full, destino_full, dist_km, CSV_PATH
                )

        if dist_km is not None:
            final = calculate_final_distance(dist_km)
            updates.append((final, row["id"]))
        else:
            print(f"Could not get distance for {route_key}")

    if updates:
        batch_update_distances(conn, updates)
        print(f"Updated {len(updates)} records")
    else:
        print("No updates needed")

    conn.close()


if __name__ == "__main__":
    run()
