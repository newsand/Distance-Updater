import argparse
import logging
from datetime import date

from database import batch_update_distances, get_connection, get_db_config
from googletools import get_distance_from_google
from main import (
    CSV_PATH,
    VERSION,
    append_to_cache,
    build_route_key,
    calculate_final_distance,
    get_distance_from_cache,
    load_distances_cache,
)

logger = logging.getLogger(__name__)

QUERY_LEGACY_BASE = """
    SELECT s.id,
           origem.nome AS origem,
           origem.sigla_uf AS uf_origem,
           destino.nome AS destino,
           destino.sigla_uf AS uf_destino
    FROM solicitacao s
    JOIN rel_solicitacao_veiculo rsv ON s.id = rsv.solicitacao_id
    JOIN endereco e ON e.id = rsv.origem
    JOIN endereco ee ON ee.id = rsv.destino
    JOIN municipio origem ON e.municipio_id = origem.id
    JOIN municipio destino ON ee.municipio_id = destino.id
    WHERE s.data_conclusao IS NOT NULL
      AND s.distancia = 0
      AND s.data_conclusao::date <= CURRENT_DATE
"""


def fetch_legacy_pending(conn, p_from_date: date | None = None) -> list[dict]:
    """Fetch all completed solicitacoes without distance, optionally from a start date."""
    query = QUERY_LEGACY_BASE
    params: tuple[date, ...] = ()

    if p_from_date is not None:
        query += "\n      AND s.data_conclusao >= %s"
        params = (p_from_date,)

    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def parse_from_date(p_month: int | None, p_year: int | None) -> date | None:
    """Parse optional -m / -YYYY filters into the first day of that month."""
    if p_month is None and p_year is None:
        return None
    if p_month is None or p_year is None:
        raise ValueError("Both -m MONTH and -YYYY YEAR are required when filtering by date")
    if not 1 <= p_month <= 12:
        raise ValueError("Month must be between 1 and 12")
    return date(p_year, p_month, 1)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for legacy distance updater."""
    parser = argparse.ArgumentParser(
        description="Legacy bulk distance updater for all historical pending records"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report how many records would be updated",
    )
    parser.add_argument(
        "-m",
        type=int,
        metavar="MONTH",
        dest="from_month",
        help="Filter from month (1-12), requires -YYYY",
    )
    parser.add_argument(
        "-YYYY",
        type=int,
        metavar="YEAR",
        dest="from_year",
        help="Filter from year, requires -m",
    )
    return parser.parse_args()


def run(p_dry_run: bool = False, p_from_date: date | None = None):
    """Run legacy distance update for all historical pending solicitacoes."""
    logger.info("Distance-Updater legacy %s", VERSION)

    config = get_db_config()
    conn = get_connection(config)

    pending = fetch_legacy_pending(conn, p_from_date)
    logger.info("Found %d pending records", len(pending))

    if p_dry_run:
        logger.info("Dry run: %d records would be updated", len(pending))
        conn.close()
        return

    df = load_distances_cache(CSV_PATH)
    updates: list[tuple[int, int]] = []

    for row in pending:
        route_key = build_route_key(
            row["origem"], row["uf_origem"], row["destino"], row["uf_destino"]
        )
        origem_full = f"{row['origem']}/{row['uf_origem']}"
        destino_full = f"{row['destino']}/{row['uf_destino']}"

        dist_km = get_distance_from_cache(df, route_key)

        if dist_km is None:
            dist_km = get_distance_from_google(
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
            logger.warning("Could not get distance for %s", route_key)

    if updates:
        batch_update_distances(conn, updates)
        logger.info("Updated %d records", len(updates))
    else:
        logger.info("No updates needed")

    conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    try:
        from_date = parse_from_date(args.from_month, args.from_year)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    run(p_dry_run=args.dry_run, p_from_date=from_date)
