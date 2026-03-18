import os

import psycopg2
from dotenv import load_dotenv
from pydantic import BaseModel


class DBConfig(BaseModel):
    dbname: str
    user: str
    password: str
    host: str
    port: str = "5432"
    sslmode: str = "require"


def get_db_config() -> DBConfig:
    load_dotenv()
    return DBConfig(
        dbname=os.getenv("DB_NAME", ""),
        user=os.getenv("DB_USER", ""),
        password=os.getenv("DB_PASSWORD", ""),
        host=os.getenv("DB_HOST", ""),
        port=os.getenv("DB_PORT", "5432"),
        sslmode=os.getenv("DB_SSLMODE", "require"),
    )


def get_connection(config: DBConfig):
    return psycopg2.connect(
        dbname=config.dbname,
        user=config.user,
        password=config.password,
        host=config.host,
        port=config.port,
        sslmode=config.sslmode,
    )


QUERY_PENDING = """
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
"""


def fetch_pending(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(QUERY_PENDING)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def batch_update_distances(conn, updates: list[tuple[int, int]], batch_size: int = 500):
    """Batch-updates distancia column. updates = list of (final_distance, solicitacao_id)."""
    with conn.cursor() as cur:
        for i in range(0, len(updates), batch_size):
            batch = updates[i : i + batch_size]
            args = ",".join(
                cur.mogrify("(%s, %s)", (dist, sid)).decode() for dist, sid in batch
            )
            cur.execute(
                f"""
                UPDATE solicitacao AS s
                SET distancia = v.distancia
                FROM (VALUES {args}) AS v(distancia, id)
                WHERE s.id = v.id
                """
            )
    conn.commit()
