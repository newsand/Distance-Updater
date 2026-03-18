import os
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "distancias.csv"

from main import (
    build_route_key,
    calculate_final_distance,
    get_distance_from_cache,
    load_distances_cache,
)


@pytest.fixture(scope="module")
def distances_df() -> pd.DataFrame:
    return load_distances_cache(str(CSV_PATH))


@pytest.fixture(scope="module")
def sample_10(distances_df: pd.DataFrame) -> pd.DataFrame:
    return distances_df.sample(n=10, random_state=42)


# ---------------------------------------------------------------------------
# build_route_key
# ---------------------------------------------------------------------------
class TestBuildRouteKey:
    def test_basic_format(self):
        key = build_route_key("SÃO PAULO", "SP", "RIO DE JANEIRO", "RJ")
        assert key == "SÃO PAULO/SP:RIO DE JANEIRO/RJ"

    def test_strips_whitespace(self):
        key = build_route_key("  CONTAGEM  ", " MG ", " SÃO PAULO ", " SP ")
        assert key == "CONTAGEM/MG:SÃO PAULO/SP"

    def test_uppercases(self):
        key = build_route_key("belo horizonte", "mg", "rio de janeiro", "rj")
        assert key == "BELO HORIZONTE/MG:RIO DE JANEIRO/RJ"


# ---------------------------------------------------------------------------
# get_distance_from_cache
# ---------------------------------------------------------------------------
class TestGetDistanceFromCache:
    def test_known_route_found(self, distances_df):
        dist = get_distance_from_cache(distances_df, "SÃO PAULO/SP:CONTAGEM/MG")
        assert dist is not None
        assert isinstance(dist, float)
        assert dist == pytest.approx(574.287)

    def test_another_known_route(self, distances_df):
        dist = get_distance_from_cache(
            distances_df, "WENCESLAU BRAZ/MG:RIO DE JANEIRO/RJ"
        )
        assert dist == pytest.approx(300.829)

    def test_unknown_route_returns_none(self, distances_df):
        dist = get_distance_from_cache(distances_df, "FAKE CITY/XX:NOWHERE/YY")
        assert dist is None

    def test_same_city_zero(self, distances_df):
        dist = get_distance_from_cache(
            distances_df, "BELO HORIZONTE/MG:BELO HORIZONTE/MG"
        )
        assert dist is not None
        assert dist == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# calculate_final_distance  (raw_km + 40, rounded)
# ---------------------------------------------------------------------------
class TestCalculateFinalDistance:
    def test_adds_40_and_rounds(self):
        assert calculate_final_distance(300.829) == 341

    def test_zero_distance(self):
        assert calculate_final_distance(0.0) == 40

    def test_exact_integer(self):
        assert calculate_final_distance(100.0) == 140

    def test_rounds_half_up(self):
        assert calculate_final_distance(0.5) == 40

    def test_large_value(self):
        assert calculate_final_distance(1133.76) == 1174

    def test_return_type_is_int(self):
        result = calculate_final_distance(574.287)
        assert isinstance(result, int)


# ---------------------------------------------------------------------------
# Sample 10 rows: end-to-end cache lookup + final distance calc
# ---------------------------------------------------------------------------
class TestSample10Rows:
    """Pick 10 random rows from the CSV and verify the full pipeline:
    1. get_distance_from_cache finds them
    2. calculate_final_distance produces correct int = round(km + 40)
    """

    def test_all_10_found_in_cache(self, distances_df, sample_10):
        for _, row in sample_10.iterrows():
            route = row["ROTA"]
            dist = get_distance_from_cache(distances_df, route)
            assert dist is not None, f"Route not found in cache: {route}"
            assert dist == pytest.approx(float(row["distancia_km"]))

    def test_final_distance_for_sample(self, sample_10):
        for _, row in sample_10.iterrows():
            raw_km = float(row["distancia_km"])
            final = calculate_final_distance(raw_km)

            assert isinstance(final, int), f"Expected int, got {type(final)}"
            assert final == round(raw_km + 40), (
                f"Route {row['ROTA']}: expected {round(raw_km + 40)}, got {final}"
            )
            assert final >= 40, "Minimum distance should be 40 (0 km + 40)"

    def test_sample_has_10_rows(self, sample_10):
        assert len(sample_10) == 10
