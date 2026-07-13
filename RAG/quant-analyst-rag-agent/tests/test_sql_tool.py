from quant_agent.database.build_db import build_database
from quant_agent.database.sql_queries import QueryService
from quant_agent.config import Paths

def test_best_factor_by_high_volatility(tmp_path):
    paths = Paths(); db_path = build_database(tmp_path / "quant_agent.db", paths.raw_data_dir); service = QueryService(db_path); rows = service.get_best_factor_by_regime("high-volatility")
    assert rows[0]["factor_name"] == "sector_relative_strength"; assert rows[0]["sharpe"] == 0.67

def test_compare_momentum_and_volatility(tmp_path):
    paths = Paths(); db_path = build_database(tmp_path / "quant_agent.db", paths.raw_data_dir); service = QueryService(db_path); rows = service.compare_factors(["60-day momentum", "volatility"], "2020-01-01", "2024-12-31"); factors = {row["factor_name"] for row in rows}
    assert {"momentum_60d", "volatility_20d"}.issubset(factors)
