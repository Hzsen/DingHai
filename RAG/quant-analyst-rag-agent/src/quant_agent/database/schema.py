from __future__ import annotations

SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS prices (ticker TEXT NOT NULL, date DATE NOT NULL, open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL, adjusted_close REAL NOT NULL, volume INTEGER NOT NULL, sector TEXT NOT NULL, PRIMARY KEY (ticker, date))""",
    """CREATE TABLE IF NOT EXISTS factors (ticker TEXT NOT NULL, date DATE NOT NULL, factor_name TEXT NOT NULL, factor_value REAL NOT NULL, factor_rank REAL NOT NULL, sector TEXT NOT NULL, PRIMARY KEY (ticker, date, factor_name))""",
    """CREATE TABLE IF NOT EXISTS backtest_results (strategy_name TEXT NOT NULL, factor_name TEXT NOT NULL, start_date DATE NOT NULL, end_date DATE NOT NULL, annual_return REAL NOT NULL, sharpe REAL NOT NULL, max_drawdown REAL NOT NULL, volatility REAL NOT NULL, turnover REAL NOT NULL, transaction_cost_bps REAL NOT NULL, benchmark_return REAL NOT NULL, PRIMARY KEY (strategy_name, factor_name, start_date, end_date))""",
    """CREATE TABLE IF NOT EXISTS regime_performance (strategy_name TEXT NOT NULL, factor_name TEXT NOT NULL, regime TEXT NOT NULL, annual_return REAL NOT NULL, sharpe REAL NOT NULL, max_drawdown REAL NOT NULL, hit_rate REAL NOT NULL, PRIMARY KEY (strategy_name, factor_name, regime))""",
    """CREATE TABLE IF NOT EXISTS anomaly_logs (date DATE NOT NULL, ticker TEXT NOT NULL, anomaly_type TEXT NOT NULL, description TEXT NOT NULL, severity TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS factor_definitions (factor_name TEXT PRIMARY KEY, definition TEXT NOT NULL, formula TEXT NOT NULL, interpretation TEXT NOT NULL, common_failure_modes TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS idx_factors_name_date ON factors (factor_name, date)",
    "CREATE INDEX IF NOT EXISTS idx_backtests_factor ON backtest_results (factor_name)",
    "CREATE INDEX IF NOT EXISTS idx_regime_factor_regime ON regime_performance (factor_name, regime)",
    "CREATE INDEX IF NOT EXISTS idx_anomalies_date_type ON anomaly_logs (date, anomaly_type)",
]
TABLE_INSERT_COLUMNS = {
    "prices": ["ticker", "date", "open", "high", "low", "close", "adjusted_close", "volume", "sector"],
    "factors": ["ticker", "date", "factor_name", "factor_value", "factor_rank", "sector"],
    "backtest_results": ["strategy_name", "factor_name", "start_date", "end_date", "annual_return", "sharpe", "max_drawdown", "volatility", "turnover", "transaction_cost_bps", "benchmark_return"],
    "regime_performance": ["strategy_name", "factor_name", "regime", "annual_return", "sharpe", "max_drawdown", "hit_rate"],
    "anomaly_logs": ["date", "ticker", "anomaly_type", "description", "severity"],
    "factor_definitions": ["factor_name", "definition", "formula", "interpretation", "common_failure_modes"],
}
