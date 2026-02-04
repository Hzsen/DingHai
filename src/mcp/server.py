import os
from typing import Dict, List, Optional

import pandas as pd
from mcp.server.fastmcp import FastMCP

from src.core.screener_logic import list_processed_files, load_config

mcp = FastMCP("dinghai-processed")


def _read_dataframe(path: str, max_rows: Optional[int] = None) -> pd.DataFrame:
    if path.lower().endswith(".csv"):
        df = pd.read_csv(path)
    else:
        df = pd.read_excel(path)
    if max_rows:
        return df.head(max_rows)
    return df


@mcp.tool()
def list_processed() -> List[Dict[str, str]]:
    """List processed datasets with filename and path."""
    config = load_config("config.yaml")
    files = list_processed_files(config.processed_dir)
    return [{"name": os.path.basename(path), "path": path} for path in files]


@mcp.tool()
def read_processed(path: str, max_rows: int = 200) -> Dict[str, List[Dict[str, object]]]:
    """Read a processed dataset and return rows as JSON."""
    if not os.path.isfile(path):
        raise FileNotFoundError(f"File not found: {path}")
    df = _read_dataframe(path, max_rows=max_rows)
    return {"rows": df.to_dict(orient="records")}


if __name__ == "__main__":
    mcp.run()
