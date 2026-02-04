import os
import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import pandas as pd
import yaml


@dataclass
class ETLConfig:
    data_dir: str
    processed_dir: str
    input_extensions: List[str]
    min_inputs: int
    header_scan_rows: int
    encoding_candidates: List[str]
    excel_engines: List[str]
    range_columns_start: str
    range_columns_end: str
    output_name_template: str


def load_config(path: str) -> ETLConfig:
    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    return ETLConfig(
        data_dir=raw["data_dir"],
        processed_dir=raw["processed_dir"],
        input_extensions=[ext.lower() for ext in raw["input_extensions"]],
        min_inputs=int(raw.get("min_inputs", 2)),
        header_scan_rows=int(raw.get("header_scan_rows", 5)),
        encoding_candidates=list(raw.get("encoding_candidates", [])),
        excel_engines=list(raw.get("excel_engines", [])),
        range_columns_start=raw["range_columns"]["start"],
        range_columns_end=raw["range_columns"]["end"],
        output_name_template=raw.get(
            "output_name_template", "stock_rank_change_{day1}_{day2}.xlsx"
        ),
    )


def read_csv_with_encodings(path: str, encodings: List[str], **kwargs) -> pd.DataFrame:
    last_error: Optional[Exception] = None
    for encoding in encodings:
        try:
            return pd.read_csv(
                path, encoding=encoding, sep=None, engine="python", **kwargs
            )
        except Exception as exc:  # noqa: BLE001 - best-effort encoding fallback
            last_error = exc
    if last_error:
        raise last_error
    raise ValueError("No encoding candidates provided.")


def find_header_row(path: str, max_scan_rows: int, encodings: List[str]) -> int:
    sample = read_csv_with_encodings(path, encodings, header=None, nrows=max_scan_rows)
    for i in range(len(sample)):
        row = sample.iloc[i].astype(str).str.replace(r"\s+", "", regex=True)
        if row.str.contains("代码").any() and (
            row.str.contains("名称").any() or row.str.contains("涨幅%").any()
        ):
            return i
    return 0


def read_excel_with_engines(path: str, engines: List[str], **kwargs) -> pd.DataFrame:
    last_error: Optional[Exception] = None
    for engine in engines:
        try:
            return pd.read_excel(path, engine=engine, **kwargs)
        except Exception as exc:  # noqa: BLE001 - best-effort engine fallback
            last_error = exc
    if last_error:
        raise last_error
    raise ValueError("No excel engines provided.")


def find_header_row_excel(path: str, max_scan_rows: int, engines: List[str]) -> int:
    sample = read_excel_with_engines(path, engines, header=None, nrows=max_scan_rows)
    for i in range(len(sample)):
        row = sample.iloc[i].astype(str).str.replace(r"\s+", "", regex=True)
        if row.str.contains("代码").any() and (
            row.str.contains("名称").any() or row.str.contains("涨幅%").any()
        ):
            return i
    return 0


def read_data(path: str, config: ETLConfig) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".csv", ".txt"]:
        skip = find_header_row(path, config.header_scan_rows, config.encoding_candidates)
        return read_csv_with_encodings(path, config.encoding_candidates, skiprows=skip)
    engines = config.excel_engines
    try:
        skip = find_header_row_excel(path, config.header_scan_rows, engines)
        return read_excel_with_engines(path, engines, skiprows=skip)
    except Exception:
        skip = find_header_row(path, config.header_scan_rows, config.encoding_candidates)
        return read_csv_with_encodings(path, config.encoding_candidates, skiprows=skip)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [re.sub(r"\s+", "", str(c)) for c in df.columns]
    code_col = next((c for c in df.columns if "代码" in c), None)
    name_col = next((c for c in df.columns if "名称" in c), None)
    pct_col = next((c for c in df.columns if "涨幅" in c), None)

    if not code_col or not pct_col:
        raise ValueError("未找到必要列：代码 或 涨幅%")

    rename_map = {}
    if code_col != "代码":
        rename_map[code_col] = "代码"
    if name_col and name_col != "名称":
        rename_map[name_col] = "名称"
    if pct_col != "涨幅%":
        rename_map[pct_col] = "涨幅%"

    df = df.rename(columns=rename_map)
    df["代码"] = (
        df["代码"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)
    )
    df["涨幅%"] = (
        df["涨幅%"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .replace(["--", "nan", "None", ""], "0")
    )
    df["涨幅%"] = pd.to_numeric(df["涨幅%"], errors="coerce").fillna(0.0)
    return df


def compute_rank(df: pd.DataFrame, rank_col: str) -> pd.DataFrame:
    df = df.sort_values("涨幅%", ascending=False).copy()
    df[rank_col] = range(1, len(df) + 1)
    return df


def extract_date_label(path: str, fallback: str) -> str:
    match = re.search(r"(20\d{6})", path)
    if match:
        return match.group(1)
    return fallback


def get_range_columns(
    df: pd.DataFrame, start_col: str, end_col: str
) -> List[str]:
    columns = list(df.columns)
    if start_col not in columns or end_col not in columns:
        return []
    start_idx = columns.index(start_col)
    end_idx = columns.index(end_col)
    if end_idx < start_idx:
        start_idx, end_idx = end_idx, start_idx
    return columns[start_idx : end_idx + 1]


def discover_latest_inputs(config: ETLConfig) -> List[str]:
    candidates: List[Tuple[float, str]] = []
    for entry in os.listdir(config.data_dir):
        full_path = os.path.join(config.data_dir, entry)
        if not os.path.isfile(full_path):
            continue
        ext = os.path.splitext(entry)[1].lower()
        if ext not in config.input_extensions:
            continue
        candidates.append((os.path.getmtime(full_path), full_path))
    candidates.sort()
    return [path for _, path in candidates][-config.min_inputs :]


def prepare_inputs(input_paths: Optional[Iterable[str]], config: ETLConfig) -> List[str]:
    if input_paths:
        paths = list(input_paths)
    else:
        paths = discover_latest_inputs(config)
    if len(paths) < config.min_inputs:
        raise ValueError(f"需要至少 {config.min_inputs} 个输入文件")
    return paths


def run_etl(
    input_paths: Optional[Iterable[str]] = None,
    config_path: str = "config.yaml",
) -> str:
    config = load_config(config_path)
    os.makedirs(config.processed_dir, exist_ok=True)

    day1_path, day2_path = prepare_inputs(input_paths, config)[-2:]
    day1 = normalize_columns(read_data(day1_path, config))
    day2 = normalize_columns(read_data(day2_path, config))

    day1_label = extract_date_label(day1_path, "Day1")
    day2_label = extract_date_label(day2_path, "Day2")

    day1 = compute_rank(day1, "Rank_Day1")
    day2 = compute_rank(day2, "Rank_Day2")

    range_cols_day2 = get_range_columns(
        day2, config.range_columns_start, config.range_columns_end
    )
    range_cols = [c for c in range_cols_day2 if c in day2.columns]

    merged = pd.merge(
        day1[["代码", "名称", "涨幅%", "Rank_Day1"]],
        day2[["代码", "名称", "涨幅%", "Rank_Day2"] + range_cols],
        on="代码",
        how="inner",
        suffixes=("_Day1", "_Day2"),
    )

    if "名称_Day1" in merged.columns and "名称_Day2" in merged.columns:
        merged["名称"] = merged["名称_Day1"].fillna(merged["名称_Day2"])
        merged = merged.drop(columns=["名称_Day1", "名称_Day2"])
    elif "名称" not in merged.columns:
        merged["名称"] = ""

    merged = merged.rename(
        columns={
            "涨幅%_Day1": f"{day1_label}涨幅",
            "涨幅%_Day2": f"{day2_label}涨幅",
            "Rank_Day1": f"{day1_label}排名",
            "Rank_Day2": f"{day2_label}排名",
        }
    )

    for col in range_cols:
        day2_col = f"{col}_Day2"
        if day2_col in merged.columns:
            merged = merged.rename(columns={day2_col: f"{day2_label}_{col}"})
        elif col in merged.columns:
            merged = merged.rename(columns={col: f"{day2_label}_{col}"})

    merged["Delta"] = merged[f"{day1_label}排名"] - merged[f"{day2_label}排名"]

    result_columns = [
        "代码",
        "名称",
        f"{day1_label}涨幅",
        f"{day1_label}排名",
        f"{day2_label}涨幅",
        f"{day2_label}排名",
        "Delta",
    ] + [f"{day2_label}_{col}" for col in range_cols]

    result = merged[result_columns].sort_values("Delta", ascending=False)

    output_name = config.output_name_template.format(
        day1=day1_label, day2=day2_label
    )
    output_path = os.path.join(config.processed_dir, output_name)
    result.to_excel(output_path, index=False)
    return output_path


def list_processed_files(processed_dir: str) -> List[str]:
    if not os.path.isdir(processed_dir):
        return []
    return sorted(
        [
            os.path.join(processed_dir, name)
            for name in os.listdir(processed_dir)
            if os.path.isfile(os.path.join(processed_dir, name))
        ],
        key=os.path.getmtime,
    )
