import os
import re
import pandas as pd


def find_header_row(path, max_scan_rows=5):
    """
    Try to locate the header row that contains '代码' and '名称' or '涨幅%'.
    Returns number of rows to skip (skiprows).
    """
    sample = pd.read_csv(path, header=None, nrows=max_scan_rows, encoding="utf-8-sig")
    for i in range(len(sample)):
        row = sample.iloc[i].astype(str).str.replace(r"\s+", "", regex=True)
        if row.str.contains("代码").any() and (row.str.contains("名称").any() or row.str.contains("涨幅%").any()):
            return i
    return 0


def find_header_row_excel(path, max_scan_rows=5):
    sample = pd.read_excel(path, header=None, nrows=max_scan_rows)
    for i in range(len(sample)):
        row = sample.iloc[i].astype(str).str.replace(r"\s+", "", regex=True)
        if row.str.contains("代码").any() and (row.str.contains("名称").any() or row.str.contains("涨幅%").any()):
            return i
    return 0


def read_data(path):
    ext = os.path.splitext(path)[1].lower()
    if ext in [".csv", ".txt"]:
        skip = find_header_row(path)
        df = pd.read_csv(path, skiprows=skip, encoding="utf-8-sig")
    else:
        skip = find_header_row_excel(path)
        df = pd.read_excel(path, skiprows=skip)
    return df


def normalize_columns(df):
    # Strip spaces in column names (both ends and middle)
    df.columns = [re.sub(r"\s+", "", str(c)) for c in df.columns]
    # Try to locate required columns with flexible names
    code_col = next((c for c in df.columns if "代码" in c), None)
    name_col = next((c for c in df.columns if "名称" in c), None)
    pct_col = next((c for c in df.columns if "涨幅" in c), None)

    if not code_col or not pct_col:
        raise ValueError("未找到必要列：代码 或 涨幅%")

    # Rename for consistency
    rename_map = {}
    if code_col != "代码":
        rename_map[code_col] = "代码"
    if name_col and name_col != "名称":
        rename_map[name_col] = "名称"
    if pct_col != "涨幅%":
        rename_map[pct_col] = "涨幅%"

    df = df.rename(columns=rename_map)

    # Ensure code is 6-digit string
    df["代码"] = df["代码"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)

    # Clean percent column to float
    df["涨幅%"] = (
        df["涨幅%"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .replace(["--", "nan", "None", ""], "0")
    )
    df["涨幅%"] = pd.to_numeric(df["涨幅%"], errors="coerce").fillna(0.0)

    return df


def compute_rank(df, rank_col):
    df = df.sort_values("涨幅%", ascending=False).copy()
    df[rank_col] = range(1, len(df) + 1)
    return df


def extract_date_label(path, fallback):
    match = re.search(r"(20\\d{6})", path)
    if match:
        return match.group(1)[-4:]
    return fallback


def get_range_columns(df, start_col="AAA", end_col="反核标"):
    columns = list(df.columns)
    if start_col not in columns or end_col not in columns:
        return []
    start_idx = columns.index(start_col)
    end_idx = columns.index(end_col)
    if end_idx < start_idx:
        start_idx, end_idx = end_idx, start_idx
    return columns[start_idx : end_idx + 1]


def main():
    # 修改为你的实际路径与文件名
    day1_path = "data/沪深京非ST20260119.xlsx"
    day2_path = "data/沪深京非ST20260120.xlsx"

    day1 = normalize_columns(read_data(day1_path))
    day2 = normalize_columns(read_data(day2_path))

    day1_label = extract_date_label(day1_path, "Day1")
    day2_label = extract_date_label(day2_path, "Day2")

    day1 = compute_rank(day1, "Rank_Day1")
    day2 = compute_rank(day2, "Rank_Day2")

    range_cols_day1 = get_range_columns(day1)
    range_cols = [c for c in range_cols_day1 if c in day2.columns]

    # 取交集
    merged = pd.merge(
        day1[["代码", "名称", "涨幅%", "Rank_Day1"] + range_cols],
        day2[["代码", "名称", "涨幅%", "Rank_Day2"] + range_cols],
        on="代码",
        how="inner",
        suffixes=("_Day1", "_Day2"),
    )

    # 如有名称列冲突，优先Day1名称
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
        day1_col = f"{col}_Day1"
        day2_col = f"{col}_Day2"
        if day1_col in merged.columns:
            merged = merged.rename(columns={day1_col: f"{day1_label}_{col}"})
        if day2_col in merged.columns:
            merged = merged.rename(columns={day2_col: f"{day2_label}_{col}"})

    merged["Delta"] = merged[f"{day1_label}排名"] - merged[f"{day2_label}排名"]

    result = merged[
        [
            "代码",
            "名称",
            f"{day1_label}涨幅",
            f"{day1_label}排名",
            f"{day2_label}涨幅",
            f"{day2_label}排名",
            "Delta",
        ]
        + [f"{day1_label}_{col}" for col in range_cols]
        + [f"{day2_label}_{col}" for col in range_cols]
    ].sort_values("Delta", ascending=False)

    result.to_excel("stock_rank_change19_20.xlsx", index=False)
    print("已保存: stock_rank_change19_20.xlsx")


if __name__ == "__main__":
    main()