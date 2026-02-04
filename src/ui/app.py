import os
from typing import List, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

from src.core.screener_logic import list_processed_files, load_config


def _load_dataframe(path: str) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()
    if path.lower().endswith(".csv"):
        return pd.read_csv(path)
    return pd.read_excel(path)


def _guess_metric_columns(columns: List[str]) -> Tuple[List[str], List[str]]:
    pct_cols = [c for c in columns if c.endswith("涨幅")]
    rank_cols = [c for c in columns if c.endswith("排名")]
    return pct_cols, rank_cols


def _apply_text_filter(df: pd.DataFrame, keyword: str) -> pd.DataFrame:
    if not keyword:
        return df
    keyword = keyword.strip()
    if not keyword:
        return df
    mask = df["代码"].astype(str).str.contains(keyword) | df["名称"].astype(str).str.contains(
        keyword
    )
    return df[mask]


def _numeric_filter(df: pd.DataFrame, column: str, label: str) -> pd.DataFrame:
    if column not in df.columns:
        return df
    min_val = float(df[column].min())
    max_val = float(df[column].max())
    selected = st.slider(label, min_val, max_val, (min_val, max_val))
    return df[df[column].between(selected[0], selected[1])]


def main() -> None:
    st.set_page_config(page_title="DingHai Screener", layout="wide")
    st.title("DingHai 数据筛选与可视化")

    config = load_config("config.yaml")
    os.makedirs(config.processed_dir, exist_ok=True)
    processed_files = list_processed_files(config.processed_dir)
    latest_file = processed_files[-1] if processed_files else ""

    selected_file = st.sidebar.selectbox(
        "选择处理后的数据集",
        options=[""] + processed_files,
        index=(processed_files.index(latest_file) + 1) if latest_file else 0,
    )

    df = _load_dataframe(selected_file)
    if df.empty:
        st.info("暂无处理后的数据，请先在 data/ 中拖入新文件。")
        return

    pct_cols, rank_cols = _guess_metric_columns(list(df.columns))

    keyword = st.sidebar.text_input("代码/名称搜索")
    df = _apply_text_filter(df, keyword)

    if "Delta" in df.columns:
        df = _numeric_filter(df, "Delta", "Delta 范围")
    for col in pct_cols[:2]:
        df = _numeric_filter(df, col, f"{col} 范围")
    for col in rank_cols[:2]:
        df = _numeric_filter(df, col, f"{col} 范围")

    top_n = st.sidebar.number_input(
        "显示前 N 条", min_value=10, max_value=1000, value=config.ui.get("default_top_n", 200)
    )
    df = df.head(int(top_n))

    st.subheader("筛选结果")
    st.dataframe(df, use_container_width=True)

    if "Delta" in df.columns:
        st.subheader("Delta 分布")
        st.plotly_chart(px.histogram(df, x="Delta", nbins=30), use_container_width=True)

    if pct_cols and "Delta" in df.columns:
        st.subheader("涨幅 vs Delta")
        st.plotly_chart(
            px.scatter(df, x=pct_cols[-1], y="Delta", hover_data=["代码", "名称"]),
            use_container_width=True,
        )

    st.download_button(
        "导出筛选结果 CSV",
        data=df.to_csv(index=False).encode("utf-8-sig"),
        file_name="filtered_results.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
