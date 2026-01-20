#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_level2_data_free.py

功能：
1. 读取 target.csv。
2. 使用 yfinance (Yahoo财经) 免费获取 Sector, Industry, MarketCap。
3. 支持断点续传（如果中途停止，下次运行会跳过已下载的）。
4. 整理并输出 Excel。

注意：
免费接口速度受限，17000只股票可能需要较长时间（建议挂机运行）。
"""

import pandas as pd
import yfinance as yf
import os
import time
from tqdm import tqdm  # 进度条库

# === 配置 ===
INPUT_FILE = "target.csv"
OUTPUT_FILE = "二级数据整理_免费版.xlsx"
TEMP_FILE = "temp_data_cache.csv"  # 缓存文件，用于断点续传

def load_file(filepath):
    """读取目标文件"""
    encodings = ['utf-8', 'gbk', 'gb18030']
    for enc in encodings:
        try:
            return pd.read_csv(filepath, encoding=enc)
        except:
            continue
    raise ValueError(f"无法读取文件 {filepath}")

def main():
    # 1. 读取原始列表
    print(f"正在读取 {INPUT_FILE}...")
    df = load_file(INPUT_FILE)
    
    # 找到代码列
    ticker_col = None
    for col in ['代码', 'Ticker', 'Symbol']:
        if col in df.columns:
            ticker_col = col
            break
    
    if not ticker_col:
        print("错误：找不到代码列")
        return

    # 清洗代码 (去空格，转大写)
    # 注意：Yahoo财经的代码中，美股通常不需要后缀，但如果是其他市场可能需要调整
    # 您的数据看起来是美股，直接用即可
    tickers = df[ticker_col].astype(str).str.upper().str.strip().tolist()
    
    print(f"共加载 {len(tickers)} 个股票代码。")

    # 2. 检查是否有缓存（断点续传）
    cached_data = {}
    if os.path.exists(TEMP_FILE):
        print("发现缓存文件，正在恢复进度...")
        try:
            df_cache = pd.read_csv(TEMP_FILE)
            # 将缓存转为字典: ticker -> {info}
            for _, row in df_cache.iterrows():
                cached_data[row['Ticker']] = {
                    'Sector': row['Sector'],
                    'Industry': row['Industry'],
                    'MarketCap': row['MarketCap']
                }
            print(f"已恢复 {len(cached_data)} 条数据。")
        except Exception as e:
            print(f"缓存文件读取失败: {e}，将重新开始。")

    # 3. 开始抓取
    results = []
    
    # 只需要抓取缓存里没有的
    tickers_to_fetch = [t for t in tickers if t not in cached_data]
    
    if not tickers_to_fetch and len(tickers) > 0:
        print("所有数据已在缓存中！直接开始整理...")
    elif tickers_to_fetch:
        print(f"剩余 {len(tickers_to_fetch)} 个股票待下载，开始运行 (按 Ctrl+C 可随时停止)...")
        
        # 使用 yfinance 获取信息
        # 为了提高效率和稳定性，我们每 10 个一组或逐个请求
        # yfinance 的 Tickers 类适合批量，但对 17000 个容易卡死，这里用单次循环更稳健
        
        batch_size = 10  # 每次存一次盘的间隔
        current_batch = []
        
        # 进度条
        pbar = tqdm(tickers_to_fetch, unit="stock")
        
        for i, ticker in enumerate(pbar):
            try:
                # 获取数据
                stock = yf.Ticker(ticker)
                info = stock.info
                
                # 提取需要的字段
                # 注意：有些退市股票可能没有 sector
                sec = info.get('sector', 'Unknown')
                ind = info.get('industry', 'Unknown')
                cap = info.get('marketCap', 0)
                
                # 如果获取到的是空值，标记一下
                if sec == 'Unknown' and ind == 'Unknown':
                    # 有时候 yfinance 即使代码对也返回空，尝试 fast_info
                    try:
                        # fast_info 有时更稳定但不含行业，只能作为兜底
                        pass 
                    except:
                        pass

                data_row = {
                    'Ticker': ticker,
                    'Sector': sec,
                    'Industry': ind,
                    'MarketCap': cap
                }
                
                # 更新到内存和缓存列表
                cached_data[ticker] = data_row
                current_batch.append(data_row)
                
            except Exception as e:
                # 报错也记录，避免死循环，标记为 Error
                error_row = {'Ticker': ticker, 'Sector': 'Error', 'Industry': 'Error', 'MarketCap': 0}
                cached_data[ticker] = error_row
                current_batch.append(error_row)
            
            # 每抓取 batch_size 个，写入一次 CSV 缓存
            if len(current_batch) >= batch_size:
                df_temp = pd.DataFrame(current_batch)
                # 追加模式写入，如果文件不存在则写入头
                header = not os.path.exists(TEMP_FILE)
                df_temp.to_csv(TEMP_FILE, mode='a', header=header, index=False)
                current_batch = []
                # 稍微休眠一下避免被封 IP
                time.sleep(0.1)

        # 循环结束，把最后剩余的也写入
        if current_batch:
            df_temp = pd.DataFrame(current_batch)
            header = not os.path.exists(TEMP_FILE)
            df_temp.to_csv(TEMP_FILE, mode='a', header=header, index=False)

    # 4. 合并数据并输出 Excel
    print("\n正在合并数据并生成 Excel...")
    
    # 将缓存字典转回 DataFrame
    # 确保顺序和原始 target.csv 一致
    final_data = []
    for t in tickers:
        info = cached_data.get(t, {'Sector': 'Pending', 'Industry': 'Pending', 'MarketCap': 0})
        final_data.append(info)
    
    df_info = pd.DataFrame(final_data)
    
    # 合并原始数据和新抓取的数据
    df['Match_Ticker'] = df[ticker_col].astype(str).str.upper().str.strip()
    df_info['Match_Ticker'] = df_info['Ticker']
    
    df_merged = pd.merge(df, df_info, on='Match_Ticker', how='left')
    
    # 整理列名
    df_merged['一级行业(Sector)'] = df_merged['Sector']
    df_merged['二级行业(Industry)'] = df_merged['Industry']
    df_merged['市值(Yahoo)'] = df_merged['MarketCap']
    
    # 删除辅助列
    drop_cols = ['Match_Ticker', 'Ticker_y', 'Sector', 'Industry', 'MarketCap']
    df_merged.drop(columns=[c for c in drop_cols if c in df_merged.columns], inplace=True)
    if 'Ticker_x' in df_merged.columns:
        df_merged.rename(columns={'Ticker_x': 'Ticker'}, inplace=True)

    # 排序
    df_merged.sort_values(by=['一级行业(Sector)', '二级行业(Industry)', ticker_col], inplace=True)
    
    # 保存
    df_merged.to_excel(OUTPUT_FILE, index=False)
    print(f"完成！文件已保存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()