#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
merge_finviz_local.py

功能：
将本地的 target.csv 与 finviz.csv 合并，生成包含二级行业分类的最终表格。
"""

import pandas as pd
import os
import sys

# === 文件名配置 ===
TARGET_FILE = "target.csv"      # 您的原始股票列表
FINVIZ_FILE = "finviz.csv"      # 您手动下载的 Finviz 数据
OUTPUT_FILE = "二级数据整理_Finviz合并版.xlsx" # 输出文件名

def load_file_auto_encoding(filepath):
    """尝试多种编码读取 CSV"""
    encodings = ['utf-8', 'utf-8-sig', 'gbk', 'gb18030', 'latin-1']
    for enc in encodings:
        try:
            df = pd.read_csv(filepath, encoding=enc)
            print(f"[成功] 已读取 {filepath} (编码: {enc})，共 {len(df)} 行。")
            return df
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            print(f"[错误] 找不到文件: {filepath}")
            sys.exit(1)
        except Exception as e:
            print(f"[错误] 读取 {filepath} 失败: {e}")
            sys.exit(1)
    print(f"[失败] 无法识别文件编码: {filepath}")
    sys.exit(1)

def main():
    print("=== 开始合并数据 ===")
    
    # 1. 读取文件
    df_target = load_file_auto_encoding(TARGET_FILE)
    df_finviz = load_file_auto_encoding(FINVIZ_FILE)

    # 2. 识别代码列并标准化
    # Target 文件
    target_ticker_col = None
    for col in ['代码', 'Ticker', 'Symbol']:
        if col in df_target.columns:
            target_ticker_col = col
            break
    if not target_ticker_col:
        print(f"[错误] {TARGET_FILE} 中找不到股票代码列。")
        sys.exit(1)
        
    # Finviz 文件
    finviz_ticker_col = None
    for col in ['Ticker', 'Symbol']:
        if col in df_finviz.columns:
            finviz_ticker_col = col
            break
    if not finviz_ticker_col:
        print(f"[错误] {FINVIZ_FILE} 中找不到股票代码列(Ticker)。")
        sys.exit(1)

    # 创建匹配用的辅助列 (大写去空格)
    df_target['Match_Ticker'] = df_target[target_ticker_col].astype(str).str.upper().str.strip()
    df_finviz['Match_Ticker'] = df_finviz[finviz_ticker_col].astype(str).str.upper().str.strip()

    # 3. 准备要合并的 Finviz 数据
    # 只保留我们需要的列，防止列名冲突
    # 通常我们需要: Sector, Industry, Country, Market Cap
    cols_to_use = ['Match_Ticker']
    rename_map = {}
    
    if 'Sector' in df_finviz.columns:
        cols_to_use.append('Sector')
        rename_map['Sector'] = '一级行业(Sector)'
    if 'Industry' in df_finviz.columns:
        cols_to_use.append('Industry')
        rename_map['Industry'] = '二级行业(Industry)'
    if 'Country' in df_finviz.columns:
        cols_to_use.append('Country')
        rename_map['Country'] = '国家'
    if 'Market Cap' in df_finviz.columns:
        cols_to_use.append('Market Cap')
        rename_map['Market Cap'] = '市值(Finviz)'

    df_finviz_clean = df_finviz[cols_to_use].copy()
    df_finviz_clean.rename(columns=rename_map, inplace=True)
    
    # 去重：防止 Finviz 表里有重复代码导致数据膨胀
    df_finviz_clean.drop_duplicates(subset=['Match_Ticker'], inplace=True)

    # 4. 执行合并 (Left Join)
    # 保留 target.csv 的所有行，匹配不上的显示为空
    df_merged = pd.merge(df_target, df_finviz_clean, on='Match_Ticker', how='left')

    # 5. 填充缺失值
    if '一级行业(Sector)' in df_merged.columns:
        df_merged['一级行业(Sector)'].fillna('Unknown', inplace=True)
    if '二级行业(Industry)' in df_merged.columns:
        df_merged['二级行业(Industry)'].fillna('Unknown', inplace=True)

    # 6. 清理和排序
    # 删除辅助列
    if 'Match_Ticker' in df_merged.columns:
        df_merged.drop(columns=['Match_Ticker'], inplace=True)
    
    # 排序逻辑：一级行业 -> 二级行业 -> 市值(降序) -> 代码
    sort_cols = []
    ascending_order = []
    
    if '一级行业(Sector)' in df_merged.columns:
        sort_cols.append('一级行业(Sector)')
        ascending_order.append(True)
    
    if '二级行业(Industry)' in df_merged.columns:
        sort_cols.append('二级行业(Industry)')
        ascending_order.append(True)
        
    # 如果有市值数据，按市值降序排（让大市值的排前面）
    if '市值(Finviz)' in df_merged.columns:
        sort_cols.append('市值(Finviz)')
        ascending_order.append(False)
    elif '总市值' in df_merged.columns: # 使用原表市值作为备选
        sort_cols.append('总市值')
        ascending_order.append(False)
        
    sort_cols.append(target_ticker_col)
    ascending_order.append(True)

    df_merged.sort_values(by=sort_cols, ascending=ascending_order, inplace=True)

    # 7. 保存输出
    try:
        df_merged.to_excel(OUTPUT_FILE, index=False)
        print(f"\n[完成] 合并成功！")
        print(f"输出文件: {OUTPUT_FILE}")
        print(f"共处理: {len(df_merged)} 行")
        
        # 简单统计匹配情况
        if '一级行业(Sector)' in df_merged.columns:
            matched_count = len(df_merged[df_merged['一级行业(Sector)'] != 'Unknown'])
            print(f"成功匹配行业信息的股票数: {matched_count} / {len(df_merged)}")
            
    except Exception as e:
        print(f"[错误] 保存文件失败 (可能文件被占用): {e}")

if __name__ == "__main__":
    main()