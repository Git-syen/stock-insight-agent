import pandas as pd
from filters.momentum import run_momentum_filter
from filters.rs import run_rs_filter
from filters.accumulation import run_accumulation_filter
from filters.price_volume_signal import run_price_volume_filter
from filters.multifactor import run_multifactor_filter
from notion_sync import update_notion

# Load data
df = pd.read_parquet("data/all_stocks.parquet")
index_df = pd.read_parquet("data/nifty.parquet")

# Normalize column names
df.columns = [col.capitalize() for col in df.columns]
index_df.columns = [col.capitalize() for col in index_df.columns]

# Use NIFTY as benchmark
benchmark_df = index_df[index_df["Symbol"] == "NIFTY"].copy()

# Run filters
filters = {
    "Momentum Stocks": run_momentum_filter(df),
    "RS Outperformers": run_rs_filter(df, benchmark_df),
    "Accumulating Stocks": run_accumulation_filter(df),
    "Price Action Volume Spike": run_price_volume_filter(df),
    "Multi-Factor Picks": run_multifactor_filter(df, benchmark_df),
}

# Save and sync to Notion
for name, result_df in filters.items():
    filename = f"outputs/{name.replace(' ', '_').lower()}.csv"
    result_df.to_csv(filename, index=False)
    update_notion(name, f"https://github.com/Git-syen/stock-insight-agent/tree/main/outputs/{filename}")
