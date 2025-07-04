import pandas as pd
import re
from filters.momentum_wk import run_weekly_momentum_filter
from filters.rs_wk import run_weekly_rs_filter
from filters.accumulation_wk import run_weekly_accumulation_filter
from filters.price_volume_signal_wk import run_weekly_price_volume_filter
from filters.multifactor_wk import run_weekly_multifactor_filter
from notion_sync import update_notion

def safe_filename(name):
    return re.sub(r'[^a-zA-Z0-9_]', '', name.replace(' ', '_')).lower()

# Load weekly data
df = pd.read_parquet("data_weekly/all_stocks.parquet")
index_df = pd.read_parquet("data_weekly/nifty.parquet")

# Normalize column names
df.columns = [col.capitalize() for col in df.columns]
index_df.columns = [col.capitalize() for col in index_df.columns]

# Use NIFTY as benchmark
benchmark_df = index_df[index_df["Symbol"] == "NIFTY"].copy()

# Run weekly filters
filters = {
    "Momentum Stocks – Weekly": run_weekly_momentum_filter(df),
    "RS Outperformers – Weekly": run_weekly_rs_filter(df, benchmark_df),
    "Accumulating Stocks – Weekly": run_weekly_accumulation_filter(df),
    "Price Action Volume Spike – Weekly": run_weekly_price_volume_filter(df),
    "Multi-Factor Picks – Weekly": run_weekly_multifactor_filter(df, benchmark_df),
}

# Save outputs and sync to Notion
for name, result_df in filters.items():
    filename = f"outputs_weekly/{safe_filename(name)}.csv"
    result_df.to_csv(filename, index=False)
    update_notion(name, f"https://github.com/Git-syen/stock-insight-agent/tree/main/{filename}")
