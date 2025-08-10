import pandas as pd
import re
from filters.momentum_mo import run_monthly_momentum_filter
from filters.rs_mo import run_monthly_rs_filter
from filters.accumulation_mo import run_monthly_accumulation_filter
from filters.price_volume_signal_mo import run_monthly_price_volume_filter
from filters.multifactor_mo import run_monthly_multifactor_filter
from notion_sync import update_notion

def safe_filename(name):
    return re.sub(r'[^a-zA-Z0-9_]', '', name.replace(' ', '_')).lower()

# Load monthly data
df = pd.read_parquet("data_monthly/all_stocks.parquet")
index_df = pd.read_parquet("data_monthly/nifty.parquet")

# Normalize column names
df.columns = [col.capitalize() for col in df.columns]
index_df.columns = [col.capitalize() for col in index_df.columns]

# Use NIFTY as benchmark
benchmark_df = index_df[index_df["Symbol"] == "NIFTY"].copy()

# Run monthly filters
filters = {
    "Momentum Stocks – Monthly": run_monthly_momentum_filter(df),
    "RS Outperformers – Monthly": run_monthly_rs_filter(df, benchmark_df),
    "Accumulating Stocks – Monthly": run_monthly_accumulation_filter(df),
    "Price Action Volume Spike – Monthly": run_monthly_price_volume_filter(df),
    "Multi-Factor Picks – Monthly": run_monthly_multifactor_filter(df, benchmark_df),
}

# Save outputs and sync to Notion
for name, result_df in filters.items():
    filename = f"outputs_monthly/{safe_filename(name)}.csv"
    result_df.to_csv(filename, index=False)
    update_notion(name, f"https://github.com/Git-syen/stock-insight-agent/tree/main/{filename}")
