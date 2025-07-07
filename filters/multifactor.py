from filters.momentum import run_momentum_filter
from filters.rs import run_rs_filter
from filters.accumulation import run_accumulation_filter
from filters.price_volume_signal import run_price_volume_filter
import pandas as pd

def run_multifactor_filter(df: pd.DataFrame, index_df: pd.DataFrame) -> pd.DataFrame:
    # Apply all four filters
    momentum = run_momentum_filter(df)
    rs = run_rs_filter(df, index_df)
    accumulation = run_accumulation_filter(df)
    price_action = run_price_volume_filter(df)

    # Compute intersection of symbols
    common_symbols = (
        set(momentum["Symbol"]) &
        set(rs["Symbol"]) &
        set(accumulation["Symbol"]) &
        set(price_action["Symbol"])
    )

    # Create result DataFrame
    result = pd.DataFrame(sorted(common_symbols), columns=["Symbol"])

    # Merge with reference data for Sector and Mktcap
    try:
        ref_df = pd.read_excel("data_ref/NSE_Stocks.xlsx", sheet_name=0)
        ref_df = ref_df[["Symbol", "Sector", "Mktcap"]].drop_duplicates()
        result = result.merge(ref_df, on="Symbol", how="left")
    except Exception as e:
        print("⚠️ Could not load reference Sector/Mktcap data:", e)

    return result
