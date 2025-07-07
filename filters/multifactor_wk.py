from filters.momentum_wk import run_weekly_momentum_filter
from filters.rs_wk import run_weekly_rs_filter
from filters.accumulation_wk import run_weekly_accumulation_filter
from filters.price_volume_signal_wk import run_weekly_price_volume_filter
import pandas as pd

def run_weekly_multifactor_filter(df: pd.DataFrame, index_df: pd.DataFrame) -> pd.DataFrame:
    # Apply all four weekly filters
    momentum = run_weekly_momentum_filter(df)
    rs = run_weekly_rs_filter(df, index_df)
    accumulation = run_weekly_accumulation_filter(df)
    price_action = run_weekly_price_volume_filter(df)

    # Get intersection of symbols passing all filters
    common_symbols = (
        set(momentum['Symbol']) &
        set(rs['Symbol']) &
        set(accumulation['Symbol']) &
        set(price_action['Symbol'])
    )

    # Convert to DataFrame
    result_df = pd.DataFrame(sorted(common_symbols), columns=["Symbol"])

    # Merge Sector and Mktcap from reference file
    try:
        ref_df = pd.read_excel("data_ref/NSE_Stocks.xlsx", sheet_name=0)
        ref_df = ref_df[["Symbol", "Sector", "Mktcap"]].drop_duplicates()
        result_df = result_df.merge(ref_df, on="Symbol", how="left")
    except Exception as e:
        print("⚠️ Sector/Mktcap merge failed:", e)

    return result_df
