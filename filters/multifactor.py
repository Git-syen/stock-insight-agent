from filters.momentum import run_momentum_filter
from filters.rs import run_rs_filter
from filters.accumulation import run_accumulation_filter
from filters.price_volume_signal import run_price_volume_filter
import pandas as pd

def run_multifactor_filter(df: pd.DataFrame, index_df: pd.DataFrame) -> pd.DataFrame:
    # Get filtered symbol lists from all filters
    momentum = run_momentum_filter(df)
    rs = run_rs_filter(df, index_df)
    accumulation = run_accumulation_filter(df)
    price_action = run_price_volume_filter(df)

    # Get intersection of all 4 filters (common stocks)
    common_symbols = (
        set(momentum['Symbol']) &
        set(rs['Symbol']) &
        set(accumulation['Symbol']) &
        set(price_action['Symbol'])
    )

    # Return only those symbols
    return pd.DataFrame(sorted(common_symbols), columns=["Symbol"])
