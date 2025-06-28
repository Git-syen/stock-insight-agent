from filters.momentum import run_momentum_filter
from filters.rs import run_rs_filter
from filters.accumulation import run_accumulation_filter

def run_multifactor_filter(df, index_df):
    m_df = run_momentum_filter(df)
    r_df = run_rs_filter(df, index_df)
    a_df = run_accumulation_filter(df)

    symbols = set(m_df['Symbol']) & set(r_df['Symbol']) & set(a_df['Symbol'])
    return df[df['Symbol'].isin(symbols)].copy()