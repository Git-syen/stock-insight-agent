import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame, rs_period: int = 252) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    # Align the index for merge
    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Merge benchmark index close to each stock row based on Timestamp
    df = df.merge(
        index_df[["Timestamp", "Close"]].rename(columns={"Close": "Benchmark_Close"}),
        on="Timestamp", how="left"
    )

    # Calculate RS
    df["RS"] = (
        100 * (1 + (df["Close"] / df["Close"].shift(rs_period) - 1)) /
        (1 + (df["Benchmark_Close"] / df["Benchmark_Close"].shift(rs_period) - 1))
    )

    # Filter outperformers
    rs_outperformers = df[df["RS"] > 105]

    # Get last 10 entries per symbol
    last_10 = (
        rs_outperformers
        .sort_values(by=["Symbol", "Timestamp"])
        .groupby("Symbol")
        .tail(10)[["Symbol", "Timestamp", "RS"]]
        .reset_index(drop=True)
    )

    return last_10
