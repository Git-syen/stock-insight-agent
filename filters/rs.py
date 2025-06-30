import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame, rs_period: int = 252) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Merge index data
    df = df.merge(index_df[["Timestamp", "Close"]].rename(columns={"Close": "Benchmark_Close"}), on="Timestamp", how="left")

    # RS Calculation
    df["RS"] = (
        100 * (1 + (df["Close"] / df["Close"].shift(rs_period) - 1)) /
        (1 + (df["Benchmark_Close"] / df["Benchmark_Close"].shift(rs_period) - 1))
    )

    # Filter where RS > 105
    rs_outperformers = df[df["RS"] > 105].copy()

    # Get last 10 RS values per Symbol
    recent_rs = (
        rs_outperformers
        .groupby("Symbol")
        .apply(lambda x: x.sort_values("Timestamp").tail(10)["RS"].reset_index(drop=True))
        .unstack()
        .reset_index()
    )

    # Rename RS columns as RS_T-9, RS_T-8, ..., RS_T
    recent_rs.columns = ["Symbol"] + [f"RS_T-{9-i}" for i in range(10)]

    return recent_rs
