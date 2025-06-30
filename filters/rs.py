import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame, rs_period: int = 252) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Merge benchmark index
    df = df.merge(
        index_df[["Timestamp", "Close"]].rename(columns={"Close": "Benchmark_Close"}),
        on="Timestamp", how="left"
    )

    # Compute RS
    df["RS"] = (
        100 * (1 + (df["Close"] / df["Close"].shift(rs_period) - 1)) /
        (1 + (df["Benchmark_Close"] / df["Benchmark_Close"].shift(rs_period) - 1))
    )

    # Filter outperformers
    rs_outperformers = df[df["RS"] > 105].copy()

    # Extract last 10 RS values per symbol
    last_10_rs = (
        rs_outperformers
        .sort_values(["Symbol", "Timestamp"])
        .groupby("Symbol")
        .tail(10)
        .groupby("Symbol")["RS"]
        .apply(lambda x: list(x)[-10:])
        .reset_index()
    )

    # Convert RS lists into columns
    rs_df = pd.DataFrame(
        last_10_rs["RS"].to_list(),
        columns=[f"RS_T-{9-i}" for i in range(10)]
    )
    rs_df.insert(0, "Symbol", last_10_rs["Symbol"])

    return rs_df
