import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame, rs_period: int = 252) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Merge index
    df = df.merge(
        index_df[["Timestamp", "Close"]].rename(columns={"Close": "Benchmark_Close"}),
        on="Timestamp", how="left"
    )

    # Shift close values per symbol and benchmark
    df["Close_shifted"] = df.groupby("Symbol")["Close"].shift(rs_period)
    df["Benchmark_shifted"] = df["Benchmark_Close"].shift(rs_period)

    # RS Calculation
    df["RS"] = (
        100 * (1 + (df["Close"] / df["Close_shifted"] - 1)) /
        (1 + (df["Benchmark_Close"] / df["Benchmark_shifted"] - 1))
    )

    # Filter outperformers
    rs_outperformers = df[df["RS"] > 105].copy()

    # Extract last 10 RS values per symbol with dates
    def extract_recent_rs(group):
        recent = group[["Timestamp", "RS"]].tail(10).reset_index(drop=True)
        return recent.set_index("Timestamp")["RS"]

    recent_rs = (
        rs_outperformers
        .sort_values(["Symbol", "Timestamp"])
        .groupby("Symbol")
        .apply(extract_recent_rs)
        .unstack()
        .reset_index()
    )

    # Format date columns
    recent_rs.columns = ["Symbol"] + [ts.strftime("%d-%b") for ts in recent_rs.columns[1:]]

    return recent_rs
