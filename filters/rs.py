import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    # Sort data
    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Merge benchmark close into stock data
    df = df.merge(
        index_df[["Timestamp", "Close"]].rename(columns={"Close": "Benchmark_Close"}),
        on="Timestamp", how="left"
    )

    # Calculate RS and RS Ratio
    df["RS"] = df["Close"] / df["Benchmark_Close"]
    df["RS_SMA"] = df.groupby("Symbol")["RS"].transform(lambda x: x.rolling(window=20, min_periods=1).mean())
    df["RS_Ratio"] = df["RS"] / df["RS_SMA"]

    # Filter stocks with RS_Ratio > 1.05
    shortlisted = df[df["RS_Ratio"] > 1.05].copy()

    # Take last 10 RS values for each shortlisted symbol
    recent_rs = (
        shortlisted.sort_values(["Symbol", "Timestamp"])
        .groupby("Symbol")
        .tail(10)
        .groupby("Symbol")[["RS", "Timestamp"]]
        .apply(lambda x: x.set_index("Timestamp").RS.tail(10))
        .unstack()
        .reset_index()
    )

    # Format columns with actual dates
    recent_rs.columns = ["Symbol"] + [ts.strftime("%d-%b") for ts in recent_rs.columns[1:]]

    return recent_rs
