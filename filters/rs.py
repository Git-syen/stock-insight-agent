import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame, rs_period: int = 252) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    # Sort both DataFrames
    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Calculate RS and RS Ratio
    #df["RS"] = index_df["Close"] / index_df["Close"].shift(rs_period)
    df = df.merge(index_df[["Timestamp", "Close"]].rename(columns={"Close": "Benchmark_Close"}), on="Timestamp", how="left")
    #df["RS"] = 100 * (1+(df["Close"] / df["Benchmark_Close"]-1))/(1+(df["Close"].shift(rs_period) / df["Benchmark_Close"].shift(rs_period)-1))

    df["RS"] = 100 * (1+(df["Close"] / df["Close"].shift(rs_period)-1))/(1+(df["Benchmark_Close"] / df["Benchmark_Close"].shift(rs_period)-1))

    #df["RS"] = 100 * (1+(df["Close"] / df["Close"].shift(rs_period)-1))/(1+(index_df["Close"] / index_df["Close"].shift(rs_period)-1))


    # Filter strong RS symbols
    filtered = df[df["RS"] > 105].copy()
    #filtered = df

    # Ensure single RS value per Symbol per Timestamp by taking the last
    filtered = (
        filtered.sort_values("Timestamp")
        .groupby(["Symbol", "Timestamp"], as_index=False)
        .last()
    )

    # Proceed to get last 10 dates' RS per symbol
    recent_rs = (
        filtered
        .sort_values(["Symbol", "Timestamp"])
        .groupby("Symbol")
        .tail(10)
        .set_index(["Symbol", "Timestamp"])["RS"]
        .unstack()
        .reset_index()
    )

    # Sort date columns and rename
    date_cols = sorted(recent_rs.columns[1:], key=lambda x: pd.to_datetime(x))
    recent_rs.columns = ["Symbol"] + [d.strftime("%d-%b") for d in date_cols]
    recent_rs = recent_rs[["Symbol"] + [d.strftime("%d-%b") for d in date_cols]]



    return recent_rs
