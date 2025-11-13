import pandas as pd
import os

def transform_data():
    df = pd.read_csv("/opt/airflow/data/raw/covid_data.csv")
    df = df[["city", "date", "confirmed", "deaths"]].dropna()
    df["date"] = pd.to_datetime(df["date"])
    agg = df.groupby(["city", pd.Grouper(key="date", freq="D")]).sum().reset_index()
    agg["rolling_avg"] = agg.groupby("city")["confirmed"].transform(
        lambda x: x.rolling(7, 1).mean()
    )
    os.makedirs("/opt/airflow/data/processed", exist_ok=True)
    agg.to_csv("/opt/airflow/data/processed/covid_transformed.csv", index=False)
    print("Dados transformados e salvos em /data/processed/covid_transformed.csv")
    return agg
