import requests
import pandas as pd
import os


def fetch_covid_data():
    url = "https://brasil.io/covid19/cities/cases/"
    output_dir = "/opt/airflow/data/raw"
    os.makedirs(output_dir, exist_ok=True)

    response = requests.get(url)
    response.raise_for_status()

    data_json = response.json()

    if "cities" not in data_json:
        raise ValueError("Estrutura inesperada da API. Chave 'cities' não encontrada.")

    df = pd.DataFrame(list(data_json["cities"].values()))

    df = df[df["state"] == "CE"]

    df.to_csv(f"{output_dir}/covid_data.csv", index=False)
    print(
        f"Dados baixados e salvos em {output_dir}/covid_data.csv ({len(df)} registros)"
    )


if __name__ == "__main__":
    fetch_covid_data()
