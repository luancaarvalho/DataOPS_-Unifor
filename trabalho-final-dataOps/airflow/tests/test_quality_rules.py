import pandas as pd

def test_not_null():
    df = pd.read_csv("data/raw/covid_data.csv")
    assert not df.isnull().any().any(), "Existem valores nulos no dataset"

def test_positive_confirmed():
    df = pd.read_csv("data/raw/covid_data.csv")
    assert (df["confirmed"] >= 0).all(), "Casos confirmados negativos detectados"
