import pandas as pd
from logs.logger import Logger

log = Logger("etl.transformer")


class Transformer:
    def __init__(self):
       pass

    def bcb_selic_to_history_series(self, df: pd.DataFrame) -> pd.DataFrame:
        df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")

        df_t = df.loc[df["valor"].ne(df["valor"].shift())].reset_index(drop=True)

        return df_t