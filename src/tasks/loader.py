import duckdb
from pathlib import Path
import pandas as pd

from logs.logger import Logger

class Loader:
    def __init__(self):
        current_dir = Path(__file__).parent
        relative_db_path = Path("../../data/financial_data.duckdb")
        path = (current_dir / relative_db_path).resolve()

        print(f"Connecting to duckDB at {path}...")

        self.conn = duckdb.connect(path)

    def close(self):
        self.conn.close()

    def __temp_table(self, table: str) -> str:
        return f"temp_{table}"

    def __create_schema(self, table: str, df: pd.DataFrame):
        self.conn.register(self.__temp_table(table), df)
        self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM temp_{table}")

    def load_news(self, df: pd.DataFrame):
        self.write_df("news", df)

    def load_monetary_history(self, df: pd.DataFrame):
        self.write_df("monetary_history", df)

    def load_selic_history(self, df: pd.DataFrame):
        self.write_df("selic_history", df)
    
    def load_selic(self, df: pd.DataFrame):
        self.write_df("selic", df)

    def write_df(self, table: str, df: pd.DataFrame):
        print(f"Loading dataframe to duckDB: {table}...")
        
        self.__create_schema(table, df)

        self.conn.execute(f"INSERT INTO {table} SELECT * FROM {self.__temp_table(table)}")

        print("Succesfully loaded data to duckDB!")
