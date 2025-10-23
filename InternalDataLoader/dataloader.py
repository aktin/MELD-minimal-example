import pandas as pd

from .db import engine

def get_results(sql: str) -> pd.DataFrame:
    with engine.connect() as connection:
        df = pd.read_sql(sql, connection)

    return df
