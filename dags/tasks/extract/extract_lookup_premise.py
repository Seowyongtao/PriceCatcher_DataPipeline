from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task
def extract_lookup_premise():
    import pandas as pd

    hook = PostgresHook(postgres_conn_id="raw_db_postgres")
    engine = hook.get_sqlalchemy_engine()

    URL_DATA = 'https://storage.data.gov.my/pricecatcher/lookup_premise.parquet'

    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns: 
        df['date'] = pd.to_datetime(df['date'])

    if 'premise_code' in df.columns:
        df['premise_code'] = pd.to_numeric(df['premise_code'], errors='coerce').astype('Int64')

    df.to_sql("lookup_premise", engine, if_exists="replace", index=False)
    print("Inserted into raw_db.lookup_premise")

