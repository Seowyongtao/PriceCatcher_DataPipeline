from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import hashlib


@task
def dim_premise():
    import pandas as pd

    # Connect to raw database to read lookup_premise
    raw_hook = PostgresHook(postgres_conn_id="raw_db_postgres")
    raw_engine = raw_hook.get_sqlalchemy_engine()
    
    # Connect to data warehouse database to write dim_premise
    dw_hook = PostgresHook(postgres_conn_id="datawarehouse_db_postgres")
    dw_engine = dw_hook.get_sqlalchemy_engine()
    
    # Read data from lookup_premise table
    query = """
    SELECT premise_code,premise,address,premise_type,state,district
    FROM lookup_premise
    """
    
    df = pd.read_sql(query, raw_engine)
    
    if 'premise_code' in df.columns:
        df['premise_code'] = pd.to_numeric(df['premise_code'], errors='coerce').astype('Int64')
    
    # Add premise_hashid column (SHA256 hash of premise_code)
    df['premise_hashid'] = df['premise_code'].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()
    )
    
    # Insert transformed data into dim_premise table
    df.to_sql("dim_premise", dw_engine, if_exists="replace", index=False)
    print("Inserted into datawarehouse_db.dim_premise")

