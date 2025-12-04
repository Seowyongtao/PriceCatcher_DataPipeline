from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import hashlib


@task
def dim_item():
    import pandas as pd

    # Connect to raw database to read lookup_item
    raw_hook = PostgresHook(postgres_conn_id="raw_db_postgres")
    raw_engine = raw_hook.get_sqlalchemy_engine()
    
    # Connect to data warehouse database to write dim_item
    dw_hook = PostgresHook(postgres_conn_id="datawarehouse_db_postgres")
    dw_engine = dw_hook.get_sqlalchemy_engine()
    
    # Read data from lookup_item table
    query = """
    SELECT item_code, item, unit, item_group, item_category
    FROM lookup_item
    """
    
    df = pd.read_sql(query, raw_engine)
    
    # Add item_hashid column (SHA256 hash of item_code)
    df['item_hashid'] = df['item_code'].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()
    )
    
    # Insert transformed data into dim_item table
    df.to_sql("dim_item", dw_engine, if_exists="replace", index=False)
    print("Inserted into datawarehouse_db.dim_item")


