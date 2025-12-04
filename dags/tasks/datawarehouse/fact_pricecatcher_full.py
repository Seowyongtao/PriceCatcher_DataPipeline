from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import hashlib


@task
def fact_pricecatcher_full():
    import pandas as pd

    # Database connections
    raw_hook = PostgresHook(postgres_conn_id="raw_db_postgres")
    raw_engine = raw_hook.get_sqlalchemy_engine()
    
    dw_hook = PostgresHook(postgres_conn_id="datawarehouse_db_postgres")
    dw_engine = dw_hook.get_sqlalchemy_engine()
    
    print("Starting fact_pricecatcher task...")
    
    chunk_size = 25000  # Process 25K rows at a time
    offset = 0
    total_processed = 0
    
    while True:
        # Read chunk with LIMIT and OFFSET
        query = f"""
        SELECT date, premise_code, item_code, price
        FROM pricecatcher_data
        ORDER BY date, premise_code, item_code
        LIMIT {chunk_size} OFFSET {offset}
        """
        
        df = pd.read_sql(query, raw_engine)
        
        if len(df) == 0:
            break  # No more data
            
        # Add hash columns
        df['item_hashid'] = df['item_code'].apply(
            lambda x: hashlib.sha256(str(x).encode()).hexdigest()
        )
        df['premise_hashid'] = df['premise_code'].apply(
            lambda x: hashlib.sha256(str(x).encode()).hexdigest()
        )
        
        # Insert chunk
        if_exists_mode = "replace" if offset == 0 else "append"
        
        df.to_sql("fact_pricecatcher", dw_engine, if_exists=if_exists_mode, index=False)

        total_processed += len(df)
        print(f"Processed chunk: {len(df):,} rows (Total: {total_processed:,})")
        
        offset += chunk_size
        del df  # Free memory
    
    print(f"fact_pricecatcher completed: {total_processed:,} total rows")

