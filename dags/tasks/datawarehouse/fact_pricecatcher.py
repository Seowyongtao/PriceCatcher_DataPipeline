from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import hashlib
import pendulum as p
from datetime import timedelta
from sqlalchemy import text


@task
def fact_pricecatcher():
    """Incremental fact load: delete and reload the past 10 days of data."""
    # Database connections
    raw_hook = PostgresHook(postgres_conn_id="raw_db_postgres")
    raw_engine = raw_hook.get_sqlalchemy_engine()
    
    dw_hook = PostgresHook(postgres_conn_id="datawarehouse_db_postgres")
    dw_engine = dw_hook.get_sqlalchemy_engine()
    
    print("Starting fact_pricecatcher task (incremental)")
    
    # Use current date (today) when the task runs
    today = p.now()
    
    # Calculate date range: past 10 days 
    end_date = today.date()
    start_date = (today - timedelta(days=9)).date()
    
    print(f"Date range: {start_date} to {end_date} (10 days)")
    print(f"Today's date: {today.date()}")
    
    # Delete data for the past 10 days from fact table
    print(f"\nDeleting data from fact_pricecatcher for date range: {start_date} to {end_date}")
    
    delete_query = text("""
    DELETE FROM fact_pricecatcher
    WHERE date >= :start_date AND date <= :end_date
    """)
    
    with dw_engine.begin() as conn:
        result = conn.execute(delete_query, {"start_date": start_date, "end_date": end_date})
        deleted_rows = result.rowcount
    
    print(f"Deleted {deleted_rows:,} rows from fact_pricecatcher table")
    
    # Process data in chunks for the date range
    chunk_size = 25000  # Process 25K rows at a time
    offset = 0
    total_processed = 0
    
    while True:
        # Read chunk with LIMIT and OFFSET, filtered by date range
        query = text("""
        SELECT date, premise_code, item_code, price
        FROM pricecatcher_data
        WHERE date >= :start_date AND date <= :end_date
        ORDER BY date, premise_code, item_code
        LIMIT :chunk_size OFFSET :offset
        """)
        
        # Execute with parameters
        with raw_engine.connect() as conn:
            result = conn.execute(
                query, 
                {"start_date": start_date, "end_date": end_date, "chunk_size": chunk_size, "offset": offset}
            )
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        if len(df) == 0:
            break  # No more data
            
        # Add hash columns
        df['item_hashid'] = df['item_code'].apply(
            lambda x: hashlib.sha256(str(x).encode()).hexdigest()
        )
        df['premise_hashid'] = df['premise_code'].apply(
            lambda x: hashlib.sha256(str(x).encode()).hexdigest()
        )
        
        
        df.to_sql("fact_pricecatcher", dw_engine, if_exists="append", index=False)

        total_processed += len(df)
        print(f"Processed chunk: {len(df):,} rows (Total: {total_processed:,})")
        
        offset += chunk_size
        del df  # Free memory
    
    print("\nfact_pricecatcher (incremental) completed")
    print(f"Date range processed: {start_date} to {end_date}")
    print(f"Total rows processed: {total_processed:,}")

