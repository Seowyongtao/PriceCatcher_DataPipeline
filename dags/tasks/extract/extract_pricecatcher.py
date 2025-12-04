from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import pendulum as p
from datetime import timedelta
from sqlalchemy import text


@task
def extract_pricecatcher():
    """Incremental extract: delete and reload the past 10 days of data."""

    # Use current date (today) when the task runs
    today = p.now()
    
    # Calculate date range: past 10 days 
    end_date = today.date()
    start_date = (today - timedelta(days=9)).date()
    
    print("Starting task: extract_pricecatcher (incremental)")
    print(f"Date range: {start_date} to {end_date} (10 days)")
    print(f"Today's date: {today.date()}")
    
    hook = PostgresHook(postgres_conn_id="raw_db_postgres")
    engine = hook.get_sqlalchemy_engine()
    
    # Delete data for the past 10 days from raw database
    print(f"\nDeleting data for date range: {start_date} to {end_date}")
    
    delete_query = text("""
    DELETE FROM pricecatcher_data
    WHERE date >= :start_date AND date <= :end_date
    """)
    
    with engine.begin() as conn:
        result = conn.execute(delete_query, {"start_date": start_date, "end_date": end_date})
        deleted_rows = result.rowcount
    
    print(f"Deleted {deleted_rows:,} rows from pricecatcher_data table")
    
    # Determine which months we need to download based on date range
    start_month = p.datetime(start_date.year, start_date.month, 1)
    end_month = p.datetime(end_date.year, end_date.month, 1)
    
    months = []
    cursor = start_month
    while cursor <= end_month:
        months.append(f"{cursor.year:04d}-{cursor.month:02d}")
        cursor = cursor.add(months=1)
    
    print(f"\nMonths to process: {', '.join(months)}")
    
    # Download and filter data for the date range
    chunk_size = 25000
    total_processed = 0
    
    for month_idx, month in enumerate(months):
        print(f"\nProcessing month: {month} ({month_idx + 1}/{len(months)})")
        
        URL_DATA = f"https://storage.data.gov.my/pricecatcher/pricecatcher_{month}.parquet"
        
        try:
            print(f"Downloading data from {URL_DATA}")
            df = pd.read_parquet(URL_DATA)
            print(f"Total rows for {month}: {len(df):,}")
            
            # Process data in chunks (filter during chunking to save memory)
            month_processed = 0
            total_chunks = (len(df) + chunk_size - 1) // chunk_size
            
            for i in range(0, len(df), chunk_size):
                chunk_num = i // chunk_size + 1
                print(f"\n{month} - Chunk {chunk_num}/{total_chunks}")
                
                # Process chunk
                chunk = df.iloc[i : i + chunk_size].copy()
                print(f"Chunk size: {len(chunk):,} rows")
                
                # Process date column and filter on the chunk (not the entire df)
                if "date" in chunk.columns:
                    chunk["date"] = pd.to_datetime(chunk["date"])
                    # Filter to only include rows within our date range
                    chunk = chunk[
                        (chunk["date"].dt.date >= start_date)
                        & (chunk["date"].dt.date <= end_date)
                    ]
                    print(f"Filtered chunk: {len(chunk):,} rows match date range")
                else:
                    print("Warning: No 'date' column found, skipping date filtering")
                
                # Skip empty chunks after filtering
                if len(chunk) == 0:
                    print("No data in date range for this chunk, skipping...")
                    del chunk
                    continue
                
                # Add source_month column to track which month the data came from
                chunk["source_month"] = month
                
                chunk.to_sql("pricecatcher_data", engine, if_exists="append", index=False)
                
                month_processed += len(chunk)
                total_processed += len(chunk)
                
                print("Inserted chunk into database")
                print(f"Month progress: {month_processed:,} rows processed")
                print(f"Overall progress: {total_processed:,} total rows processed")
                
                # Free memory
                del chunk
            
            print(f"\nCompleted {month}: {month_processed:,} rows processed")
            
            # Free the monthly DataFrame
            del df
        
        except Exception as e:
            # Stop immediately if any month fails.
            raise
    
    print("\nCompleted incremental load")
    print(f"Date range processed: {start_date} to {end_date}")
    print(f"Total rows processed: {total_processed:,}")
    print(f"Months processed: {', '.join(months)}")
