from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum as p


@task
def extract_pricecatcher_full():
    import pandas as pd

    hook = PostgresHook(postgres_conn_id="raw_db_postgres")
    engine = hook.get_sqlalchemy_engine()

    # Build list of months from 2025-11 through current month
    start_month = p.datetime(2025, 11, 1)
    now = p.now()
    end_month = p.datetime(now.year, now.month, 1)

    months = []
    cursor = start_month
    while cursor <= end_month:
        months.append(f"{cursor.year:04d}-{cursor.month:02d}")
        cursor = cursor.add(months=1)

    print("Starting extract_pricecatcher_full")
    print(f"Month range: {months[0]} to {months[-1]} ({len(months)} months)")

    # Process in chunks of 25,000 rows
    chunk_size = 25000
    total_processed_all_months = 0

    for month_idx, month in enumerate(months):
        print(f"\nProcessing month {month} ({month_idx + 1}/{len(months)})")

        URL_DATA = f"https://storage.data.gov.my/pricecatcher/pricecatcher_{month}.parquet"

        try:
            print(f"Downloading data from {URL_DATA}")
            df = pd.read_parquet(URL_DATA)
            print(f"Total rows for {month}: {len(df):,}")

            # Process data in chunks
            month_processed = 0

            for i in range(0, len(df), chunk_size):
                chunk_num = i // chunk_size + 1
                total_chunks = (len(df) + chunk_size - 1) // chunk_size

                print(f"  Chunk {chunk_num}/{total_chunks}")

                # Process chunk
                chunk = df.iloc[i : i + chunk_size].copy()
                print(f"  Chunk size: {len(chunk):,} rows")

                # Process date column if it exists
                if "date" in chunk.columns:
                    chunk["date"] = pd.to_datetime(chunk["date"])

                # Add source_month column to track which month the data came from
                chunk["source_month"] = month

                # Write chunk to database
                if month_idx == 0 and i == 0:
                    # First chunk of first month - replace table
                    if_exists_mode = "replace"
                else:
                    # All other chunks - append to table
                    if_exists_mode = "append"

                chunk.to_sql(
                    "pricecatcher_data",
                    engine,
                    if_exists=if_exists_mode,
                    index=False,
                )

                month_processed += len(chunk)
                total_processed_all_months += len(chunk)

                print(
                    f"  Month progress: {month_processed:,}/{len(df):,} rows "
                    f"({month_processed/len(df)*100:.1f}%)"
                )

                # Free memory 
                del chunk

            print(f"Completed {month}: {month_processed:,} rows processed")

            # Free the monthly DataFrame
            del df

        except Exception as e:
            raise

    print("\nFinished all requested months")
    print(f"Total rows processed across all months: {total_processed_all_months:,}")
    print(f"Months processed: {', '.join(months)}")

