from airflow import DAG
from pendulum import datetime
from tasks.extract import extract_lookup_premise, extract_lookup_item, extract_pricecatcher_full
from tasks.datawarehouse import dim_item, dim_premise, fact_pricecatcher_full


with DAG(
    dag_id="full_load",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["full_load"],
) as dag:
    
    extract_tasks = [
        extract_lookup_premise(),
        extract_lookup_item(),
        extract_pricecatcher_full(),
    ]
    
    dim_tasks = [
        dim_item(),
        dim_premise(),
    ]
    
    fact = fact_pricecatcher_full()
    
    for extract_task in extract_tasks:
        for dim_task in dim_tasks:
            extract_task >> dim_task
    
    for dim_task in dim_tasks:
        dim_task >> fact