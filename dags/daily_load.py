from airflow import DAG
from pendulum import datetime
from tasks.extract import extract_lookup_premise, extract_lookup_item, extract_pricecatcher
from tasks.datawarehouse import dim_item, dim_premise, fact_pricecatcher


with DAG(
    dag_id="daily_load",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["daily_load"],
) as dag:
    
    extract_tasks = [
        extract_lookup_premise(),
        extract_lookup_item(),
        extract_pricecatcher(),
    ]
    
    dim_tasks = [
        dim_item(),
        dim_premise(),
    ]
    
    fact = fact_pricecatcher()
    
    for extract_task in extract_tasks:
        for dim_task in dim_tasks:
            extract_task >> dim_task
    
    for dim_task in dim_tasks:
        dim_task >> fact