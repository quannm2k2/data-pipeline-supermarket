from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator # type: ignore
import os

RAW_TABLES = [
    'aisles',
    'departments',
    'order_products',
    'orders_small_version',
    'products'
]

# Danh sách các Airbyte connection ID
AIRBYTE_CONNECTION_IDS = [
    'dd5f46ea-fa33-40bc-afae-d96579a30e4b',
    '41faff1b-fc11-4b69-aa46-e6e0c253335d',
    #'aaab6305-81ab-4a9b-a528-0ab0d082fc1f'
    #'3617d558-c568-480b-8240-f9b519ce8e93'
    'd20731fc-29c5-4f2f-847b-db356d3bf83f'
    # Thêm các connection ID khác ở đây
]

# Danh sách các bảng cần copy và offload
TABLES_TO_COPY = [
    'fct_user_orders',
    'dim_products',
    'dim_departments',
    'dim_aisles'
    # Thêm các bảng khác ở đây
]

# Đường dẫn thư mục lưu file CSV (trên WSL)
CSV_DIR = 'F:/AllCode/Data Engineer/'
CSV_DIR_WSL = '/mnt/f/AllCode/Data Engineer/'

# Đường dẫn bucket S3
S3_BUCKET = 's3://quannm-07252024-demo/'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'data_pipeline_supermarket_2',
    default_args=default_args,
    description='Data Pipeline with multiple Airbyte syncs, dbt run/test, and S3 offload',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

start_airbyte = DummyOperator(
    task_id='start_airbyte',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

upload_raw_tasks = []

# Tạo các task copy và offload các bảng lên S3
for table in RAW_TABLES:
    csv_file_path_wsl = os.path.join(CSV_DIR_WSL, f'raw_data/supermarket/{table}.csv')
    s3_key = f'raw_data/supermarket/{table}.csv'

    upload_raw_tables = BashOperator(
        task_id=f'offload_to_s3_{table}',
        bash_command=f'aws s3 cp "{csv_file_path_wsl}" {S3_BUCKET}{s3_key}',
        dag=dag,
    )

    start >> upload_raw_tables
    upload_raw_tasks.append(upload_raw_tables)

upload_raw_tasks >> start_airbyte

# Tạo các task đồng bộ Airbyte
airbyte_tasks = []

for i, connection_id in enumerate(AIRBYTE_CONNECTION_IDS):
    trigger_sync_task = AirbyteTriggerSyncOperator(
        task_id=f'airbyte_trigger_sync_{i}',
        airbyte_conn_id='airflow-call-to-airbyte-example',
        connection_id=connection_id,
        asynchronous=True,
        dag=dag,
    )

    wait_for_sync_task = AirbyteJobSensor(
        task_id=f'airbyte_check_sync_{i}',
        airbyte_conn_id='airflow-call-to-airbyte-example',
        airbyte_job_id=trigger_sync_task.output,
        dag=dag,
    )

    start_airbyte >> trigger_sync_task >> wait_for_sync_task
    airbyte_tasks.append(wait_for_sync_task)

# Định nghĩa task dbt run
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /root/project/dbt_supermarket && dbt run',
    dag=dag,
)

# Định nghĩa task dbt test
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /root/project/dbt_supermarket && dbt test',
    dag=dag,
)

# Đặt phụ thuộc task
for task in airbyte_tasks:
    task >> dbt_run

dbt_run >> dbt_test

# Tạo các task copy và offload các bảng lên S3
for table in TABLES_TO_COPY:
    csv_file_path = os.path.join(CSV_DIR, f'data_processed/supermarket/{table}.csv')
    csv_file_path_wsl = os.path.join(CSV_DIR_WSL, f'data_processed/supermarket/{table}.csv')
    s3_key = f'data_processed/supermarket/{table}.csv'
    
    copy_task = PostgresOperator(
        task_id=f'copy_table_{table}',
        postgres_conn_id='postgres_supermarket',
        sql=f"""
        SET search_path TO supermarket;
        COPY {table} TO '{csv_file_path}' CSV HEADER;
        """,
        dag=dag,
    )

    offload_task = BashOperator(
        task_id=f'offload_to_s3_{table}',
        bash_command=f'aws s3 cp "{csv_file_path_wsl}" {S3_BUCKET}{s3_key}',
        dag=dag,
    )

    dbt_test >> copy_task >> offload_task >> end
