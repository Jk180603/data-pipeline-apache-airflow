from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = "/opt/airflow"

with DAG(
    dag_id="retail_sales_etl_pipeline",
    description="Retail sales ETL pipeline with data quality, analytics, and PostgreSQL loading",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retail", "etl", "postgres"],
) as dag:

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/transform.py",
    )

    data_quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/data_quality.py",
    )

    generate_analytics = BashOperator(
        task_id="generate_analytics",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/analytics.py",
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/load.py",
        env={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "airflow",
            "POSTGRES_PASSWORD": "airflow",
            "POSTGRES_DB": "retail_warehouse",
        },
    )

    transform_data >> data_quality_checks >> generate_analytics >> load_to_postgres