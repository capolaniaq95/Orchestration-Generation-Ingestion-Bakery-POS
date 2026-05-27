from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='test_celery_executor',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['testing'],
) as dag:

    # Una tarea que duerme por 30 segundos
    tarea_larga = BashOperator(
        task_id='dormir_30_segundos',
        bash_command='sleep 30 && echo "Desperté en el Worker!"'
    )