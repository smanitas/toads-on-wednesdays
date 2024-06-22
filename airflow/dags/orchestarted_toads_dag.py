import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow import DAG
from scripts.toads_on_Wednesday import (
    _load_image,
    _create_content_image,
    _send_to_teams
)

with DAG(
    dag_id="orchestrated_toads_dag",
    start_date=pendulum.datetime(2024, 6, 17, tz="UTC"),
    schedule_interval="0 10 * * 3",
    catchup=False,
    tags=["msi_6", "toads", "Wednesday"]
) as dag:

    start_op = EmptyOperator(task_id="start")

    load_image_op = PythonOperator(
        task_id="load_image",
        python_callable=_load_image
    )

    create_content_image_op = PythonOperator(
        task_id="create_content_image",
        python_callable=_create_content_image
    )

    send_to_teams_op = PythonOperator(
        task_id="send_to_teams",
        python_callable=_send_to_teams
    )

    finish_op = EmptyOperator(task_id="finish")

    start_op >> load_image_op >> create_content_image_op >> send_to_teams_op >> finish_op
