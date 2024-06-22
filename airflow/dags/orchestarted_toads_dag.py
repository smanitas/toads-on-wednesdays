import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from scripts.toads_on_Wednesday import load_image, send_to_teams

with DAG(
    dag_id="orchestrated_toads_dag",
    start_date=pendulum.datetime(2024, 6, 17, tz="UTC"),
    schedule_interval="0 10 * * 3",
    catchup=False,
    tags=["msi_4", "toads", "Wednesdays"]
) as dag:

    start_op = EmptyOperator(task_id="start")

    load_image_op = PythonOperator(
        task_id="load_image",
        python_callable=load_image
    )

    send_to_teams_op = PythonOperator(
        task_id="send_to_teams",
        python_callable=send_to_teams
    )

    finish_op = EmptyOperator(task_id="finish")

    start_op >> load_image_op >> send_to_teams_op >> finish_op
