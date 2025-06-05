from datetime import datetime, timedelta
import random
import time

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'serhii',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='medals_dag',
    default_args=default_args,
    description='DAG for counting medals and handling delays',
    schedule_interval=None,
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='my_postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS medals_table (
                id SERIAL PRIMARY KEY,
                medal_type VARCHAR(10),
                count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    def pick_medal(**kwargs):
        medal = random.choice(['Gold', 'Silver', 'Bronze'])
        kwargs['ti'].xcom_push(key='medal', value=medal)

    pick_medal_task = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal,
        provide_context=True
    )

    def branch_medal(**kwargs):
        medal = kwargs['ti'].xcom_pull(key='medal')
        if medal == 'Gold':
            return 'calc_Gold'
        elif medal == 'Silver':
            return 'calc_Silver'
        else:
            return 'calc_Bronze'

    branch_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=branch_medal,
        provide_context=True
    )

    calc_Gold = PostgresOperator(
        task_id='calc_Gold',
        postgres_conn_id='my_postgres',
        sql="""
            INSERT INTO medals_table (medal_type, count)
            SELECT 'Gold', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';
        """
    )

    calc_Silver = PostgresOperator(
        task_id='calc_Silver',
        postgres_conn_id='my_postgres',
        sql="""
            INSERT INTO medals_table (medal_type, count)
            SELECT 'Silver', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver';
        """
    )

    calc_Bronze = PostgresOperator(
        task_id='calc_Bronze',
        postgres_conn_id='my_postgres',
        sql="""
            INSERT INTO medals_table (medal_type, count)
            SELECT 'Bronze', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
        """
    )

    def sleep_task():
        time.sleep(35)

    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=sleep_task,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id='my_postgres',
        sql="""
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX(created_at)))
            FROM medals_table
            HAVING EXTRACT(EPOCH FROM (NOW() - MAX(created_at))) < 30;
        """,
        timeout=60,
        poke_interval=10,
        mode='poke',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )
    create_table >> pick_medal_task >> branch_task
    branch_task >> [calc_Gold, calc_Silver, calc_Bronze] >> generate_delay >> check_for_correctness
