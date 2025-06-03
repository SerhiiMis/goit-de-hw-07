# goit-de-hw-07

This repository contains the homework assignment for the topic "Apache Airflow".

## Structure

- `dags/medals_dag.py` — Airflow DAG with tasks to create a table, pick a medal, branch execution, calculate medals, introduce a delay, and check data freshness.
- `screenshots/` — Folder containing screenshots of Airflow UI and DAG tasks.
- `screenshots.md` — Step-by-step documentation with screenshots.

## How to run

1. Clone this repository.
2. Copy `dags/medals_dag.py` into your Airflow DAGs directory.
3. Start Airflow and verify DAG execution in the Airflow UI.

## Notes

- MySQL connection ID: `mysql_default`.
- The DAG uses `MySqlOperator`, `PythonOperator`, `BranchPythonOperator`, and `SqlSensor`.
