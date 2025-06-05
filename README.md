# goit-de-hw-07

This repository contains the homework assignment for the topic "Apache Airflow".

## Structure

- `dags/medals_dag.py` — Airflow DAG with tasks to:
  - create a table
  - pick a medal
  - branch execution
  - calculate medals
  - introduce a delay
  - check data freshness
- `screenshots/` — Folder containing screenshots of the Airflow UI and DAG tasks.
- `screenshots.md` — Step-by-step documentation with screenshots.

## How to run

1. Clone this repository.
2. Copy `dags/medals_dag.py` into your Airflow DAGs directory.
3. Start Airflow (using `docker-compose up`) and verify DAG execution in the Airflow UI.

## Notes

- PostgreSQL connection ID: `my_postgres`.
- The DAG uses `PostgresOperator`, `PythonOperator`, `BranchPythonOperator`, and `SqlSensor`.
- Make sure the PostgreSQL database has the required `olympic_dataset.athlete_event_results` table.

---
