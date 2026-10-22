from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


DEFAULT_ARGS = {
    "owner": "metropolia_data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# Jadwal demonstrasi dijalankan pada tanggal aktual, sementara data yang
# diproses tetap memakai tujuh tanggal simulasi setelah historical backfill.
DAILY_RUNTIME_FIRST_DATE = pendulum.date(2026, 9, 20)
DAILY_SIMULATION_FIRST_DATE = pendulum.date(2025, 9, 1)


def daily_simulation_date(data_interval_end) -> str:
    runtime_date = data_interval_end.in_timezone("Asia/Jakarta").date()
    day_offset = (runtime_date - DAILY_RUNTIME_FIRST_DATE).days
    return DAILY_SIMULATION_FIRST_DATE.add(days=day_offset).to_date_string()


def validation_task(task_id: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=(
            "python /opt/airflow/scripts/check_input.py "
            "--data-root /opt/airflow/data"
        ),
    )


def transform_task(task_id: str, mode: str, start_date: str, end_date: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=(
            "python /opt/airflow/scripts/spark_metropolia_etl.py "
            f"--mode {mode} --start-date {start_date} --end-date {end_date}"
        ),
    )


def upsert_task(task_id: str) -> PostgresOperator:
    return PostgresOperator(
        task_id=task_id,
        postgres_conn_id="metropolia_postgres",
        sql="upsert_staging.sql",
    )


with DAG(
    dag_id="metropolia_historical_backfill",
    default_args=DEFAULT_ARGS,
    description="Initial batch load untuk histori Metropolia",
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/sql"],
    tags=["metropolia", "backfill", "pyspark", "postgres"],
) as historical_backfill_dag:
    validate_backfill = validation_task("validate_backfill_inputs")
    transform_backfill = transform_task(
        "transform_historical_data",
        "backfill",
        "2022-01-01",
        "2025-08-31",
    )
    upsert_backfill = upsert_task("upsert_historical_warehouse")

    validate_backfill >> transform_backfill >> upsert_backfill


with DAG(
    dag_id="metropolia_daily_batch",
    default_args=DEFAULT_ARGS,
    description="Tujuh batch harian sebelum cutover streaming",
    schedule_interval="0 14 * * *",
    # Airflow membuat run setelah interval selesai. Start satu hari sebelumnya
    # membuat run pertama dipicu 20 September 2026 pukul 14.00 WIB.
    start_date=pendulum.datetime(2026, 9, 19, 14, 0, tz="Asia/Jakarta"),
    end_date=pendulum.datetime(2026, 9, 25, 14, 0, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/sql"],
    user_defined_macros={"daily_simulation_date": daily_simulation_date},
    tags=["metropolia", "daily", "pyspark", "postgres"],
) as daily_batch_dag:
    validate_daily = validation_task("validate_daily_inputs")
    transform_daily = transform_task(
        "transform_daily_data",
        "daily",
        "{{ daily_simulation_date(data_interval_end) }}",
        "{{ daily_simulation_date(data_interval_end) }}",
    )
    upsert_daily = upsert_task("upsert_daily_warehouse")

    validate_daily >> transform_daily >> upsert_daily
