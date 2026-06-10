from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ─── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_DIR  = "/Users/juneadhi/socialpulse"
VENV_PYTHON  = f"{PROJECT_DIR}/venv/bin/python3"
DBT_DIR      = f"{PROJECT_DIR}/dbt"
DBT_BIN      = f"{PROJECT_DIR}/dbt_venv/bin/dbt"

# ─── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "socialpulse",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

# ─── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="socialpulse_pipeline",
    description="SocialPulse: Bronze → Silver → Gold → Postgres → dbt",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["socialpulse", "spark", "dbt"],
) as dag:

    # ── Task 1: Bronze → Silver ─────────────────────────────────────────────────
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{VENV_PYTHON} spark/batch/bronze_to_silver.py"
        ),
    )

    # ── Task 2: Silver → Gold ───────────────────────────────────────────────────
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{VENV_PYTHON} spark/batch/silver_to_gold.py"
        ),
    )

    # ── Task 3: Load Gold → Postgres ────────────────────────────────────────────
    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{VENV_PYTHON} spark/batch/load_to_postgres.py"
        ),
    )

    # ── Task 4: dbt run ─────────────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} run --profiles-dir ~/.dbt"
        ),
    )

    # ── Task 5: dbt test ────────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} test --profiles-dir ~/.dbt"
        ),
    )

    # ── Pipeline Order ──────────────────────────────────────────────────────────
    bronze_to_silver >> silver_to_gold >> load_to_postgres >> dbt_run >> dbt_test
