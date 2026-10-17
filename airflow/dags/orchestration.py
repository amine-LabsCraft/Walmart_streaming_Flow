
import os
from datetime import datetime, timedelta

from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

from walmart_ingestion import SourceResetError, sync_source_to_bronze


# Resilience for a pipeline that talks to a remote SQL warehouse:
# - a task that loses its network connection or its worker is retried
#   automatically instead of failing the whole run;
# - every task has a time limit, so a stalled call ends instead of hanging
#   (normal durations: 30 s to 5 min).
DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=20),
}


@dag(
    dag_id="orchestrate",
    schedule="@hourly",
    start_date=datetime(2026, 8, 17),
    catchup=False,
    # One run at a time: a manual trigger waits for the scheduled run
    # instead of writing the same tables and dbt target folder in parallel.
    max_active_runs=1,
    # A run that cannot finish within the hour is stopped before the next one.
    dagrun_timeout=timedelta(minutes=55),
    default_args=DEFAULT_ARGS,
    tags=["walmart", "dbt", "databricks"],
)
def orchestrate():

    @task
    def ingest_cdc():
        """
        Push new and changed rows from the local Walmart source
        PostgreSQL into the Databricks bronze schema.
        """

        # Credentials come from .env and must NOT be committed to Git.
        try:
            synced = sync_source_to_bronze(
                source_url=os.environ["WALMART_SOURCE_DB_URL"],
                server_hostname=os.environ["DATABRICKS_HOST"],
                http_path=os.environ["DATABRICKS_HTTP_PATH"],
                access_token=os.environ["DATABRICKS_TOKEN"],
                catalog=os.environ.get("DBT_CATALOG", "walmart"),
                schema=os.environ.get("WALMART_BRONZE_SCHEMA", "bronze_local"),
            )
        except SourceResetError as error:
            # Retrying cannot fix a reset source: fail at once, untouched bronze.
            raise AirflowFailException(str(error)) from error

        return synced

    @task.bash
    def clean_target():
        """
        Remove dbt generated target and logs directories.
        """
        return (
            "rm -rf /opt/airflow/walmart_dbt/target "
            "&& "
            "rm -rf /opt/airflow/walmart_dbt/logs"
        )

    @task.bash
    def source_freshness():
        """
        Check dbt source freshness.
        """
        return (
            "cd /opt/airflow/walmart_dbt "
            "&& "
            "dbt source freshness"
        )

    # ============================================================
    # SILVER - TECHNICAL
    # ============================================================

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_dbt",
        bash_command="dbt run --select silver_t",
    )

    silver_technical_tests = BashOperator(
        task_id="silver_technical_tests",
        cwd="/opt/airflow/walmart_dbt",
        bash_command="dbt test --select silver_t",
    )

    # ============================================================
    # SILVER - BUSINESS
    # ============================================================

    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/walmart_dbt",
        bash_command="dbt run --select silver_b",
    )

    silver_business_tests = BashOperator(
        task_id="silver_business_tests",
        cwd="/opt/airflow/walmart_dbt",
        bash_command="dbt test --select silver_b",
    )

    # ============================================================
    # GOLD - DIMENSIONS
    # 1. dbt snapshot records the SCD2 history (walmart.snapshots),
    #    compiling the ephemeral models inline.
    # 2. The star-schema dimensions read the current version of
    #    each snapshot, plus the date, time and order-profile dims.
    # ============================================================

    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        cwd="/opt/airflow/walmart_dbt",
        bash_command="dbt snapshot && dbt run --select path:models/gold/dim",
    )

    # ============================================================
    # GOLD - FACTS
    # ============================================================

    gold_facts = BashOperator(
        task_id="gold_facts",
        cwd="/opt/airflow/walmart_dbt",
        bash_command="dbt run --select path:models/gold/fact",
    )

    gold_facts_tests = BashOperator(
        task_id="gold_facts_tests",
        cwd="/opt/airflow/walmart_dbt",
        bash_command="dbt test --select path:models/gold",
    )

    # ============================================================
    # DAG DEPENDENCIES
    # ============================================================

    (
        ingest_cdc()
        >> clean_target()
        >> source_freshness()
        >> silver_technical
        >> silver_technical_tests
        >> silver_business
        >> silver_business_tests
        >> gold_dimensions
        >> gold_facts
        >> gold_facts_tests
    )


# Instantiate DAG
orchestrate_dag = orchestrate()
