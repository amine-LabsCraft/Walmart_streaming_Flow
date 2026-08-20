
import os
import time
from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    RunLifeCycleState,
    RunResultState,
)


@dag(
    dag_id="orchestrate",
    schedule="@hourly",
    start_date=datetime(2026, 8, 17),
    catchup=False,
    tags=["walmart", "dbt", "databricks"],
)
def orchestrate():

    @task
    def ingest_cdc():
        """
        Trigger the Databricks CDC ingestion job
        and wait until the job finishes.
        """

        # Retrieve Databricks credentials from environment variables.
        # The actual values must be stored in .env
        # and must NOT be committed to Git.
        databricks_host = os.environ["DATABRICKS_HOST"]
        databricks_token = os.environ["DATABRICKS_TOKEN"]

        ws = WorkspaceClient(
            host=databricks_host,
            token=databricks_token,
        )

        # Trigger Databricks job
        job_trigger = ws.jobs.run_now(
            job_id="984394399040296"
        )

        print(
            f"Databricks job triggered successfully. "
            f"Run ID: {job_trigger.run_id}"
        )

        # Wait for the Databricks job to finish
        while True:

            job_run = ws.jobs.get_run(
                job_trigger.run_id
            )

            lifecycle_state = (
                job_run.state.life_cycle_state
            )

            result_state = (
                job_run.state.result_state
            )

            print(
                f"Databricks job status: "
                f"{lifecycle_state}"
            )

            # Job finished
            if lifecycle_state in [
                RunLifeCycleState.TERMINATED,
                RunLifeCycleState.SKIPPED,
                RunLifeCycleState.INTERNAL_ERROR,
            ]:

                if result_state == RunResultState.SUCCESS:
                    print(
                        "Databricks CDC job "
                        "completed successfully!"
                    )
                    break

                else:
                    raise Exception(
                        f"Databricks job failed "
                        f"with result state: "
                        f"{result_state}"
                    )

            # Wait before checking again
            time.sleep(5)

        return "CDC Ingestion Completed"

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
    # GOLD - EPHEMERAL
    # ============================================================

    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/airflow/walmart_dbt",
        bash_command="dbt run --select path:models/gold/ephemeral",
    )

    # ============================================================
    # GOLD - DIMENSIONS
    # ============================================================

    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        cwd="/opt/airflow/walmart_dbt",
        bash_command="dbt snapshot",
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
        bash_command="dbt test --select path:models/gold/fact",
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
        >> gold_ephemeral
        >> gold_dimensions
        >> gold_facts
        >> gold_facts_tests
    )


# Instantiate DAG
orchestrate_dag = orchestrate()
