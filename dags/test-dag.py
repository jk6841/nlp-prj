from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="test-dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smoke"],
)
def test_dag() -> None:
    @task
    def ping() -> str:
        return "ok"

    ping()


test_dag()
