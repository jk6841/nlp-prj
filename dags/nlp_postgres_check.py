"""nlp-postgres connection 연결 테스트 DAG.

테스트용 테이블(conn_check)을 만들고 1행을 쓴 뒤 다시 읽어 로그로 남긴다.
실행할 때마다 count가 1씩 늘어난다.
"""

import logging
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

log = logging.getLogger(__name__)

CONN_ID = "nlp-postgres"

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS conn_check (
    id         BIGSERIAL   PRIMARY KEY,
    note       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


@dag(
    dag_id="nlp_postgres_check",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smoke", "postgres"],
)
def nlp_postgres_check() -> None:
    @task
    def check(run_id=None) -> None:
        hook = PostgresHook(postgres_conn_id=CONN_ID)

        log.info("version: %s", hook.get_first("SELECT version()")[0])

        hook.run(CREATE_SQL)
        hook.run("INSERT INTO conn_check (note) VALUES (%s)", parameters=(run_id,))

        count, last = hook.get_first("SELECT count(*), max(created_at) FROM conn_check")
        log.info("conn_check rows=%s last_created_at=%s", count, last)

    check()


nlp_postgres_check()
