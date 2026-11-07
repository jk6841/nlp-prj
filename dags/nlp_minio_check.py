"""nlp-minio connection 연결 테스트 DAG.

버킷이 없으면 만들고, 테스트 파일을 하나 쓴 뒤 다시 읽어 내용이 같은지 확인한다.
버킷 이름은 trigger할 때 params.bucket으로 바꿀 수 있다.
"""

import logging
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

log = logging.getLogger(__name__)

CONN_ID = "nlp-minio"


@dag(
    dag_id="nlp_minio_check",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smoke", "minio"],
    params={"bucket": "nlp"},
)
def nlp_minio_check() -> None:
    @task
    def check(params=None, run_id=None) -> None:
        bucket = params["bucket"]
        hook = S3Hook(aws_conn_id=CONN_ID)

        if not hook.check_for_bucket(bucket):
            log.info("bucket %s not found, creating", bucket)
            hook.create_bucket(bucket_name=bucket)

        key = "conn_check/latest.txt"
        hook.load_string(run_id, key=key, bucket_name=bucket, replace=True)
        body = hook.read_key(key, bucket_name=bucket)
        if body != run_id:
            raise ValueError(f"read mismatch: wrote {run_id!r}, read {body!r}")

        keys = hook.list_keys(bucket_name=bucket, prefix="conn_check/")
        log.info("bucket=%s wrote and read %s ok, keys=%s", bucket, key, keys)

    check()


nlp_minio_check()
