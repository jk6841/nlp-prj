"""토스증권 커뮤니티 댓글 수집 DAG.

data interval(1시간) 하나마다 [start, end) 사이에 작성된 댓글만 모아 nlp-postgres에 적재한다.

- 수집 대상: nlp-minio의 nlp 버킷 root universe.yaml에서 enabled: true인 종목
- 시작 커서: DB에 이미 있는 end 이후 댓글 중 가장 오래된 comment_id (없으면 최신부터)
- 과거 데이터: 같은 DAG를 과거 방향으로 backfill한다. 더 최신 구간이 먼저 끝나 있으므로
  이미 지나온 페이지를 다시 받지 않는다.
    airflow backfill create --dag-id toss_comments \
        --from-date 2026-11-06T00:00:00+09:00 --to-date 2026-11-07T00:00:00+09:00 \
        --run-backwards --max-active-runs 1
- 재실행해도 ON CONFLICT DO NOTHING이라 중복이 생기지 않는다.
- 테이블은 sql/schema.sql을 미리 수동으로 실행해 만들어 둔다.
"""

import logging
import time

import pendulum
import yaml
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

from toss import repo
from toss.client import TossClient
from toss.collect import collect_interval

log = logging.getLogger(__name__)

MINIO_CONN_ID = "nlp-minio"
UNIVERSE_BUCKET = "nlp"
UNIVERSE_KEY = "universe.yaml"
REQUEST_DELAY_SEC = 1.0


@dag(
    # cron 문자열(@hourly 등)의 Airflow 3 기본 timetable은 CronTriggerTimetable이라
    # data_interval_start == data_interval_end(길이 0)가 된다. [이전 정각, 이번 정각) 구간을
    # 받으려면 CronDataIntervalTimetable을 명시해야 한다.
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="Asia/Seoul"),
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["toss", "comments"],
)
def toss_comments():
    @task
    def load_universe() -> list[dict]:
        raw = S3Hook(aws_conn_id=MINIO_CONN_ID).read_key(UNIVERSE_KEY, UNIVERSE_BUCKET)
        subjects = (yaml.safe_load(raw) or {}).get("subjects") or []

        repo.upsert_stocks(subjects)

        enabled = [
            {"subject_id": s["subject_id"], "name": s["name"], "max_pages": s.get("max_pages")}
            for s in subjects
            if s.get("enabled")
        ]
        log.info("universe: %d subjects, %d enabled: %s",
                 len(subjects), len(enabled), [s["name"] for s in enabled])
        return enabled

    @task(max_active_tis_per_dag=2, retries=3, retry_exponential_backoff=True)
    def collect(subject: dict, data_interval_start=None, data_interval_end=None) -> dict:
        if (data_interval_start is None or data_interval_end is None
                or data_interval_start >= data_interval_end):
            raise ValueError(
                f"수집할 구간이 비어 있다: [{data_interval_start}, {data_interval_end}). "
                "DAG timetable이 1시간 구간을 주는지 확인해야 한다."
            )

        stock_id = subject["subject_id"]
        cursor = repo.cursor_before(stock_id, data_interval_end)

        started = time.monotonic()
        rows, stats = collect_interval(
            fetch_page=TossClient(delay_sec=REQUEST_DELAY_SEC).fetch_page,
            subject_id=stock_id,
            cursor=cursor,
            start=data_interval_start,
            end=data_interval_end,
            max_pages=subject.get("max_pages"),
        )
        inserted = repo.insert_comments(rows)
        elapsed = time.monotonic() - started

        log.info(
            "%s [%s, %s) cursor=%s pages=%d skipped=%d collected=%d inserted=%d elapsed=%.1fs",
            subject["name"], data_interval_start, data_interval_end, cursor,
            stats.pages, stats.skipped, stats.collected, inserted, elapsed,
        )
        if stats.truncated:
            log.warning("%s truncated at max_pages=%s, older comments in this interval are skipped",
                        subject["name"], subject.get("max_pages"))

        return {"stock_id": stock_id, "pages": stats.pages, "inserted": inserted,
                "truncated": stats.truncated}

    collect.expand(subject=load_universe())


toss_comments()
