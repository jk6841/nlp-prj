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
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from toss import notify, repo
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
    # 모든 task: Airflow 재시도 시와 최종 실패 시 Discord 알림
    default_args={"on_retry_callback": notify.on_retry, "on_failure_callback": notify.on_failure},
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

    @task(
        max_active_tis_per_dag=2,
        retries=3,
        retry_exponential_backoff=True,
        map_index_template="{{ ticker }}",  # UI에 0, 1 대신 SOXL, AAPL로 표시
    )
    def collect(subject: dict, data_interval_start=None, data_interval_end=None) -> dict:
        # 가장 먼저 설정해야 task가 실패해도 티커로 표시되고, 실패 알림에도 티커가 들어간다.
        context = get_current_context()
        context["ticker"] = subject["name"]

        if (data_interval_start is None or data_interval_end is None
                or data_interval_start >= data_interval_end):
            raise ValueError(
                f"수집할 구간이 비어 있다: [{data_interval_start}, {data_interval_end}). "
                "DAG timetable이 1시간 구간을 주는지 확인해야 한다."
            )

        stock_id = subject["subject_id"]
        cursor = repo.cursor_before(stock_id, data_interval_end)

        name = subject["name"]
        max_pages = subject.get("max_pages")
        interval = notify.fmt_interval(data_interval_start, data_interval_end)
        log_url = getattr(context["ti"], "log_url", None)
        client = TossClient(delay_sec=REQUEST_DELAY_SEC)

        log.info("%s [%s, %s) start cursor=%s max_pages=%s",
                 name, data_interval_start, data_interval_end, cursor, max_pages)
        started = time.monotonic()
        try:
            rows, stats = collect_interval(
                fetch_page=client.fetch_page,
                subject_id=stock_id,
                cursor=cursor,
                start=data_interval_start,
                end=data_interval_end,
                max_pages=max_pages,
            )
        finally:
            # 성공·실패와 상관없이, API 재시도가 있었으면 한 번에 모아서 알린다.
            if client.retries:
                summary = ", ".join(f"{k}×{v}" for k, v in client.retries.most_common())
                log.warning("%s API retries: %s (requests=%d)", name, summary, client.requests)
                notify.send(**notify.api_retry_message(name, interval, summary, client.requests, log_url))

        inserted = repo.insert_comments(rows)
        elapsed = time.monotonic() - started

        log.info(
            "%s [%s, %s) cursor=%s pages=%d requests=%d skipped=%d collected=%d inserted=%d elapsed=%.1fs",
            name, data_interval_start, data_interval_end, cursor,
            stats.pages, client.requests, stats.skipped, stats.collected, inserted, elapsed,
        )
        if stats.truncated:
            log.warning("%s truncated at max_pages=%s, older comments in this interval are skipped",
                        name, max_pages)
            notify.send(**notify.truncated_message(name, interval, max_pages, stats.collected, log_url))

        return {"stock_id": stock_id, "pages": stats.pages, "inserted": inserted,
                "truncated": stats.truncated, "retries": dict(client.retries)}

    collect.expand(subject=load_universe())


toss_comments()
