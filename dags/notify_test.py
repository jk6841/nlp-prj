"""Discord 웹훅 수신 테스트 DAG (수동 실행용).

toss_comments가 실제로 보내는 알림 4종을 [TEST] 표시를 붙여 보낸다.
  1. task 최종 실패  2. Airflow 재시도  3. API 재시도  4. max_pages 도달
- nlp-discord connection이 없으면 task는 skipped
- 하나라도 전송에 실패하면 task가 실패하고, 로그에 HTTP 상태와 응답이 남는다
"""

import logging

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from toss import notify
from toss.client import TossApiError

log = logging.getLogger(__name__)


class _AsCollectTI:
    """실제 알림처럼 제목이 'collect [SOXL]'로 나오도록 task_id만 바꿔 보이는 ti 래퍼."""

    task_id = "collect"

    def __init__(self, ti):
        self._ti = ti

    def __getattr__(self, name):
        return getattr(self._ti, name)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["smoke", "notify"],
)
def notify_test():
    @task
    def send_samples() -> None:
        context = get_current_context()

        # 실제 알림과 같은 함수로 만들되, 티커·구간·예외는 예시 값을 넣는다.
        end = pendulum.now("Asia/Seoul").start_of("hour")
        sample_ctx = {
            **context,
            "ti": _AsCollectTI(context["ti"]),
            "ticker": "SOXL",
            "data_interval_start": end.subtract(hours=1),
            "data_interval_end": end,
            "exception": TossApiError("HTTP 403 subject=US20100311002 lastCommentId=340657182",
                                      403, "<html>blocked</html>"),
        }
        interval = notify.fmt_interval(end.subtract(hours=1), end)
        url = getattr(context["ti"], "log_url", None)
        samples = [
            notify.task_event_message(sample_ctx, level="error", what="최종 실패"),
            notify.task_event_message(sample_ctx, level="warning", what="실패 후 재시도 예정"),
            notify.api_retry_message("SOXL", interval, "429×2, 503×1", 16, url),
            notify.truncated_message("AAPL", interval, 50, 550, url),
        ]

        results = []
        for msg in samples:
            msg["title"] = f"[TEST] {msg['title']}"
            result = notify.deliver(**msg)
            log.info("%s | %s", result, msg["title"])
            results.append(result)

        if all(r.startswith("skipped") for r in results):
            raise AirflowSkipException(f"{notify.CONN_ID} connection이 없어 건너뜀")
        failed = [r for r in results if r != "ok"]
        if failed:
            raise RuntimeError(f"전송 실패 {len(failed)}/{len(results)}: {failed[0]}")
        log.info("%d개 알림 전송 완료", len(results))

    send_samples()


notify_test()
