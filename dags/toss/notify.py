"""Discord 웹훅 알림.

웹훅 URL은 Airflow connection `nlp-discord`의 Password 칸에 넣는다 (type: generic).
운영 DAG에서는 connection이 없거나 전송이 실패해도 경고 로그만 남기고 task는 실패시키지 않는다(send).
수신 테스트(notify_test DAG)는 deliver로 결과를 받아 판단한다.
"""

import logging
from datetime import datetime, timezone

import pendulum
import requests

log = logging.getLogger(__name__)

CONN_ID = "nlp-discord"
TZ = "Asia/Seoul"
COLORS = {"error": 0xE5484D, "warning": 0xF5A524, "info": 0x3E63DD}
ICONS = {"error": "❌", "warning": "⚠️", "info": "ℹ️"}
USERNAME = "toss_comments"


def _title(title: str, level: str) -> str:
    return f"{ICONS.get(level, '')} {title}".strip()


def build_payload(
    title: str,
    description: str = "",
    level: str = "info",
    fields: dict[str, str] | None = None,
    url: str | None = None,
) -> dict:
    """Discord embed 한 개짜리 payload. 길이 제한(title 256, description 4096, field 1024)을 지킨다."""
    embed = {
        "title": _title(title, level)[:256],
        "color": COLORS.get(level, COLORS["info"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if description:
        embed["description"] = description[:4000]
    if fields:
        embed["fields"] = [
            {"name": str(k)[:256], "value": (str(v) or "-")[:1024], "inline": len(str(v)) <= 40}
            for k, v in fields.items()
        ][:25]
    if url:
        embed["url"] = url
    return {"username": USERNAME, "embeds": [embed]}


def _webhook() -> str | None:
    """connection의 Password(웹훅 URL). connection이 없으면 None."""
    from airflow.sdk import BaseHook

    try:
        return BaseHook.get_connection(CONN_ID).password or None
    except Exception:
        return None


def deliver(title: str, description: str = "", level: str = "info",
            fields: dict[str, str] | None = None, url: str | None = None) -> str:
    """보내고 결과를 돌려준다: "ok", "skipped (no connection)", 또는 "error: ...". 예외는 던지지 않는다."""
    webhook = _webhook()
    if not webhook:
        return "skipped (no connection)"
    try:
        resp = requests.post(webhook, json=build_payload(title, description, level, fields, url), timeout=10)
    except Exception as e:
        return f"error: {e}"
    if resp.status_code >= 400:
        return f"error: HTTP {resp.status_code} {resp.text[:200]!r}"
    return "ok"


def send(title: str, description: str = "", level: str = "info",
         fields: dict[str, str] | None = None, url: str | None = None) -> None:
    """운영용. 실패해도 경고 로그만 남긴다."""
    result = deliver(title, description, level, fields, url)
    if result.startswith("error"):
        log.warning("notification failed: %s", result)
    elif result != "ok":
        log.warning("notification %s: %s", result, CONN_ID)


def fmt_interval(start, end) -> str:
    if start is None or end is None:
        return "-"
    s = pendulum.instance(start).in_timezone(TZ)
    e = pendulum.instance(end).in_timezone(TZ)
    return f"{s:%m-%d %H:%M} ~ {e:%H:%M} KST"


def _task_label(context) -> str:
    ti = context["ti"]
    ticker = context.get("ticker")
    return f"{ti.task_id} [{ticker}]" if ticker else ti.task_id


def on_failure(context) -> None:
    """Airflow 재시도까지 모두 실패했을 때."""
    send(**task_event_message(context, level="error", what="최종 실패"))


def on_retry(context) -> None:
    """task가 실패해 Airflow가 다시 시도할 때."""
    send(**task_event_message(context, level="warning", what="실패 후 재시도 예정"))


# --- 메시지 정의. 운영 DAG와 notify_test DAG가 같은 함수를 쓴다. ---

def task_event_message(context, level: str, what: str) -> dict:
    ti = context["ti"]
    exc = context.get("exception")
    max_tries = getattr(ti, "max_tries", None)
    attempt = f"{ti.try_number}/{max_tries + 1}" if max_tries is not None else str(ti.try_number)
    return dict(
        title=f"toss_comments {_task_label(context)} {what}",
        description=f"```\n{type(exc).__name__}: {exc}\n```" if exc else "",
        level=level,
        fields={
            "구간": fmt_interval(context.get("data_interval_start"), context.get("data_interval_end")),
            "시도": attempt,
            "run_id": context.get("run_id", "-"),
        },
        url=getattr(ti, "log_url", None),
    )


def api_retry_message(name: str, interval: str, summary: str, requests_made: int,
                      url: str | None = None) -> dict:
    return dict(
        title=f"toss_comments collect [{name}] API 재시도 발생",
        level="warning",
        fields={"구간": interval, "재시도": summary, "요청 수": str(requests_made)},
        url=url,
    )


def truncated_message(name: str, interval: str, max_pages, collected: int,
                      url: str | None = None) -> dict:
    return dict(
        title=f"toss_comments collect [{name}] max_pages 도달",
        description="이 구간의 오래된 쪽 댓글은 수집되지 않았습니다.",
        level="warning",
        fields={"구간": interval, "max_pages": str(max_pages), "수집": str(collected)},
        url=url,
    )
