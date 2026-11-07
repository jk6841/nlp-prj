"""Discord 웹훅 알림.

웹훅 URL은 Airflow connection `nlp-discord`의 Password 칸에 넣는다 (type: generic).
connection이 없거나 전송이 실패해도 경고 로그만 남기고 task는 실패시키지 않는다.
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


def build_payload(
    title: str,
    description: str = "",
    level: str = "info",
    fields: dict[str, str] | None = None,
    url: str | None = None,
) -> dict:
    """Discord embed 한 개짜리 payload. 길이 제한(title 256, description 4096, field 1024)을 지킨다."""
    embed = {
        "title": f"{ICONS.get(level, '')} {title}".strip()[:256],
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
    return {"username": "toss_comments", "embeds": [embed]}


def send(title: str, description: str = "", level: str = "info",
         fields: dict[str, str] | None = None, url: str | None = None) -> None:
    try:
        from airflow.sdk import BaseHook

        webhook = BaseHook.get_connection(CONN_ID).password
    except Exception as e:  # connection이 없으면 알림만 건너뛴다.
        log.warning("notification skipped: cannot read connection %s (%s)", CONN_ID, e)
        return
    if not webhook:
        log.warning("notification skipped: connection %s has no password(webhook URL)", CONN_ID)
        return

    try:
        resp = requests.post(webhook, json=build_payload(title, description, level, fields, url), timeout=10)
        resp.raise_for_status()
    except Exception as e:
        log.warning("notification failed: %s", e)


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
    _task_event(context, level="error", what="최종 실패")


def on_retry(context) -> None:
    """task가 실패해 Airflow가 다시 시도할 때."""
    _task_event(context, level="warning", what="실패 후 재시도 예정")


def _task_event(context, level: str, what: str) -> None:
    ti = context["ti"]
    exc = context.get("exception")
    max_tries = getattr(ti, "max_tries", None)
    attempt = f"{ti.try_number}/{max_tries + 1}" if max_tries is not None else str(ti.try_number)
    send(
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
