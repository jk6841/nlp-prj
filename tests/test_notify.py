import sys
from pathlib import Path

import pendulum

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from toss.notify import build_payload, fmt_interval  # noqa: E402


def test_payload_shape_and_limits():
    p = build_payload("t" * 300, "d" * 5000, "error", {"구간": "x", "long": "v" * 2000}, "http://log")
    e = p["embeds"][0]
    assert e["title"].startswith("❌") and len(e["title"]) <= 256
    assert len(e["description"]) == 4000
    assert e["color"] == 0xE5484D
    assert e["url"] == "http://log"
    assert e["fields"][0] == {"name": "구간", "value": "x", "inline": True}
    assert len(e["fields"][1]["value"]) == 1024 and e["fields"][1]["inline"] is False


def test_payload_omits_empty_parts():
    e = build_payload("t")["embeds"][0]
    assert "description" not in e and "fields" not in e and "url" not in e


def test_fmt_interval_in_kst():
    s = pendulum.datetime(2026, 11, 7, 5, tz="UTC")
    assert fmt_interval(s, s.add(hours=1)) == "11-07 14:00 ~ 15:00 KST"
    assert fmt_interval(None, None) == "-"


def test_failure_callback_message(monkeypatch):
    from toss import notify

    sent = {}
    monkeypatch.setattr(notify, "send", lambda **kw: sent.update(kw))

    class TI:
        task_id, try_number, max_tries = "collect", 4, 3
        log_url = "http://airflow/log"

    s = pendulum.datetime(2026, 11, 7, 5, tz="UTC")
    notify.on_failure({"ti": TI(), "ticker": "SOXL", "exception": RuntimeError("boom"),
                       "data_interval_start": s, "data_interval_end": s.add(hours=1), "run_id": "r1"})
    assert sent["title"] == "toss_comments collect [SOXL] 최종 실패"
    assert sent["level"] == "error"
    assert "RuntimeError: boom" in sent["description"]
    assert sent["fields"] == {"구간": "11-07 14:00 ~ 15:00 KST", "시도": "4/4", "run_id": "r1"}
    assert sent["url"] == "http://airflow/log"
