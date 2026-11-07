import logging
import sys
from pathlib import Path

import pendulum
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from toss.client import Page  # noqa: E402
from toss.collect import collect_interval  # noqa: E402

TZ = "Asia/Seoul"
START = pendulum.datetime(2026, 11, 7, 14, tz=TZ)
END = pendulum.datetime(2026, 11, 7, 15, tz=TZ)


def comment(cid: int, at: pendulum.DateTime) -> dict:
    return {
        "commentId": cid,
        "createdAt": at.isoformat(),
        "authorUserProfileId": 1000 + cid,
        "author": {"userProfileId": 1000 + cid, "nickname": "닉네임"},
        "message": {"title": "", "message": f"m{cid}"},
        "statistic": {"likeCount": 1, "replyCount": 0},
        "holding": {"shareHoldingStatus": "HOLDING" if cid % 2 else None},
    }


class FakeApi:
    """comment_id 내림차순 = createdAt 내림차순인 댓글 목록을 page_size씩 넘겨준다."""

    def __init__(self, comments: list[dict], page_size: int = 3):
        self.comments = sorted(comments, key=lambda c: -c["commentId"])
        self.page_size = page_size
        self.calls: list[int | None] = []

    def fetch_page(self, subject_id: str, last_comment_id: int | None) -> Page:
        self.calls.append(last_comment_id)
        rest = [c for c in self.comments
                if last_comment_id is None or c["commentId"] < last_comment_id]
        page = rest[: self.page_size]
        return Page(
            results=page,
            key=page[-1]["commentId"] if page else None,
            has_next=len(rest) > self.page_size,
        )


def minutes(m: int) -> pendulum.DateTime:
    return START.add(minutes=m)


def ids(rows):
    return [r[0] for r in rows]


def test_boundaries_start_inclusive_end_exclusive():
    api = FakeApi([
        comment(10, minutes(61)),  # end 이후 → skip
        comment(9, minutes(60)),   # == end → skip
        comment(8, minutes(59)),
        comment(7, minutes(0)),    # == start → 포함
        comment(6, minutes(-1)),   # start 이전 → 종료
        comment(5, minutes(-2)),
    ])
    rows, stats = collect_interval(api.fetch_page, "S", None, START, END)
    assert ids(rows) == [8, 7]
    assert stats.skipped == 2
    assert stats.collected == 2
    assert not stats.truncated
    assert stats.pages == 2  # [10,9,8] [7,6,…] 에서 멈춘다


def test_cursor_is_passed_and_no_skip():
    api = FakeApi([comment(i, minutes(i)) for i in range(70, -5, -1)])
    rows, stats = collect_interval(api.fetch_page, "S", 60, START, END)
    assert api.calls[0] == 60
    assert stats.skipped == 0
    assert ids(rows) == list(range(59, -1, -1))


def test_empty_interval_costs_one_request():
    api = FakeApi([comment(3, minutes(-10)), comment(2, minutes(-20)), comment(1, minutes(-30))])
    rows, stats = collect_interval(api.fetch_page, "S", None, START, END)
    assert rows == []
    assert stats.pages == 1


def test_stops_when_no_next_page():
    api = FakeApi([comment(2, minutes(30)), comment(1, minutes(20))])
    rows, stats = collect_interval(api.fetch_page, "S", None, START, END)
    assert ids(rows) == [2, 1]
    assert stats.pages == 1


def test_max_pages_truncates():
    api = FakeApi([comment(i, minutes(i)) for i in range(59, -1, -1)], page_size=11)
    rows, stats = collect_interval(api.fetch_page, "S", None, START, END, max_pages=2)
    assert stats.truncated
    assert stats.pages == 2
    assert ids(rows) == list(range(59, 59 - 22, -1))


def test_row_shape_and_nanosecond_timestamp():
    c = comment(1, minutes(5))
    c["createdAt"] = "2026-11-07T14:05:04.380634567+09:00"
    api = FakeApi([c])
    rows, _ = collect_interval(api.fetch_page, "US20100311002", None, START, END)
    (comment_id, stock_id, author_id, holding, title, message,
     created_at, likes, replies) = rows[0]
    assert (comment_id, stock_id, author_id, holding, title, message, likes, replies) == (
        1, "US20100311002", 1001, "HOLDING", "", "m1", 1, 0)
    assert "닉네임" not in rows[0]
    assert START <= created_at < END


def test_missing_holding_is_none():
    c = comment(1, minutes(5))
    del c["holding"]
    rows, _ = collect_interval(FakeApi([c]).fetch_page, "S", None, START, END)
    assert rows[0][3] is None


def test_failure_logs_progress_context(caplog):
    api = FakeApi([comment(i, minutes(59 - i)) for i in range(10, 0, -1)], page_size=3)
    calls = {"n": 0}

    def flaky(subject_id, last_id):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("boom")
        return api.fetch_page(subject_id, last_id)

    with caplog.at_level(logging.ERROR, logger="toss.collect"), pytest.raises(RuntimeError):
        collect_interval(flaky, "S", None, START, END)
    msg = caplog.records[-1].getMessage()
    assert "collect failed subject=S" in msg
    assert "lastCommentId=5" in msg and "pages=2" in msg and "collected=6" in msg


def test_progress_logged_every_n_pages(caplog):
    api = FakeApi([comment(i, minutes(59)) for i in range(20, 0, -1)], page_size=2)
    with caplog.at_level(logging.INFO, logger="toss.collect"):
        collect_interval(api.fetch_page, "S", None, START, END, progress_every=4)
    progress = [r.getMessage() for r in caplog.records if r.getMessage().startswith("progress")]
    assert len(progress) == 2 and "pages=4" in progress[0] and "pages=8" in progress[1]
