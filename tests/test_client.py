import logging
import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from toss.client import TossApiError, TossClient  # noqa: E402

OK_BODY = {"result": {"results": [{"commentId": 1}], "key": 1, "hasNext": False}}


class FakeResp:
    def __init__(self, status=200, body=None, text=None, headers=None):
        self.status_code = status
        self._body = body
        self.text = text if text is not None else str(body)
        self.headers = headers or {}

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


class FakeSession:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.headers = {}
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        out = self.outcomes.pop(0)
        if isinstance(out, Exception):
            raise out
        return out


def client(outcomes, **kw):
    sleeps = []
    c = TossClient(delay_sec=0, session=FakeSession(outcomes), sleep=sleeps.append, **kw)
    return c, sleeps


def test_success_without_retry():
    c, sleeps = client([FakeResp(200, OK_BODY)])
    page = c.fetch_page("S")
    assert page.key == 1 and not page.has_next
    assert c.retries == {} and c.requests == 1 and sleeps == []


def test_429_retries_with_backoff_and_logs_warning(caplog):
    c, sleeps = client([FakeResp(429, text="slow down"), FakeResp(503, text="busy"), FakeResp(200, OK_BODY)])
    with caplog.at_level(logging.WARNING, logger="toss.client"):
        c.fetch_page("S", 123)
    assert c.retries == {"429": 1, "503": 1}
    assert sleeps == [2.0, 4.0]
    assert c.requests == 3
    msgs = [r.getMessage() for r in caplog.records]
    assert any("retry 1/5" in m and "reason=429" in m and "lastCommentId=123" in m for m in msgs)


def test_retry_after_header_is_respected_and_capped():
    c, sleeps = client([FakeResp(429, text="", headers={"Retry-After": "30"}),
                        FakeResp(429, text="", headers={"Retry-After": "9999"}),
                        FakeResp(200, OK_BODY)])
    c.fetch_page("S")
    assert sleeps == [30.0, 120.0]


def test_connection_error_is_retried():
    c, sleeps = client([requests.ConnectionError("reset"), FakeResp(200, OK_BODY)])
    c.fetch_page("S")
    assert c.retries == {"ConnectionError": 1}


def test_403_fails_immediately_with_body():
    c, sleeps = client([FakeResp(403, text="blocked by waf")])
    with pytest.raises(TossApiError) as e:
        c.fetch_page("S", 7)
    assert e.value.status == 403
    assert "blocked by waf" in str(e.value) and "lastCommentId=7" in str(e.value)
    assert sleeps == [] and c.retries == {}


def test_gives_up_after_max_retries():
    c, sleeps = client([FakeResp(503, text="down")] * 3, max_retries=2)
    with pytest.raises(TossApiError) as e:
        c.fetch_page("S")
    assert "2회 재시도 후에도 실패(503)" in str(e.value)
    assert c.retries == {"503": 2} and len(sleeps) == 2 and c.requests == 3


def test_unexpected_body_raises_with_snippet():
    c, _ = client([FakeResp(200, body={"error": "x"}, text='{"error": "x"}')])
    with pytest.raises(TossApiError) as e:
        c.fetch_page("S")
    assert '{"error": "x"}' in str(e.value)
