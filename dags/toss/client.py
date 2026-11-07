"""토스증권 커뮤니티 댓글 API 클라이언트.

웹 UI가 내부에서 호출하는 댓글 API(GET /api/v4/comments)를 쓴다.
최신순(RECENT)으로만 받고, lastCommentId 커서로 과거 방향 페이지를 넘긴다.

재시도는 urllib3 Retry 대신 직접 구현한다. urllib3는 상태 코드 재시도를 DEBUG로만 남겨
Airflow 로그에 보이지 않기 때문이다. 재시도할 때마다 WARNING을 남기고 사유별 횟수를
self.retries에 모아 둔다(알림용).
"""

import json
import logging
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import Callable

import requests

log = logging.getLogger(__name__)

API_URL = "https://wts-cert-api.tossinvest.com/api/v4/comments"
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
MAX_WAIT_SEC = 120.0
BODY_SNIPPET_LEN = 300


@dataclass
class Page:
    results: list[dict]
    key: int | None  # 이 페이지 마지막 댓글 ID. 다음 요청의 lastCommentId
    has_next: bool


class TossApiError(Exception):
    """재시도할 수 없거나 재시도를 다 쓴 API 실패. 상태 코드와 응답 본문 앞부분을 담는다."""

    def __init__(self, message: str, status: int | None = None, body: str | None = None):
        self.status = status
        self.body = body
        parts = [message]
        if status is not None:
            parts.append(f"status={status}")
        if body:
            parts.append(f"body={body!r}")
        super().__init__(" ".join(parts))


def _snippet(resp: requests.Response) -> str:
    return (resp.text or "")[:BODY_SNIPPET_LEN]


def _retry_after(resp: requests.Response) -> float | None:
    value = resp.headers.get("Retry-After")
    try:
        return float(value) if value is not None else None
    except ValueError:  # HTTP-date 형식은 무시하고 백오프를 쓴다.
        return None


class TossClient:
    def __init__(
        self,
        delay_sec: float = 1.0,
        timeout: float = 30.0,
        max_retries: int = 5,
        backoff_base: float = 2.0,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.delay_sec = delay_sec
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.sleep = sleep
        self.retries: Counter[str] = Counter()  # 재시도 사유("429", "ConnectionError" 등)별 횟수
        self.requests = 0
        self._last_request_at = 0.0

        self.session = session or requests.Session()
        self.session.headers.update(
            {
                # Cookie에 deviceId만 있으면 값과 상관없이 요청이 성공한다.
                "Cookie": f"deviceId={uuid.uuid4()}",
                # requests 기본 UA(python-requests/*)는 403으로 막힌다.
                "User-Agent": "Mozilla/5.0",
            }
        )

    def _throttle(self) -> None:
        wait = self.delay_sec - (time.monotonic() - self._last_request_at)
        if wait > 0:
            self.sleep(wait)
        self._last_request_at = time.monotonic()

    def fetch_page(self, subject_id: str, last_comment_id: int | None = None) -> Page:
        params = {
            "subjectType": "STOCK",
            "subjectId": subject_id,
            "commentSortType": "RECENT",
        }
        if last_comment_id is not None:
            params["lastCommentId"] = last_comment_id

        for attempt in range(1, self.max_retries + 2):
            self._throttle()
            self.requests += 1
            retry_after = None
            try:
                resp = self.session.get(API_URL, params=params, timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout) as e:
                reason, detail, status = type(e).__name__, str(e)[:BODY_SNIPPET_LEN], None
            else:
                if resp.status_code in RETRY_STATUS:
                    reason, detail, status = str(resp.status_code), _snippet(resp), resp.status_code
                    retry_after = _retry_after(resp)
                elif resp.status_code >= 400:
                    # 403(차단) 등은 재시도해도 같으므로 바로 실패시킨다.
                    raise TossApiError(
                        f"HTTP {resp.status_code} subject={subject_id} lastCommentId={last_comment_id}",
                        resp.status_code, _snippet(resp),
                    )
                else:
                    return self._parse(resp, subject_id, last_comment_id)

            if attempt > self.max_retries:
                raise TossApiError(
                    f"{self.max_retries}회 재시도 후에도 실패({reason}) "
                    f"subject={subject_id} lastCommentId={last_comment_id}",
                    status, detail,
                )
            wait = min(max(retry_after or 0.0, self.backoff_base ** attempt), MAX_WAIT_SEC)
            self.retries[reason] += 1
            log.warning(
                "API retry %d/%d subject=%s lastCommentId=%s reason=%s retry_after=%s wait=%.0fs detail=%r",
                attempt, self.max_retries, subject_id, last_comment_id, reason, retry_after, wait, detail,
            )
            self.sleep(wait)

        raise AssertionError("unreachable")

    @staticmethod
    def _parse(resp: requests.Response, subject_id: str, last_comment_id: int | None) -> Page:
        try:
            result = resp.json()["result"]
            results = result.get("results") or []
        except (ValueError, KeyError, TypeError, AttributeError, json.JSONDecodeError):
            raise TossApiError(
                f"예상과 다른 응답 형식 subject={subject_id} lastCommentId={last_comment_id}",
                resp.status_code, _snippet(resp),
            ) from None
        return Page(results=results, key=result.get("key"), has_next=bool(result.get("hasNext")))
