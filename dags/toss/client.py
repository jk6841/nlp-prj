"""토스증권 커뮤니티 댓글 API 클라이언트.

웹 UI가 내부에서 호출하는 댓글 API(GET /api/v4/comments)를 쓴다.
최신순(RECENT)으로만 받고, lastCommentId 커서로 과거 방향 페이지를 넘긴다.
"""

import time
import uuid
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = "https://wts-cert-api.tossinvest.com/api/v4/comments"


@dataclass
class Page:
    results: list[dict]
    key: int | None  # 이 페이지 마지막 댓글 ID. 다음 요청의 lastCommentId
    has_next: bool


class TossClient:
    def __init__(self, delay_sec: float = 1.0, timeout: float = 30.0):
        self.delay_sec = delay_sec
        self.timeout = timeout
        self._last_request_at = 0.0

        self.session = requests.Session()
        self.session.headers.update(
            {
                # Cookie에 deviceId만 있으면 값과 상관없이 요청이 성공한다.
                "Cookie": f"deviceId={uuid.uuid4()}",
                # requests 기본 UA(python-requests/*)는 403으로 막힌다.
                "User-Agent": "Mozilla/5.0",
            }
        )
        retry = Retry(
            total=5,
            backoff_factor=2,  # 2, 4, 8, 16, 32초
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def _throttle(self) -> None:
        wait = self.delay_sec - (time.monotonic() - self._last_request_at)
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def fetch_page(self, subject_id: str, last_comment_id: int | None = None) -> Page:
        params = {
            "subjectType": "STOCK",
            "subjectId": subject_id,
            "commentSortType": "RECENT",
        }
        if last_comment_id is not None:
            params["lastCommentId"] = last_comment_id

        self._throttle()
        resp = self.session.get(API_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()  # 403 등은 재시도하지 않고 그대로 실패시킨다.
        result = resp.json()["result"]
        return Page(
            results=result.get("results") or [],
            key=result.get("key"),
            has_next=bool(result.get("hasNext")),
        )
