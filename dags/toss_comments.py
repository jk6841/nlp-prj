"""토스증권 커뮤니티 댓글 수집 DAG.

웹 UI가 내부에서 호출하는 댓글 API(GET /api/v4/comments)를 호출해 응답을 로그로 남긴다.
지금은 첫 페이지 1회 호출 후 로그 출력까지만 한다. 적재(PostgreSQL)는 이후 단계에서 추가한다.
"""

import logging
import uuid

import pendulum
import requests
from airflow.sdk import dag, task

log = logging.getLogger(__name__)

API_URL = "https://wts-cert-api.tossinvest.com/api/v4/comments"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["toss", "comments"],
    params={
        "subject_id": "US20100311002",  # SOXL
        "sort_type": "RECENT",  # RECENT | POPULAR
    },
)
def toss_comments():
    @task
    def fetch_comments(params=None):
        query = {
            "subjectType": "STOCK",
            "subjectId": params["subject_id"],
            "commentSortType": params["sort_type"],
        }
        headers = {
            # Cookie에 deviceId만 있으면 값과 상관없이 요청이 성공한다.
            "Cookie": f"deviceId={uuid.uuid4()}",
            # requests 기본 UA(python-requests/*)는 403으로 막힌다.
            "User-Agent": "Mozilla/5.0",
        }

        resp = requests.get(API_URL, params=query, headers=headers, timeout=30)
        resp.raise_for_status()
        result = resp.json()["result"]

        log.info(
            "totalCount=%s hasNext=%s key=%s count=%d",
            result.get("totalCount"),
            result.get("hasNext"),
            result.get("key"),
            len(result["results"]),
        )
        for c in result["results"]:
            msg = c.get("message") or {}
            log.info(
                "[%s] %s | %s | %s",
                c.get("commentId"),
                c.get("createdAt"),
                msg.get("title", ""),
                msg.get("message", ""),
            )

    fetch_comments()


toss_comments()
