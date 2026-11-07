"""data interval 하나([start, end))의 댓글을 모으는 페이지 루프.

API는 최신 → 과거 방향으로만 넘어가고, commentId 내림차순과 createdAt 내림차순이 같다.
그래서 커서부터 받으면서
  - created_at >= end   : 이 구간보다 최신이라 건너뛴다
  - start <= created_at : 모은다 (작성자는 authorUserProfileId만 남긴다)
  - created_at < start  : 구간을 지나쳤으니 멈춘다
Airflow에 의존하지 않아 단위 테스트할 수 있다.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

import pendulum

from toss.client import Page

log = logging.getLogger(__name__)

FetchPage = Callable[[str, int | None], Page]


@dataclass
class CollectStats:
    pages: int = 0
    skipped: int = 0  # end 이후라 건너뛴 댓글 수
    collected: int = 0
    truncated: bool = False  # max_pages에 걸려 구간 끝까지 못 갔는지


def to_row(stock_id: str, c: dict, created_at: datetime) -> tuple:
    msg = c.get("message") or {}
    stat = c.get("statistic") or {}
    return (
        int(c["commentId"]),
        stock_id,
        c.get("authorUserProfileId"),
        (c.get("holding") or {}).get("shareHoldingStatus"),
        msg.get("title"),
        msg.get("message"),
        created_at,
        stat.get("likeCount"),
        stat.get("replyCount"),
    )


def collect_interval(
    fetch_page: FetchPage,
    subject_id: str,
    cursor: int | None,
    start: datetime,
    end: datetime,
    max_pages: int | None = None,
    progress_every: int = 50,
) -> tuple[list[tuple], CollectStats]:
    """progress_every 페이지마다 진행 상황을 INFO로 남긴다.

    페이지 요청이나 파싱이 실패하면 어디까지 진행했는지(lastCommentId, 페이지 수 등)를
    ERROR로 남기고 예외를 그대로 다시 던진다.
    """
    rows: list[tuple] = []
    stats = CollectStats()
    last_id = cursor
    oldest_seen = None

    try:
        while True:
            if max_pages is not None and stats.pages >= max_pages:
                stats.truncated = True
                break

            page = fetch_page(subject_id, last_id)
            stats.pages += 1

            for c in page.results:
                created_at = pendulum.parse(c["createdAt"])
                oldest_seen = created_at
                if created_at >= end:
                    stats.skipped += 1
                elif created_at >= start:
                    rows.append(to_row(subject_id, c, created_at))
                else:
                    stats.collected = len(rows)
                    return rows, stats

            if progress_every and stats.pages % progress_every == 0:
                log.info(
                    "progress subject=%s pages=%d collected=%d skipped=%d lastCommentId=%s oldest_seen=%s",
                    subject_id, stats.pages, len(rows), stats.skipped, page.key, oldest_seen,
                )

            if not page.has_next or page.key is None:
                break
            last_id = page.key
    except Exception:
        log.error(
            "collect failed subject=%s interval=[%s, %s) cursor=%s lastCommentId=%s "
            "pages=%d collected=%d skipped=%d oldest_seen=%s",
            subject_id, start, end, cursor, last_id,
            stats.pages, len(rows), stats.skipped, oldest_seen,
        )
        raise

    stats.collected = len(rows)
    return rows, stats
