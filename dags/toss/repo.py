"""nlp-postgres 적재 로직. 테이블은 sql/schema.sql로 미리 만들어 둔다.

postgres provider는 설치 환경에 따라 psycopg2 또는 psycopg3 연결을 돌려준다.
드라이버 전용 헬퍼(psycopg2.extras.execute_values 등) 없이, 컬럼별 리스트를 배열로 넘겨
unnest로 펼치는 SQL 한 문장으로 적재해 두 드라이버에서 똑같이 동작하게 한다.
"""

from contextlib import closing
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "nlp-postgres"

def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONN_ID)


def _execute(sql: str, params: tuple) -> int:
    """한 문장을 실행하고 commit한 뒤 영향받은 행 수를 반환한다."""
    with closing(_hook().get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            count = cur.rowcount
        conn.commit()
    return count


def upsert_stocks(subjects: list[dict]) -> None:
    if not subjects:
        return
    _execute(
        """
        INSERT INTO stocks (stock_id, ticker)
        SELECT * FROM unnest(%s::text[], %s::text[])
        ON CONFLICT (stock_id) DO UPDATE
           SET ticker = EXCLUDED.ticker,
               updated_at = now()
        """,
        ([s["subject_id"] for s in subjects], [s["name"] for s in subjects]),
    )


def cursor_before(stock_id: str, end: datetime) -> int | None:
    """end 이후(최신 쪽)에 이미 저장된 댓글 중 가장 오래된 ID.

    이 ID를 lastCommentId로 주면 end 근처부터 받기 시작해 지나온 페이지를 다시 받지 않는다.
    없으면 None (최신부터 받는다).
    """
    row = _hook().get_first(
        "SELECT min(comment_id) FROM comments WHERE stock_id = %s AND created_at >= %s",
        parameters=(stock_id, end),
    )
    return row[0] if row else None


def upsert_comments(rows: list[tuple]) -> tuple[int, int]:
    """댓글을 넣거나, 이미 있으면 바뀐 값만 갱신한다. (inserted, updated)를 반환한다.

    rows의 각 원소는 toss.collect.to_row()가 만든 튜플이다.
    - 새 댓글: insert
    - 이미 있는 댓글: 수정될 수 있는 값(title, message, like_count, reply_count, holding_status)이
      하나라도 달라졌을 때만 갱신하고 collected_at을 now()로 바꾼다. 같으면 건드리지 않는다.
      collected_at은 "저장된 값을 마지막으로 받아 쓴 시각"이 된다.
    - comment_id, stock_id, author_id, created_at은 바뀌지 않는 값이라 갱신하지 않는다.
    같은 구간을 다시 돌리면(예: backfill --reprocess-behavior completed) 수정된 댓글이 반영된다.
    """
    if not rows:
        return 0, 0
    cols = list(zip(*rows))
    with closing(_hook().get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO comments
                    (comment_id, stock_id, author_id, holding_status, title, message, created_at,
                     like_count, reply_count)
                SELECT * FROM unnest(
                    %s::bigint[], %s::text[], %s::bigint[], %s::text[], %s::text[], %s::text[],
                    %s::timestamptz[], %s::int[], %s::int[]
                )
                ON CONFLICT (comment_id) DO UPDATE
                   SET title          = EXCLUDED.title,
                       message        = EXCLUDED.message,
                       like_count     = EXCLUDED.like_count,
                       reply_count    = EXCLUDED.reply_count,
                       holding_status = EXCLUDED.holding_status,
                       collected_at   = now()
                 WHERE (comments.title, comments.message, comments.like_count,
                        comments.reply_count, comments.holding_status)
                       IS DISTINCT FROM
                       (EXCLUDED.title, EXCLUDED.message, EXCLUDED.like_count,
                        EXCLUDED.reply_count, EXCLUDED.holding_status)
                RETURNING (xmax = 0) AS inserted
                """,
                tuple(list(c) for c in cols),
            )
            flags = [r[0] for r in cur.fetchall()]  # 바뀐 게 없는 기존 행은 반환되지 않는다
        conn.commit()
    inserted = sum(1 for f in flags if f)
    return inserted, len(flags) - inserted
