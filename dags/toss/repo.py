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


def insert_comments(rows: list[tuple]) -> int:
    """중복은 건너뛰고 실제로 들어간 행 수를 반환한다.

    rows의 각 원소는 toss.collect.to_row()가 만든 튜플이다.
    ON CONFLICT DO NOTHING으로 건너뛴 행은 rowcount에 잡히지 않는다.
    """
    if not rows:
        return 0
    cols = list(zip(*rows))
    return _execute(
        """
        INSERT INTO comments
            (comment_id, stock_id, author_id, holding_status, title, message, created_at,
             like_count, reply_count)
        SELECT * FROM unnest(
            %s::bigint[], %s::text[], %s::bigint[], %s::text[], %s::text[], %s::text[],
            %s::timestamptz[], %s::int[], %s::int[]
        )
        ON CONFLICT (comment_id) DO NOTHING
        """,
        tuple(list(c) for c in cols),
    )
