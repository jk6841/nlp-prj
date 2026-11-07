"""nlp-postgres 적재 로직. 테이블은 sql/schema.sql로 미리 만들어 둔다."""

from contextlib import closing
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

CONN_ID = "nlp-postgres"

def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONN_ID)


def upsert_stocks(subjects: list[dict]) -> None:
    rows = [(s["subject_id"], s["name"], bool(s.get("enabled", False))) for s in subjects]
    # closing: 연결을 닫는다. conn: 블록이 끝나면 commit(예외 시 rollback)한다.
    with closing(_hook().get_conn()) as conn, conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO stocks (stock_id, ticker, is_active) VALUES %s
            ON CONFLICT (stock_id) DO UPDATE
               SET ticker = EXCLUDED.ticker,
                   is_active = EXCLUDED.is_active,
                   updated_at = now()
            """,
            rows,
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
    """중복은 건너뛰고 실제로 들어간 행 수를 반환한다."""
    if not rows:
        return 0
    # closing: 연결을 닫는다. conn: 블록이 끝나면 commit(예외 시 rollback)한다.
    with closing(_hook().get_conn()) as conn, conn, conn.cursor() as cur:
        inserted = execute_values(
            cur,
            """
            INSERT INTO comments
                (comment_id, stock_id, author_id, title, message, created_at,
                 like_count, reply_count)
            VALUES %s
            ON CONFLICT (comment_id) DO NOTHING
            RETURNING comment_id
            """,
            rows,
            fetch=True,
        )
    return len(inserted)
