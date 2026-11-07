-- 토스증권 댓글 수집용 스키마. 배포 전에 nlp-postgres에서 수동으로 1회 실행한다.
--   psql "$NLP_POSTGRES_URL" -f sql/schema.sql
CREATE TABLE IF NOT EXISTS stocks (
    stock_id   TEXT        PRIMARY KEY,
    ticker     TEXT        NOT NULL,
    is_active  BOOLEAN     NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS comments (
    comment_id   BIGINT      PRIMARY KEY,
    stock_id     TEXT        NOT NULL REFERENCES stocks (stock_id),
    title        TEXT,
    message      TEXT,
    created_at   TIMESTAMPTZ NOT NULL,
    like_count   INTEGER,
    reply_count  INTEGER,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS comments_stock_created_idx
    ON comments (stock_id, created_at);
