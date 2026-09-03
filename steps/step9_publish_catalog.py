"""
STEP 9 · 발행(publish): Mongo → PostgreSQL 서빙 테이블

Amazon 해결·성분·효능 분석이 끝난 상품을 PostgreSQL 서빙 테이블로 내보낸다.
  - products_url        : 제휴링크 (url, domain)
  - products_catalog    : 카탈로그 (asin, name, category, brand, price, thumnail_url,
                          mention_count, ad_ratio, description, products_url_id)
  - products_normalized : 효능 정규화 점수 (asin + 점수 컬럼들)
  - youtuber_reviews    : 상품별 유튜버 리뷰 (asin, product_name, youtuber_name, video_url,
                          ad_likelihood, review) — review는 저작권·길이 문제로 원문이 아니라
                          Gemini가 1문장(영어)으로 요약한 것. 요약은 raw_reviews.review_summary에
                          캐시해 매 발행마다 재요약하지 않는다.

네 테이블은 asin으로 연결된다. Mongo products와 products_normalized를 asin으로 조인해,
둘 다 있는(=카탈로그+점수 완비) 상품만 발행한다. youtuber_reviews도 그 asin의 리뷰만 넣는다.

멱등성: 매 실행마다 테이블을 DROP 후 재생성한다. DDL+INSERT를 한 트랜잭션으로 커밋하므로
읽는 쪽은 커밋 전까지 기존 데이터를 본다. (Gemini 요약은 트랜잭션 밖에서 먼저 처리)

필요 환경변수: MONGODB_URI, MONGODB_DB_NAME, POSTGRES_URI, GEMINI_API_KEY
"""
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from dotenv import load_dotenv
from loguru import logger
from pymongo import MongoClient

from steps.gemini_client import get_client

load_dotenv()

GEMINI_MODEL = "gemini-2.5-flash"

# 리뷰 요약 Gemini 호출 동시 실행 수(병렬).
CONCURRENCY = 8

# products_normalized에서 텍스트로 저장할 필드(나머지는 전부 DOUBLE PRECISION 점수로 본다).
NORMALIZED_TEXT_FIELDS = {"asin", "product_name", "brand", "category", "summarized_at", "ingredient_source"}


def _pg_ident(name: str) -> str:
    """식별자를 큰따옴표로 감싼다 (점수 필드명이 예약어와 겹칠 경우 대비)."""
    return '"' + name.replace('"', '') + '"'


# ── Gemini 리뷰 요약 (raw_reviews.review_summary에 캐시) ────────────────────
def _summarize(client, content: str) -> str | None:
    prompt = (
        "Summarize the following YouTube product review in ONE concise, factual English sentence. "
        "Paraphrase in your own words — do NOT copy phrases verbatim. Output only the sentence.\n\n"
        f"Review:\n{content}"
    )
    try:
        resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
        text = (resp.text or "").strip()
        return text or None
    except Exception as exc:
        logger.warning(f"[STEP9] 요약 실패: {exc}")
        return None


def _get_summary(mdb, raw: dict, client) -> str | None:
    """캐시된 요약이 있으면 재사용, 없으면 생성 후 raw_reviews에 저장."""
    if raw.get("review_summary"):
        return raw["review_summary"]
    content = raw.get("content")
    if not content or not client:
        return None
    summary = _summarize(client, content)
    if summary:
        mdb["raw_reviews"].update_one({"_id": raw["_id"]}, {"$set": {"review_summary": summary}})
    return summary


# ── PostgreSQL DDL ─────────────────────────────────────────────────────
def _create_catalog_tables(cur) -> None:
    cur.execute("DROP TABLE IF EXISTS youtuber_reviews CASCADE")
    cur.execute("DROP TABLE IF EXISTS products_catalog CASCADE")
    cur.execute("DROP TABLE IF EXISTS products_url CASCADE")
    cur.execute("""
        CREATE TABLE products_url (
            id     SERIAL PRIMARY KEY,
            url    TEXT NOT NULL,
            domain TEXT
        )""")
    cur.execute("""
        CREATE TABLE products_catalog (
            id              SERIAL PRIMARY KEY,
            asin            TEXT UNIQUE NOT NULL,
            name            TEXT NOT NULL,
            category        TEXT,
            brand           TEXT,
            price           NUMERIC,
            thumnail_url    TEXT,
            mention_count   INTEGER,
            ad_ratio        DOUBLE PRECISION,
            description     TEXT,
            products_url_id INTEGER REFERENCES products_url(id)
        )""")
    cur.execute("""
        CREATE TABLE youtuber_reviews (
            id            SERIAL PRIMARY KEY,
            asin          TEXT REFERENCES products_catalog(asin),
            product_name  TEXT,
            youtuber_name TEXT,
            video_url     TEXT,
            ad_likelihood DOUBLE PRECISION,
            review        TEXT
        )""")


def _create_normalized_table(cur, numeric_fields: list[str]) -> None:
    cur.execute("DROP TABLE IF EXISTS products_normalized CASCADE")
    cols = [
        "asin TEXT PRIMARY KEY",
        "product_name TEXT",
        "brand TEXT",
        "category TEXT",
        "summarized_at TEXT",
    ]
    cols += [f"{_pg_ident(f)} DOUBLE PRECISION" for f in numeric_fields]
    cur.execute(f"CREATE TABLE products_normalized ({', '.join(cols)})")


# ── 유튜버 리뷰 행 수집 (Gemini 요약 포함, 트랜잭션 밖에서 미리 처리) ──────────
def _gather_youtuber_reviews(mdb, products: dict, publishable_asins: set) -> list[tuple]:
    client = get_client()
    videos = {v["video_id"]: v for v in mdb["videos"].find({}, {"video_id": 1, "url": 1, "youtuber_id": 1})}
    yt_name = {y.get("channel_id"): y.get("channel_name") for y in mdb["youtubers"].find({})}

    raw_by_id, raw_by_key = {}, {}
    for r in mdb["raw_reviews"].find({}):
        raw_by_id[r["_id"]] = r
        raw_by_key[(r.get("video_id"), r.get("product_name"))] = r

    # 1) 발행 대상 리뷰 + 매칭 raw 수집
    items = []
    for review in mdb["reviews"].find({"status": "COMPLETED", "asin": {"$in": list(publishable_asins)}}):
        raw = raw_by_id.get(review.get("raw_review_id")) or raw_by_key.get((review.get("video_id"), review.get("product_name")))
        if raw is None:
            continue  # 원문(raw_review)을 못 찾으면 요약 불가 → 건너뜀
        video = videos.get(review.get("video_id")) or {}
        items.append((review, raw, video, video.get("youtuber_id") or raw.get("youtuber_id")))

    # 2) Gemini 1문장 요약을 병렬 생성(캐시). 동시 CONCURRENCY개.
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        summaries = list(ex.map(lambda it: _get_summary(mdb, it[1], client), items))

    # 3) 행 조립
    rows = []
    for (review, raw, video, youtuber_id), summary in zip(items, summaries):
        asin = review["asin"]
        rows.append((
            asin,
            products[asin].get("name"),        # Amazon 공식명
            yt_name.get(youtuber_id),          # 유튜버 채널명
            video.get("url"),                  # 영상 링크
            review.get("ad_likelihood"),
            summary,                           # Gemini 1문장 요약(캐시)
        ))
    return rows


def _publish() -> None:
    mdb = MongoClient(os.environ["MONGODB_URI"])[os.environ["MONGODB_DB_NAME"]]

    products = {p["asin"]: p for p in mdb["products"].find({"asin": {"$exists": True}}, {"_id": 0})}
    normalized = list(mdb["products_normalized"].find({"asin": {"$exists": True}}, {"_id": 0}))
    publishable = [n for n in normalized if n.get("asin") in products]

    if not publishable:
        logger.warning("[STEP9] 발행 대상이 없습니다 (asin 있는 products ∩ products_normalized = 0). "
                       "재설계된 파이프라인(step4~8)이 먼저 돌아야 합니다.")
        return

    publishable_asins = {n["asin"] for n in publishable}
    numeric_fields = sorted({k for n in publishable for k in n if k not in NORMALIZED_TEXT_FIELDS})

    # Gemini 요약은 느리므로 PG 트랜잭션 밖에서 먼저 수집(+캐시).
    review_rows = _gather_youtuber_reviews(mdb, products, publishable_asins)

    conn = psycopg2.connect(os.environ["POSTGRES_URI"])
    try:
        with conn:  # 트랜잭션: 성공 시 커밋, 예외 시 롤백
            with conn.cursor() as cur:
                _create_catalog_tables(cur)
                _create_normalized_table(cur, numeric_fields)

                for n in publishable:
                    asin = n["asin"]
                    p = products[asin]
                    cur.execute(
                        "INSERT INTO products_url (url, domain) VALUES (%s, %s) RETURNING id",
                        (p.get("product_url"), "www.amazon.com"),
                    )
                    url_id = cur.fetchone()[0]
                    cur.execute(
                        """INSERT INTO products_catalog
                           (asin, name, category, brand, price, thumnail_url,
                            mention_count, ad_ratio, description, products_url_id)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (asin, p.get("name"), p.get("category"), p.get("brand"), p.get("price"),
                         p.get("thumbnail_url"), p.get("mention_count"), p.get("ad_ratio"),
                         p.get("description"), url_id),
                    )
                    cols = ["asin", "product_name", "brand", "category", "summarized_at"] + numeric_fields
                    vals = [asin, n.get("product_name"), n.get("brand"), n.get("category"),
                            n.get("summarized_at")] + [n.get(f) for f in numeric_fields]
                    cur.execute(
                        f"INSERT INTO products_normalized ({', '.join(_pg_ident(c) for c in cols)}) "
                        f"VALUES ({', '.join(['%s'] * len(cols))})",
                        vals,
                    )

                if review_rows:
                    cur.executemany(
                        """INSERT INTO youtuber_reviews
                           (asin, product_name, youtuber_name, video_url, ad_likelihood, review)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        review_rows,
                    )
        logger.info(f"[STEP9] {len(publishable)}개 상품 / {len(review_rows)}개 유튜버 리뷰 발행 완료.")
    finally:
        conn.close()


async def run() -> None:
    await asyncio.to_thread(_publish)


if __name__ == "__main__":
    asyncio.run(run())
