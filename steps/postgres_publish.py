"""Postgres 서빙 테이블 발행 공용 헬퍼 (운영 스키마 기준, per-step 직접 write).

운영 Postgres 스키마는 백엔드가 Flyway로 소유한다(2026-09-12 V19 기준). 그래서 이 모듈은 DDL을 실행하지 않고,
실행 전 preflight_schema()로 필요한 테이블·컬럼이 있는지만 확인한다(main.py의 preflight task).

운영 스키마 요약 (postgres_export/prod_schema_2026-09-12.md):
- products_catalog: id VARCHAR(숫자 문자열, 자동 채번 없음 → 여기서 max+1), asin(비유니크 → asin으로 조회),
  name NN, category, brand, price, thumbnail_url, mention_count, ad_ratio, content(=설명), created_at
- 하위 테이블은 전부 product_id(VARCHAR) FK: products_url(id identity), youtuber_reviews(id 자동 없음 → max+1),
  product_ingredient(단수, id serial, UNIQUE(product_id, inci_order, raw_name), source NN),
  product_inci_analysis(PK product_id, ingredients/effects jsonb), reviews_keyword(id identity, UNIQUE(product_id, keyword)),
  products_normalized(PK product_id, 점수 20종 NN)

허브는 products_catalog. 최초 생성은 step5(upsert_catalog, authoritative). 하위 step은 get_product_id로 조회하거나
get_or_create_product_id로 최소 행만 만든다(step5가 채운 값을 덮지 않음).
"""
import json
import os
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import Json


def connect():
    return psycopg2.connect(os.environ["POSTGRES_URI"])


# ── 실행 전 스키마 검사 ──────────────────────────────────────────────
# 파이프라인이 실제로 INSERT/UPDATE 하는 테이블과 컬럼(운영 스키마). 하나라도 없으면 main.py preflight가 중단한다.
REQUIRED_SCHEMA: dict[str, list[str]] = {
    "products_catalog": ["id", "asin", "name", "category", "brand", "price", "thumbnail_url",
                         "mention_count", "ad_ratio", "content", "created_at"],
    "products_url": ["id", "product_id", "url", "domain", "region", "active", "updated_at"],
    "youtuber_reviews": ["id", "product_id", "youtuber_name", "video_url", "ad_likelihood", "review"],
    "product_ingredient": ["id", "product_id", "raw_name", "inci_order", "source"],
    "product_inci_analysis": ["product_id", "ingredients", "effects", "overall_safety_score",
                              "inci_coverage", "ingredient_source", "schema_version", "analyzed_at"],
    "reviews_keyword": ["id", "product_id", "keyword", "multiple"],
    "products_normalized": ["product_id", "dark_circle", "wrinkle", "oily_intensity", "pores", "blackhead", "acne",
                            "sensitivity", "melanin", "water", "rough", "red_spot", "skin_type_dry",
                            "skin_type_combination", "skin_type_normal", "skin_type_oily", "allergen_risk",
                            "pregnancy_risk", "comedogenicity_risk", "fragrance_risk", "irritancy_risk",
                            "summarized_at"],
}


def preflight_schema(conn) -> list[str]:
    """대상 Postgres에 REQUIRED_SCHEMA의 테이블·컬럼이 전부 있는지 확인하고 문제 목록을 반환한다(빈 목록 = 통과).
    읽기 전용(information_schema 조회만)."""
    problems: list[str] = []
    with conn.cursor() as cur:
        cur.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public'")
        have: dict[str, set[str]] = {}
        for table, column in cur.fetchall():
            have.setdefault(table, set()).add(column)
    for table, cols in REQUIRED_SCHEMA.items():
        if table not in have:
            problems.append(f"테이블 없음: {table}")
            continue
        missing = [c for c in cols if c not in have[table]]
        if missing:
            problems.append(f"{table}: 컬럼 없음 {missing}")
    return problems


# ── 값 정규화 ─────────────────────────────────────────────────────────
_CATEGORIES = ("Cleansers", "Toners", "Treatments", "Moisturizers", "Sunscreens", "Face masks")
_CATEGORY_BY_LOWER = {c.lower(): c for c in _CATEGORIES}


def _canon_category(value):
    """카테고리 표기 정규화. 어드민(Spring)은 소문자('treatments')로 저장하고 운영은 'Treatments' 표기를 쓴다."""
    if not value:
        return value
    return _CATEGORY_BY_LOWER.get(str(value).strip().lower(), value)


def _num(value):
    """Mongo 숫자 → float. 어드민이 저장한 BSON Decimal128은 psycopg2가 변환하지 못한다."""
    if value is None:
        return None
    if hasattr(value, "to_decimal"):  # bson.decimal128.Decimal128
        value = value.to_decimal()
    return float(value)


def _int(value):
    return None if value is None else int(value)


# ── 카탈로그 (허브) ───────────────────────────────────────────────────
def get_product_id(conn, asin: str) -> str | None:
    """asin으로 products_catalog.id를 찾는다. 없으면 None. (asin이 유니크 제약이 아니라 가장 오래된 id를 쓴다.)"""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM products_catalog WHERE asin = %s ORDER BY id::int LIMIT 1", (asin,))
        row = cur.fetchone()
    return row[0] if row else None


def _insert_catalog(cur, product: dict) -> str:
    """products_catalog에 새 행을 넣고 id를 반환. id는 자동 채번이 없어 max+1(숫자 문자열)로 만든다.
    호출 전 같은 트랜잭션에서 테이블 락을 잡아 동시 삽입 시 id 충돌을 막는다."""
    cur.execute("LOCK TABLE products_catalog IN SHARE ROW EXCLUSIVE MODE")
    cur.execute("SELECT COALESCE(MAX(id::int), 0) + 1 FROM products_catalog WHERE id ~ '^[0-9]+$'")
    new_id = str(cur.fetchone()[0])
    cur.execute(
        """INSERT INTO products_catalog
               (id, asin, name, category, brand, price, thumbnail_url, mention_count, ad_ratio, content)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (new_id, product["asin"], product.get("name") or product["asin"], _canon_category(product.get("category")),
         product.get("brand"), _num(product.get("price")), product.get("thumbnail_url"),
         _int(product.get("mention_count")), _num(product.get("ad_ratio")), product.get("description")),
    )
    return new_id


def upsert_catalog(conn, product: dict) -> str:
    """asin 기준 카탈로그 전체 upsert(step5 authoritative). 있으면 필드 갱신, 없으면 생성. id 반환."""
    pid = get_product_id(conn, product["asin"])
    with conn.cursor() as cur:
        if pid is None:
            pid = _insert_catalog(cur, product)
        else:
            cur.execute(
                """UPDATE products_catalog SET
                       name = COALESCE(%s, name), category = COALESCE(%s, category), brand = COALESCE(%s, brand),
                       price = COALESCE(%s, price), thumbnail_url = COALESCE(%s, thumbnail_url),
                       mention_count = COALESCE(%s, mention_count), ad_ratio = COALESCE(%s, ad_ratio),
                       content = COALESCE(%s, content)
                   WHERE id = %s""",
                (product.get("name"), _canon_category(product.get("category")), product.get("brand"),
                 _num(product.get("price")), product.get("thumbnail_url"), _int(product.get("mention_count")),
                 _num(product.get("ad_ratio")), product.get("description"), pid),
            )
    conn.commit()
    return pid


def get_or_create_product_id(conn, product: dict) -> str:
    """하위 step용: 카탈로그 행이 없으면 최소(asin, name)로 생성, 있으면 그대로 두고 id만 반환(non-clobbering)."""
    pid = get_product_id(conn, product["asin"])
    if pid is None:
        with conn.cursor() as cur:
            pid = _insert_catalog(cur, {"asin": product["asin"], "name": product.get("name")})
        conn.commit()
    return pid


def upsert_url(conn, product_id: str, product: dict) -> None:
    """상품의 Amazon 구매 링크를 products_url에 발행. 같은 product_id의 amazon.com(US) 행이 있으면 갱신,
    없으면 추가. 백엔드가 관리하는 다른 링크 행은 건드리지 않는다(삭제 없음)."""
    url = product.get("product_url")
    if not url:
        return
    domain = urlparse(url).netloc or None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM products_url WHERE product_id = %s AND domain = %s AND region = 'US' ORDER BY id LIMIT 1",
            (product_id, domain),
        )
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE products_url SET url = %s, active = true, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                        (url, row[0]))
        else:
            cur.execute("INSERT INTO products_url (product_id, url, domain, region, active) VALUES (%s, %s, %s, 'US', true)",
                        (product_id, url, domain))
    conn.commit()


def publish_catalog(conn, product: dict) -> str:
    """step5용: 카탈로그 + 구매 링크를 함께 발행하고 product_id를 반환."""
    pid = upsert_catalog(conn, product)
    upsert_url(conn, pid, product)
    return pid


# ── 유튜버 리뷰 (step5 신규 / step4 기존 갱신) ─────────────────────────
def publish_youtuber_reviews(conn, product_id: str, rows: list[dict]) -> None:
    """product_id의 유튜버 리뷰를 (product_id, video_url) 기준 upsert. 새 영상은 추가, 기존 행은 값만 갱신(id 유지).
    id는 자동 채번이 없어 max+1로 만든다(테이블 락으로 동시 삽입 보호)."""
    rows = [r for r in rows if r.get("video_url")]
    if not rows:
        return
    with conn.cursor() as cur:
        cur.execute("LOCK TABLE youtuber_reviews IN SHARE ROW EXCLUSIVE MODE")
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM youtuber_reviews")
        next_id = cur.fetchone()[0] + 1
        for row in rows:
            cur.execute("SELECT id FROM youtuber_reviews WHERE product_id = %s AND video_url = %s ORDER BY id LIMIT 1",
                        (product_id, row["video_url"]))
            existing = cur.fetchone()
            if existing:
                cur.execute(
                    """UPDATE youtuber_reviews SET youtuber_name = %s, ad_likelihood = %s,
                           review = COALESCE(%s, review) WHERE id = %s""",
                    (row.get("youtuber_name"), _num(row.get("ad_likelihood")), row.get("review_summary"), existing[0]),
                )
            else:
                cur.execute(
                    """INSERT INTO youtuber_reviews (id, product_id, youtuber_name, video_url, ad_likelihood, review)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (next_id, product_id, row.get("youtuber_name"), row["video_url"],
                     _num(row.get("ad_likelihood")), row.get("review_summary")),
                )
                next_id += 1
    conn.commit()


# ── 성분 (step5 / step6) ─────────────────────────────────────────────
def publish_ingredients(conn, product_id: str, ingredients: list[str], source: str | None) -> None:
    """성분을 product_ingredient에 발행(해당 product_id 행 삭제 후 라벨순 삽입 → 멱등).
    source: official_site / oliveyoung / 어드민 입력(None)은 운영 기본값 'ADMIN'."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM product_ingredient WHERE product_id = %s", (product_id,))
        seen: set[tuple[int, str]] = set()
        for order, raw in enumerate(ingredients, 1):
            name = (raw or "").strip()[:300]
            if not name or (order, name) in seen:
                continue
            seen.add((order, name))
            cur.execute(
                "INSERT INTO product_ingredient (product_id, raw_name, inci_order, source) VALUES (%s, %s, %s, %s)",
                (product_id, name, order, source or "ADMIN"),
            )
    conn.commit()


# ── INCI 분석 (step6) ─────────────────────────────────────────────────
def publish_inci_analysis(conn, product_id: str, raw: dict, declared: list[str] | None, source: str | None) -> None:
    """INCI API 원본(products.inciapi_raw_data)을 product_inci_analysis에 upsert.
    ingredients = {"parsed": parsedIngredients, "declared": 라벨 성분 리스트}, effects = efficacySummary.topEffects."""
    ingredients = {"parsed": raw.get("parsedIngredients") or [], "declared": list(declared or [])}
    effects = ((raw.get("efficacySummary") or {}).get("topEffects")) or []
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO product_inci_analysis
                   (product_id, ingredients, effects, overall_safety_score, inci_coverage, ingredient_source,
                    schema_version, analyzed_at)
               VALUES (%s, %s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP)
               ON CONFLICT (product_id) DO UPDATE SET
                   ingredients = EXCLUDED.ingredients, effects = EXCLUDED.effects,
                   overall_safety_score = EXCLUDED.overall_safety_score, inci_coverage = EXCLUDED.inci_coverage,
                   ingredient_source = EXCLUDED.ingredient_source, schema_version = EXCLUDED.schema_version,
                   analyzed_at = EXCLUDED.analyzed_at""",
            (product_id, Json(ingredients, dumps=lambda o: json.dumps(o, ensure_ascii=False)),
             Json(effects, dumps=lambda o: json.dumps(o, ensure_ascii=False)),
             _num(raw.get("overallSafetyScore")), _num(raw.get("coverage")), source),
        )
    conn.commit()


# ── 리뷰 키워드 (step7) ────────────────────────────────────────────────
def publish_keywords(conn, product_id: str, top: list[dict]) -> None:
    """상위 키워드를 reviews_keyword에 발행(해당 product_id 행 삭제 후 삽입 → 멱등). UNIQUE(product_id, keyword)."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM reviews_keyword WHERE product_id = %s", (product_id,))
        seen: set[str] = set()
        for row in top:
            kw = (row.get("keyword") or "").strip()[:255]
            if not kw or kw in seen:
                continue
            seen.add(kw)
            cur.execute("INSERT INTO reviews_keyword (product_id, keyword, multiple) VALUES (%s, %s, %s)",
                        (product_id, kw, float(row["multiple"])))
    conn.commit()


# ── 정규화 점수 (step9) ────────────────────────────────────────────────
NORMALIZED_FIELDS = [
    "dark_circle", "wrinkle", "oily_intensity", "pores", "blackhead", "acne", "sensitivity",
    "melanin", "water", "rough", "red_spot", "skin_type_dry", "skin_type_combination",
    "skin_type_normal", "skin_type_oily", "allergen_risk", "pregnancy_risk",
    "comedogenicity_risk", "fragrance_risk", "irritancy_risk",
]


def publish_normalized(conn, product_id: str, record: dict) -> bool:
    """정규화 점수 1건을 products_normalized에 product_id 기준 upsert. 운영 컬럼이 전부 NOT NULL이라
    값이 하나라도 없으면 쓰지 않고 False를 반환한다(호출부가 경고)."""
    values = [_num(record.get(f)) for f in NORMALIZED_FIELDS]
    if any(v is None for v in values) or not record.get("summarized_at"):
        return False
    cols = ["product_id"] + NORMALIZED_FIELDS + ["summarized_at"]
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "product_id")
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO products_normalized ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT (product_id) DO UPDATE SET {updates}",
            [product_id] + values + [record["summarized_at"]],
        )
    conn.commit()
    return True
