"""Postgres 서빙 테이블 발행 공용 헬퍼 (per-step 직접 write).

products_catalog가 허브 테이블(step4가 최초 생성). 나머지 서빙 테이블은 product_catalog_id로
이를 참조한다. 각 step은 자기 테이블을 ensure_*로 보장하고 upsert 한다(멱등).

- upsert_catalog(): step4 전용. 카탈로그 필드를 전부 덮어쓰는 authoritative upsert.
- get_or_create_catalog_id(): 하위 step(step5~9)용. 없으면 최소 생성, 있으면 건드리지 않고 id만.
  (step4가 채운 name/description 등을 하위 step이 null로 덮어쓰지 않도록.)
"""
import os
from urllib.parse import urlparse

import psycopg2


def connect():
    return psycopg2.connect(os.environ["POSTGRES_URI"])


def ensure_catalog_schema(conn) -> None:
    """products_catalog(허브) + products_url 생성(없으면)."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products_catalog (
                id            SERIAL PRIMARY KEY,
                asin          TEXT UNIQUE NOT NULL,
                name          TEXT,
                category      TEXT,
                brand         TEXT,
                price         NUMERIC,
                thumbnail_url  TEXT,
                mention_count INTEGER,
                ad_ratio      DOUBLE PRECISION,
                description   TEXT,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
        # 기존 테이블에도 created_at 보장(멱등). upsert가 이 컬럼을 건드리지 않으므로 최초 삽입 시각이 유지된다.
        cur.execute("ALTER TABLE products_catalog ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products_url (
                id                 SERIAL PRIMARY KEY,
                product_catalog_id INTEGER REFERENCES products_catalog(id),
                url                TEXT,
                domain             TEXT
            )""")
    conn.commit()


def upsert_catalog(conn, product: dict) -> int:
    """asin 기준 카탈로그 전체 upsert(step4 authoritative). id 반환."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO products_catalog
                   (asin, name, category, brand, price, thumbnail_url, mention_count, ad_ratio, description)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (asin) DO UPDATE SET
                   name = EXCLUDED.name, category = EXCLUDED.category, brand = EXCLUDED.brand,
                   price = EXCLUDED.price, thumbnail_url = EXCLUDED.thumbnail_url,
                   mention_count = EXCLUDED.mention_count, ad_ratio = EXCLUDED.ad_ratio,
                   description = EXCLUDED.description
               RETURNING id""",
            (product["asin"], product.get("name"), product.get("category"),
             product.get("brand"), product.get("price"), product.get("thumbnail_url"),
             product.get("mention_count"), product.get("ad_ratio"), product.get("description")),
        )
        cid = cur.fetchone()[0]
    conn.commit()
    return cid


def get_or_create_catalog_id(conn, product: dict) -> int:
    """카탈로그 행이 없으면 최소(asin,name)로 생성, 있으면 그대로 두고 id만 반환(non-clobbering).
    ON CONFLICT DO UPDATE SET asin=EXCLUDED.asin 는 값을 안 바꾸면서 RETURNING을 성립시키는 트릭."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO products_catalog (asin, name)
               VALUES (%s, %s)
               ON CONFLICT (asin) DO UPDATE SET asin = EXCLUDED.asin
               RETURNING id""",
            (product["asin"], product.get("name") or product["asin"]),
        )
        cid = cur.fetchone()[0]
    conn.commit()
    return cid


def upsert_url(conn, catalog_id: int, product: dict) -> None:
    """상품의 판매 URL을 products_url에 발행(기존 행 삭제 후 삽입 → 멱등)."""
    url = product.get("product_url")
    with conn.cursor() as cur:
        cur.execute("DELETE FROM products_url WHERE product_catalog_id = %s", (catalog_id,))
        if url:
            cur.execute(
                "INSERT INTO products_url (product_catalog_id, url, domain) VALUES (%s, %s, %s)",
                (catalog_id, url, urlparse(url).netloc or None),
            )
    conn.commit()


def publish_catalog(conn, product: dict) -> int:
    """step4용: 카탈로그 + URL을 함께 발행하고 catalog_id를 반환."""
    cid = upsert_catalog(conn, product)
    upsert_url(conn, cid, product)
    return cid


# ── 유튜버 리뷰 (step5 신규 / step4 기존 갱신) ─────────────────────────
def ensure_youtuber_reviews_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS youtuber_reviews (
                id             SERIAL PRIMARY KEY,
                asin           TEXT REFERENCES products_catalog(asin),
                video_url      TEXT,
                youtuber_name  TEXT,
                review_summary TEXT
            )""")
        # (asin, video_url)이 행의 정체성. upsert 충돌 키로 쓰며, 재실행 시 중복 행이 생기지 않게 한다.
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_youtuber_reviews_asin_video "
                    "ON youtuber_reviews (asin, video_url)")
    conn.commit()


def catalog_exists(conn, asin: str) -> bool:
    """products_catalog에 해당 asin 행이 있는지(= 이미 발행된 상품인지)."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM products_catalog WHERE asin = %s", (asin,))
        return cur.fetchone() is not None


def publish_youtuber_reviews(conn, asin: str, rows: list[dict]) -> None:
    """asin의 유튜버 리뷰를 youtuber_reviews에 (asin, video_url) 기준 upsert한다(멱등).
    새 영상 리뷰는 행이 추가되고, 이미 있는 행은 id를 유지한 채 값만 갱신된다.
    FK(asin→catalog) 때문에 catalog 행이 먼저 있어야 한다(호출 전 보장)."""
    with conn.cursor() as cur:
        for row in rows:
            if not row.get("video_url"):
                continue  # 충돌 키가 없는 행은 넣지 않는다(videos 조인 실패 케이스)
            cur.execute(
                """INSERT INTO youtuber_reviews (asin, video_url, youtuber_name, review_summary)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (asin, video_url) DO UPDATE SET
                       youtuber_name = EXCLUDED.youtuber_name,
                       review_summary = EXCLUDED.review_summary""",
                (asin, row["video_url"], row.get("youtuber_name"), row.get("review_summary")),
            )
    conn.commit()


# ── 리뷰 키워드 (step7) ────────────────────────────────────────────────
def ensure_reviews_keyword_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reviews_keyword (
                id                 SERIAL PRIMARY KEY,
                product_catalog_id INTEGER REFERENCES products_catalog(id),
                keyword            VARCHAR(255),
                multiple           DOUBLE PRECISION
            )""")
    conn.commit()


def publish_keywords(conn, product: dict, top: list[dict]) -> int:
    """상품의 상위 키워드를 reviews_keyword에 발행(기존 행 삭제 후 삽입 → 멱등).
    catalog 행은 non-clobbering get-or-create(step4가 채운 필드를 덮지 않음)."""
    cid = get_or_create_catalog_id(conn, product)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM reviews_keyword WHERE product_catalog_id = %s", (cid,))
        for row in top:
            cur.execute(
                "INSERT INTO reviews_keyword (product_catalog_id, keyword, multiple) VALUES (%s, %s, %s)",
                (cid, row["keyword"], row["multiple"]),
            )
    conn.commit()
    return cid


# ── 정규화 점수 (step9) ────────────────────────────────────────────────
# products_normalized(Postgres) 점수 컬럼 20종 (asin + 이들 + summarized_at).
NORMALIZED_FIELDS = [
    "dark_circle", "wrinkle", "oily_intensity", "pores", "blackhead", "acne", "sensitivity",
    "melanin", "water", "rough", "red_spot", "skin_type_dry", "skin_type_combination",
    "skin_type_normal", "skin_type_oily", "allergen_risk", "pregnancy_risk",
    "comedogenicity_risk", "fragrance_risk", "irritancy_risk",
]


def ensure_normalized_schema(conn) -> None:
    cols = ",\n                ".join(f"{f} DOUBLE PRECISION" for f in NORMALIZED_FIELDS)
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS products_normalized (
                asin          TEXT PRIMARY KEY REFERENCES products_catalog(asin),
                {cols},
                summarized_at TIMESTAMP
            )""")
    conn.commit()


def publish_normalized(conn, record: dict) -> None:
    """정규화 점수 1건을 products_normalized에 asin 기준 upsert(멱등).
    FK(asin→catalog)를 위해 카탈로그 행을 먼저 보장한다."""
    get_or_create_catalog_id(conn, {"asin": record["asin"], "name": record.get("product_name")})
    cols = ["asin"] + NORMALIZED_FIELDS + ["summarized_at"]
    vals = [record.get(c) for c in cols]
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "asin")
    with conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO products_normalized ({', '.join(cols)})
                VALUES ({placeholders})
                ON CONFLICT (asin) DO UPDATE SET {updates}""",
            vals,
        )
    conn.commit()


# ── 효능 (step6) ──────────────────────────────────────────────────────
def ensure_efficacies_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS product_efficacies (
                id                 SERIAL PRIMARY KEY,
                product_catalog_id INTEGER REFERENCES products_catalog(id),
                content            TEXT
            )""")
    conn.commit()


def publish_efficacy(conn, product: dict, efficacy, functions: list[str] | None = None) -> int:
    """상품 효능을 product_efficacies에 발행(멱등): 효능 하나당 행 1개, content = 효능명.
    - INCI efficacySummary.topEffects의 target (소문자, 예: 'hydration', 'barrier_repair')
    - 어드민이 입력한 식약처 기능성 function 라벨 (대문자, 예: 'WHITENING', 'UV_PROTECTION')
    기존 행 삭제 후 재삽입. 둘 다 비면 행 없음."""
    cid = get_or_create_catalog_id(conn, product)
    names: list[str] = []
    for effect in ((efficacy or {}).get("topEffects") or []) if isinstance(efficacy, dict) else []:
        target = (effect.get("target") or "").strip()
        if target and target not in names:
            names.append(target)
    for fn in functions or []:
        fn = (fn or "").strip()
        if fn and fn not in names:
            names.append(fn)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM product_efficacies WHERE product_catalog_id = %s", (cid,))
        for name in names:
            cur.execute(
                "INSERT INTO product_efficacies (product_catalog_id, content) VALUES (%s, %s)",
                (cid, name),
            )
    conn.commit()
    return cid


# ── 성분 (step5) ──────────────────────────────────────────────────────
def ensure_ingredients_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS product_ingredients (
                id                 SERIAL PRIMARY KEY,
                product_catalog_id INTEGER REFERENCES products_catalog(id),
                ingredient_id      INTEGER,
                raw_name           TEXT,
                inci_order         INTEGER,
                concentration_min  DOUBLE PRECISION,
                concentration_max  DOUBLE PRECISION,
                concentration_unit TEXT,
                source             TEXT
            )""")
        # 기존 테이블에도 신규 컬럼 보장(멱등).
        cur.execute("ALTER TABLE product_ingredients ADD COLUMN IF NOT EXISTS concentration_max DOUBLE PRECISION")
        cur.execute("ALTER TABLE product_ingredients ADD COLUMN IF NOT EXISTS concentration_unit TEXT")
    conn.commit()


def publish_ingredients(conn, product: dict, ingredients: list[str], source: str | None) -> int:
    """성분을 product_ingredients에 발행(기존 행 삭제 후 라벨순 삽입 → 멱등).
    지금은 raw_name / inci_order / source만 채우고 ingredient_id·concentration_min은 NULL."""
    cid = get_or_create_catalog_id(conn, product)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM product_ingredients WHERE product_catalog_id = %s", (cid,))
        for order, raw in enumerate(ingredients, 1):
            cur.execute(
                """INSERT INTO product_ingredients (product_catalog_id, raw_name, inci_order, source)
                   VALUES (%s, %s, %s, %s)""",
                (cid, raw, order, source),
            )
    conn.commit()
    return cid
