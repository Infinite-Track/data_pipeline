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
    "product_ingredient": ["id", "product_id", "raw_name", "inci_order", "source",
                           "inci_name", "safety_score", "safety_level", "is_allergen", "allergen_types",
                           "comedogenicity_rating", "irritancy_potential", "pregnancy_safe", "inci_found",
                           "inci_has_data", "inci_analyzed_at"],
    "product_inci_analysis": ["product_id", "ingredients", "effects", "overall_safety_score",
                              "inci_coverage", "ingredient_source", "schema_version", "analyzed_at"],
    # contributing_ingredients(jsonb)는 쓰지 않는다 — 기여 성분은 product_ingredient_efficacy 연결 테이블로 기록.
    "product_efficacies": ["id", "product_catalog_id", "content",
                           "contributing_ingredient_count", "composite_strength", "best_evidence_level"],
    "product_ingredient_efficacy": ["product_efficacies", "product_ingredient"],
    "reviews_keyword": ["id", "product_id", "keyword", "multiple"],
    "products_normalized": ["product_id", "dark_circle", "wrinkle", "oily_intensity", "pores", "blackhead", "acne",
                            "sensitivity", "melanin", "water", "rough", "red_spot", "skin_type_dry",
                            "skin_type_combination", "skin_type_normal", "skin_type_oily", "allergen_risk",
                            "pregnancy_risk", "comedogenicity_risk", "fragrance_risk", "irritancy_risk",
                            "summarized_at"],
}

# product_inci_analysis jsonb 안에 넣을 수 있는 키. 백엔드(api-server) Jackson 레코드와 1:1 —
# ProductIngredientAnalysis.java(parsed 항목), ProductEfficacyEffect.java(effects 항목).
# 이 목록에 없는 키가 들어가면 상세 API(GET /backend/products/{id})가 500을 낸다.
PARSED_INGREDIENT_KEYS = frozenset({
    "inciName", "safetyScore", "safetyLevel", "isAllergen", "allergenTypes",
    "comedogenicityRating", "irritancyPotential", "pregnancySafe", "found",
})
EFFECT_KEYS = frozenset({
    "target", "contributingIngredientCount", "contributingIngredients", "compositeStrength", "bestEvidenceLevel",
})


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
    """products_catalog에 asin 행이 없으면 새로 넣고 id를 반환한다(있으면 그 id를 그대로 반환).

    운영 스키마에 asin 유니크 제약이 없고 id 자동 채번도 없어서, 테이블 락을 먼저 잡고
    **락 안에서 다시 존재를 확인한 뒤** max+1로 넣는다. 락 전에만 확인하면 두 트랜잭션이
    동시에 "없음"을 보고 같은 asin을 두 번 넣을 수 있다(파이프라인 재실행·어드민 동시 쓰기)."""
    cur.execute("LOCK TABLE products_catalog IN SHARE ROW EXCLUSIVE MODE")
    cur.execute("SELECT id FROM products_catalog WHERE asin = %s ORDER BY id::int LIMIT 1", (product["asin"],))
    row = cur.fetchone()
    if row:
        return row[0]   # 락을 기다리는 사이 다른 트랜잭션이 넣은 경우
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
def _dedup(ingredients: list[str]) -> list[str]:
    """라벨 성분 목록에서 빈 값과 같은 이름을 제거한다(첫 등장 순서·표기 유지, 대소문자 무시).
    다중 구성 제품은 라벨에 'Water'가 여러 번 나오는데 INCI API도 같은 이름을 한 번만 돌려주므로,
    중복을 지워야 두 목록의 순서가 맞는다(운영 124개 상품 전부 일치 확인)."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in ingredients or []:
        name = (raw or "").strip()[:300]
        if name and name.upper() not in seen:
            seen.add(name.upper())
            out.append(name)
    return out


def _inci_by_order(ingredients: list[str], parsed: list[dict] | None) -> dict[int, dict]:
    """INCI API parsedIngredients를 라벨 성분 순서(1-based)에 맞춘다.
    API는 보낸 순서대로 돌려주므로 개수가 같으면 순서로 짝짓고, 개수가 다르면(API가 일부 항목을 빼먹는 경우가 있음)
    이름이 정확히 같은 것만 짝짓는다. 짝이 없는 성분은 INCI 컬럼을 NULL로 둔다(= 분석 안 됨)."""
    if not parsed:
        return {}
    if len(parsed) == len(ingredients):
        return dict(enumerate(parsed, 1))
    by_name = {(p.get("inciName") or "").strip().upper(): p for p in parsed}
    return {i: by_name[n] for i, raw in enumerate(ingredients, 1) if (n := (raw or "").strip().upper()) in by_name}


def publish_ingredients(conn, product_id: str, ingredients: list[str], source: str | None,
                        parsed: list[dict] | None = None, analyzed_at: str | None = None,
                        effect_links: list[tuple[int, list[str]]] | None = None) -> None:
    """성분을 product_ingredient에 발행한다(라벨 목록과 같아지도록 맞춤 → 멱등).
    source: official_site / oliveyoung / 어드민 입력(None)은 운영 기본값 'ADMIN'.
    parsed(INCI API parsedIngredients)·analyzed_at을 주면 성분별 INCI 분석 컬럼(inci_name, safety_score …)도 채운다.

    행을 전부 지우고 다시 넣지 않고 raw_name 기준으로 갱신한다(어드민 JdbcProductIngredientAdapter.replace와 같은 방식).
    행 id가 바뀌면 어드민이 입력한 성분사전 id·농도가 사라지고, id를 CASCADE로 따라가는 product_ingredient_property
    (성분 특성)까지 함께 지워지기 때문이다. 농도·성분사전 id는 어드민 몫이라 여기서 쓰지 않는다.

    effect_links = publish_efficacies()의 반환값 [(효능 id, 기여 성분 INCI 이름들)].
    성분 행을 맞춘 뒤 product_ingredient_efficacy(성분↔효능 다대다)에 연결을 기록한다.
    기존 연결은 앞선 publish_efficacies()의 효능 행 삭제 때 ON DELETE CASCADE로 이미 정리된다."""
    names = _dedup(ingredients)
    inci = _inci_by_order(names, parsed)
    with conn.cursor() as cur:
        if names:
            # 라벨에서 빠진 성분 + 같은 이름이 여러 행인 경우의 나머지(가장 오래된 행만 남김)를 정리한다.
            cur.execute(
                """DELETE FROM product_ingredient p
                    WHERE p.product_id = %s
                      AND (p.raw_name <> ALL(%s)
                           OR p.id > (SELECT min(q.id) FROM product_ingredient q
                                       WHERE q.product_id = p.product_id AND q.raw_name = p.raw_name))""",
                (product_id, names),
            )
        else:
            cur.execute("DELETE FROM product_ingredient WHERE product_id = %s", (product_id,))
        for order, name in enumerate(names, 1):
            p = inci.get(order)
            values = ((None,) * 11 if p is None else (
                p.get("inciName"), _int(p.get("safetyScore")), p.get("safetyLevel"), p.get("isAllergen"),
                Json(list(p.get("allergenTypes") or [])), _int(p.get("comedogenicityRating")),
                p.get("irritancyPotential"), p.get("pregnancySafe"), p.get("found"), p.get("hasData"),
                analyzed_at))
            cur.execute(
                """UPDATE product_ingredient
                      SET inci_order = %s, source = %s,
                          inci_name = %s, safety_score = %s, safety_level = %s, is_allergen = %s, allergen_types = %s,
                          comedogenicity_rating = %s, irritancy_potential = %s, pregnancy_safe = %s, inci_found = %s,
                          inci_has_data = %s, inci_analyzed_at = %s
                    WHERE product_id = %s AND raw_name = %s""",
                (order, source or "ADMIN") + values + (product_id, name),
            )
            if cur.rowcount == 0:
                cur.execute(
                    """INSERT INTO product_ingredient
                           (product_id, raw_name, inci_order, source,
                            inci_name, safety_score, safety_level, is_allergen, allergen_types, comedogenicity_rating,
                            irritancy_potential, pregnancy_safe, inci_found, inci_has_data, inci_analyzed_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (product_id, name, order, source or "ADMIN") + values,
                )
        for effect_id, names in effect_links or []:
            upper = [n.strip().upper() for n in names if n]
            if upper:
                cur.execute(
                    """INSERT INTO product_ingredient_efficacy (product_efficacies, product_ingredient)
                       SELECT %s, id FROM product_ingredient WHERE product_id = %s AND upper(inci_name) = ANY(%s)
                       ON CONFLICT DO NOTHING""",
                    (effect_id, product_id, upper),
                )
    conn.commit()


# ── 효능 (step6) ──────────────────────────────────────────────────────
def publish_efficacies(conn, product_id: str, effects: list[dict]) -> list[tuple[int, list[str]]]:
    """INCI API efficacySummary.topEffects를 product_efficacies에 발행(해당 상품 행 삭제 후 삽입 → 멱등).
    content = target(snake_case 그대로; 백엔드가 표시용으로 바꿈). id는 자동 채번이 없어 max+1(테이블 락).
    기여 성분은 컬럼(contributing_ingredients)에 넣지 않고 product_ingredient_efficacy 연결 테이블로만 기록한다.
    반환: [(효능 id, 기여 성분 INCI 이름들)] topEffects 순서 그대로 → publish_ingredients(effect_links=)에 넘긴다."""
    links: list[tuple[int, list[str]]] = []
    with conn.cursor() as cur:
        cur.execute("LOCK TABLE product_efficacies IN SHARE ROW EXCLUSIVE MODE")
        # 이 상품의 기존 효능 행 삭제 → product_ingredient_efficacy 연결은 ON DELETE CASCADE로 함께 삭제.
        cur.execute("DELETE FROM product_efficacies WHERE product_catalog_id = %s", (product_id,))
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM product_efficacies")
        next_id = cur.fetchone()[0] + 1
        for e in effects:
            target = (e.get("target") or "").strip()
            if not target:
                continue
            cur.execute(
                """INSERT INTO product_efficacies
                       (id, product_catalog_id, content, contributing_ingredient_count, composite_strength, best_evidence_level)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (next_id, product_id, target, _int(e.get("contributingIngredientCount")) or 0,
                 e.get("compositeStrength"), e.get("bestEvidenceLevel")),
            )
            links.append((next_id, list(e.get("contributingIngredients") or [])))
            next_id += 1
    conn.commit()
    return links


# ── INCI 분석 (step6) ─────────────────────────────────────────────────
def publish_inci_analysis(conn, product_id: str, raw: dict, declared: list[str] | None, source: str | None) -> None:
    """INCI API 원본(products.inciapi_raw_data)을 product_inci_analysis에 upsert.
    ingredients = {"parsed": parsedIngredients, "declared": 라벨 성분 리스트}, effects = efficacySummary.topEffects."""
    # 백엔드 Jackson 레코드(ProductIngredientAnalysis / ProductEfficacyEffect)가 모르는 키가 있으면
    # 상세 API가 역직렬화 실패로 500을 내므로, 백엔드가 아는 키만 남긴다(예: INCI API의 hasData 제거).
    parsed = [{k: v for k, v in item.items() if k in PARSED_INGREDIENT_KEYS}
              for item in (raw.get("parsedIngredients") or [])]
    ingredients = {"parsed": parsed, "declared": list(declared or [])}
    effects = [{k: v for k, v in item.items() if k in EFFECT_KEYS}
               for item in (((raw.get("efficacySummary") or {}).get("topEffects")) or [])]
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
