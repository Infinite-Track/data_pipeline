"""
STEP 5 · 성분 추출 (상품에 성분 추가)

Gemini + Google 검색 그라운딩으로 공식 소스에서 성분(INCI)·name_kr·올리브영 prdt_no를 받는다.
prdt_no가 있으면 올리브영 글로벌 상품정보 API를 우선 호출해 정형 '045' 전성분을 쓴다
(그라운딩 텍스트보다 신뢰도 높음). OY가 없거나 요약본(truncated)이면 그라운딩 성분을 쓴다.
(Amazon Creators API는 성분을 제공하지 않으므로 성분은 이 경로로 받는다.)

    - 우선순위 1: 올리브영 글로벌 API('045', prdt_no 있을 때) — 정형 데이터
    - 우선순위 2: Gemini 그라운딩(브랜드 공식 사이트 / 올리브영 페이지)
    - 리뷰/블로그 등 3rd party 콘텐츠는 사용하지 않는다 (성분 정보 신뢰도 문제)
    - OY '046'(식약처 기능성 심사 여부)은 mfds_functional 힌트로 저장(어드민 기능성 확인 보조)

products 컬렉션에서 status=PENDING인 상품의 성분을 찾아 채워 넣고, 결과에 따라
status를 다음 중 하나로 갱신한다. 이후 흐름은 어드민 수동 검증(성분/기능성)과 연결된다.

    - NEED_MANUAL_REVIEW      : 자동 추출 실패 → 어드민이 수동으로 성분 입력
                                (어드민: INGREDIENTS_ADDED 또는 NOTFOUND로 갱신)
    - INSUFFICIENT_INGREDIENTS: 성분은 찾았지만 MIN_INGREDIENTS_FOR_ANALYSIS 미만 → 사용 안 함(드롭)
    - INGREDIENTS_ADDED       : 성분을 충분히 찾음 → 어드민이 의약품안전나라 기능성 확인 후
                                function 입력(어드민: READY_FOR_INCIAPI로 갱신) → STEP6 대상

한국 상품이면 성분 크롤링 직후 Postgres 서빙 테이블(products_catalog/products_url/
product_ingredients/youtuber_reviews)에 발행한다.
"""
import asyncio
import re

from loguru import logger

from steps.db import get_collection
from steps.gemini_client import get_client
from steps.llm_utils import parse_json_response
from steps import postgres_publish as pg
from steps import youtuber_reviews
from steps import oliveyoung_client

try:
    from google.genai import types as genai_types
except ImportError:
    genai_types = None

GEMINI_MODEL = "gemini-2.5-flash"
MAX_ATTEMPTS = 3

# 성분 출처로 인정하는 값. 그 외(모델이 자칭했거나 불명)는 source를 null로 정규화한다.
ALLOWED_SOURCES = {"official_site", "oliveyoung"}

# 성분이 이 개수 미만이면 INCI 분석 신뢰도가 낮다고 보고 INSUFFICIENT_INGREDIENTS로 남긴다.
MIN_INGREDIENTS_FOR_ANALYSIS = 5

# OliveYoung 성분을 채택하기 위한 최소 겹침 비율(Gemini가 찾은 성분 중 OY 리스트에도 있는 비율).
# 표기 차이(Water/Aqua 등)를 감안해 낮게 잡되, 전혀 다른 상품(겹침 0~20%)은 걸러낸다.
OY_MIN_OVERLAP = 0.4

# 동시에 처리할 상품 수(비동기 병렬).
CONCURRENCY = 8


def _get_products_collection():
    return get_collection("products")


async def fetch_products_needing_ingredients() -> list[dict]:
    col = _get_products_collection()
    return [
        doc async for doc in col.find(
            {"status": "PENDING"},
            {"_id": 0, "asin": 1, "name": 1, "brand": 1},
        )
    ]


def _build_prompt(name: str, brand: str | None) -> str:
    brand_hint = f"Brand: {brand}" if brand else ""
    return (
        f'K-beauty product: "{name}"\n{brand_hint}\n\n'
        "Search the web for this product's official full INCI ingredient list.\n"
        "Priority order for the source:\n"
        "  1. The brand's official website\n"
        "  2. Olive Young (oliveyoung.co.kr / global.oliveyoung.com) product page\n"
        "Do NOT use user reviews, blogs, or unofficial retailer descriptions.\n"
        "You MUST attribute the ingredient list to source #1 (official_site) or #2 (oliveyoung) and set "
        "'source' to exactly one of those. If you cannot confirm the list came from the brand's official "
        "site or Olive Young, return ingredients: [] and source: null (do NOT guess from other sites).\n\n"
        "Also find the product's official KOREAN product name (한글 제품명) for name_kr "
        "(check the brand site / Olive Young / Hwahae). If you cannot determine it, use null.\n"
        "Also find this product's Olive Young GLOBAL product number for prdt_no — the prdtNo in a "
        "global.oliveyoung.com/product/detail?prdtNo=... URL (format: 'GA' followed by digits). "
        "Only return a prdtNo you actually found in a URL for THIS exact product; if you cannot find it, "
        "use null. Never guess or invent a number.\n\n"
        "Return raw JSON only:\n"
        "{\n"
        '  "ingredients": [full INCI ingredient list, in label order, as individual strings],\n'
        '  "source": "official_site" | "oliveyoung" | null,\n'
        '  "name_kr": "<official Korean product name>" or null,\n'
        '  "prdt_no": "<Olive Young Global prdtNo, e.g. GA240825052>" or null\n'
        "}\n"
        "If the ingredient list cannot be found on either source, return ingredients: [] and source: null "
        "(still fill name_kr and prdt_no if you can find them).\n"
    )


def _extract_ingredients_sync(name: str, brand: str | None) -> dict | None:
    client = get_client()
    if not client or not genai_types:
        return None

    for attempt in range(MAX_ATTEMPTS):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=_build_prompt(name, brand),
                config=genai_types.GenerateContentConfig(
                    tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())],
                    temperature=0.1,
                ),
            )
            parsed = parse_json_response(response.text or "")
            if not isinstance(parsed, dict):
                continue

            name_kr = parsed.get("name_kr")
            name_kr = name_kr.strip() if isinstance(name_kr, str) and name_kr.strip() else None
            prdt_no = parsed.get("prdt_no")
            prdt_no = prdt_no.strip() if isinstance(prdt_no, str) and prdt_no.strip() else None

            ingredients = parsed.get("ingredients") or []
            if isinstance(ingredients, str):
                ingredients = [i.strip() for i in re.split(r"[,\n]", ingredients) if i.strip()]
            if not ingredients:
                return {"ingredients": [], "source": None, "name_kr": name_kr, "prdt_no": prdt_no}

            source = parsed.get("source")
            if source not in ALLOWED_SOURCES:
                # 공식사이트/올리브영 출처로 확인되지 않은 성분은 신뢰하지 않는다.
                # (리뷰/블로그 등 비공식 소스 혼입 방지) → 성분 버리고 수동 검토로 넘긴다.
                logger.info(f"[STEP5][SRC?] {name!r}: 출처 미확인(source={source!r}) — 성분 폐기")
                return {"ingredients": [], "source": None, "name_kr": name_kr, "prdt_no": prdt_no}
            return {"ingredients": ingredients, "source": source,
                    "name_kr": name_kr, "prdt_no": prdt_no}
        except Exception as exc:
            logger.warning(f"[STEP5] {name!r} 시도 {attempt + 1}/{MAX_ATTEMPTS} 실패: {exc}")
    return None


def _norm_ingredient(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _extract_full_sync(name: str, brand: str | None) -> dict | None:
    """Gemini 그라운딩으로 성분·prdt_no·name_kr을 받고, prdt_no가 있으면 OliveYoung API를
    우선 사용한다(정형 '045' 필드라 그라운딩보다 신뢰도 높음). OY가 없거나 요약본(truncated)/
    빈값이면 그라운딩 성분을 그대로 쓴다. '046' 기능성은 mfds_functional 힌트로 함께 반환.

    자기검증(2026-09-12 사고 이후): 틀린 prdt_no가 "존재하는 다른 상품"을 가리키면 OY가 그 상품의
    진짜 성분을 돌려줘 그대로 저장되는 오염이 발생했다(프롬프트 예시 번호를 Gemini가 베껴 62개 상품이
    같은 성분을 가짐). 그래서 OY 성분은 Gemini가 직접 찾은 성분과 OY_MIN_OVERLAP 이상 겹칠 때만 채택하고,
    Gemini 성분이 없어 대조할 수 없으면 OY 결과를 쓰지 않는다(→ 수동 검토)."""
    gemini = _extract_ingredients_sync(name, brand)
    if gemini is None:
        return None  # 일시 실패 → PENDING 유지 재시도
    prdt_no = gemini.get("prdt_no")
    if prdt_no:
        oy = oliveyoung_client.get_ingredients(prdt_no)
        if oy and oy["ingredients"] and not oy["truncated"]:
            g_set = {_norm_ingredient(i) for i in gemini.get("ingredients") or []}
            o_set = {_norm_ingredient(i) for i in oy["ingredients"]}
            overlap = len(g_set & o_set) / len(g_set) if g_set else 0.0
            if g_set and overlap >= OY_MIN_OVERLAP:
                logger.info(f"[STEP5][OY] {name!r}: OliveYoung {prdt_no} 성분 {len(oy['ingredients'])}개 사용 (Gemini 대비 겹침 {overlap:.0%})")
                return {"ingredients": oy["ingredients"], "source": "oliveyoung",
                        "name_kr": gemini.get("name_kr"), "mfds_functional": oy.get("functional")}
            logger.warning(f"[STEP5][OY-REJECT] {name!r}: prdt_no={prdt_no} 성분이 Gemini 결과와 겹침 {overlap:.0%} "
                           f"(Gemini 성분 {len(g_set)}개) — 다른 상품일 가능성, OY 결과 폐기")
    return gemini


def _publish_product_sync(product: dict, yt_rows: list[dict]) -> None:
    """한국 상품을 Postgres 서빙 테이블에 발행: catalog + url + ingredients + youtuber_reviews.
    step5는 동시 실행되므로 호출마다 자체 연결을 연다(psycopg2 연결 공유 불가). best-effort."""
    try:
        conn = pg.connect()
    except Exception as exc:
        logger.warning(f"[STEP5][PG] Postgres 연결 실패 — 발행 건너뜀: {exc}")
        return
    try:
        product_id = pg.publish_catalog(conn, product)   # products_catalog + products_url (authoritative)
        pg.publish_ingredients(conn, product_id, product.get("ingredients") or [], product.get("ingredient_source"))
        pg.publish_youtuber_reviews(conn, product_id, yt_rows)
    except Exception as exc:
        conn.rollback()
        logger.warning(f"[STEP5][PG] 발행 실패 asin={product.get('asin')}: {exc}")
    finally:
        conn.close()


async def _publish_product_pg(asin: str) -> None:
    """신규 한국 상품을 Postgres에 발행한다(Mongo 상품 doc + 유튜버 리뷰 행을 모아서)."""
    product = await _get_products_collection().find_one(
        {"asin": asin},
        {"_id": 0, "asin": 1, "name": 1, "brand": 1, "category": 1, "price": 1,
         "thumbnail_url": 1, "description": 1, "product_url": 1, "mention_count": 1,
         "ad_ratio": 1, "ingredients": 1, "ingredient_source": 1},
    )
    if not product:
        return
    rows = await youtuber_reviews.gather_rows(asin)
    await asyncio.to_thread(_publish_product_sync, product, rows)


_CONCENTRATION_RE = re.compile(
    r"\s*[\(\[]?\s*\d[\d,.]*\s*(?:%|ppm|퍼센트)\s*[\)\]]?\s*$", re.IGNORECASE
)


def _strip_concentration(name: str) -> str:
    """성분명 뒤에 붙은 함량 표기(%/ppm)를 제거해 성분명만 남긴다.
    예: '나이아신아마이드 5%'→'나이아신아마이드', 'Bakuchiol 10,000ppm'→'Bakuchiol',
        'Adenosine (0.04%)'→'Adenosine'.
    앞쪽 숫자('1,2-Hexanediol')나 %/ppm 없는 끝 숫자('Ceramide 3')는 건드리지 않는다(뒤 함량만 매칭)."""
    return _CONCENTRATION_RE.sub("", name).strip()


async def _save_result(asin: str, name: str, result: dict | None) -> None:
    col = _get_products_collection()

    if result is None:
        # 일시적 실패(Gemini 예외/미설정)는 상태를 바꾸지 않고 PENDING으로 남겨 다음 실행에 재시도한다.
        logger.warning(f"[STEP5][RETRY] {name!r}: 추출 일시 실패 — PENDING 유지(다음 실행에 재시도).")
        return

    # 성분명 뒤 함량 표기를 제거해 성분명만 저장한다(빈 문자열은 버림).
    result["ingredients"] = [c for i in (result.get("ingredients") or [])
                             if (c := _strip_concentration(i))]

    # name_kr(화해 매칭)·mfds_functional(OY '046', 어드민 기능성 확인 힌트)은 찾았으면 항상 저장.
    base = {}
    if result.get("name_kr"):
        base["name_kr"] = result["name_kr"]
    if result.get("mfds_functional"):
        base["mfds_functional"] = result["mfds_functional"]

    if not result["ingredients"]:
        # 자동 추출 실패 → 어드민이 수동으로 성분을 넣도록 넘긴다(NEED_MANUAL_REVIEW).
        await col.update_one({"asin": asin}, {"$set": {**base, "status": "NEED_MANUAL_REVIEW"}})
        # 성분이 없어도 한국 상품이므로 카탈로그/URL/유튜버리뷰는 발행한다(성분 행만 비어있음).
        await _publish_product_pg(asin)
        logger.warning(f"[STEP5][NEED_MANUAL_REVIEW] {name!r}: 자동 성분추출 실패 — 어드민 수동 입력 대기.")
        return

    ingredient_count = len(result["ingredients"])
    # 충분히 찾음 → 어드민 기능성(안전나라) 입력 대기(INGREDIENTS_ADDED). 부족하면 사용 안 함(드롭).
    status = "INGREDIENTS_ADDED" if ingredient_count >= MIN_INGREDIENTS_FOR_ANALYSIS else "INSUFFICIENT_INGREDIENTS"

    await col.update_one(
        {"asin": asin},
        {"$set": {
            **base,
            "ingredients": result["ingredients"],
            "ingredient_source": result["source"],
            "status": status,
        }},
    )
    # Postgres 서빙 발행(catalog + url + ingredients + youtuber_reviews).
    await _publish_product_pg(asin)
    logger.info(f"[STEP5][{status}] {name!r}: 성분 {ingredient_count}개 ({result['source']})")


async def run() -> None:
    products = await fetch_products_needing_ingredients()
    if not products:
        logger.info("[STEP5] 성분추출이 필요한 상품이 없습니다.")
        return

    sem = asyncio.Semaphore(CONCURRENCY)

    async def _process(product: dict) -> None:
        async with sem:
            name, brand = product["name"], product.get("brand")
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(_extract_full_sync, name, brand), timeout=150
                )
            except asyncio.TimeoutError:
                # Gemini 호출이 멈춘 경우: 일시 실패로 보고 PENDING 유지(다음 실행 재시도).
                logger.warning(f"[STEP5][TIMEOUT] 성분추출 타임아웃: {name!r}")
                result = None
            await _save_result(product["asin"], name, result)

    await asyncio.gather(*(_process(p) for p in products))


if __name__ == "__main__":
    asyncio.run(run())
