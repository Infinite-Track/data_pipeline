"""
STEP 5 · 성분 추출 (상품에 성분 추가)

Gemini + Google 검색 그라운딩으로 공식 소스에서 성분(INCI)을 수집한다.
(Amazon Creators API는 성분 데이터를 제공하지 않으므로 성분은 이 경로로 받는다.)

    - 우선순위 1: 브랜드 공식 사이트
    - 우선순위 2: 올리브영(Olive Young) 상품 페이지
    - 리뷰/블로그 등 3rd party 콘텐츠는 사용하지 않는다 (성분 정보 신뢰도 문제)

Gemini가 성분을 못 찾거나 부족하면(MIN_INGREDIENTS_FOR_ANALYSIS 미만) incidecoder.com을
폴백으로 조회한다. incidecoder는 성분 전문 DB(과학자 편집)로, 공식 소스보다는 아래 순위지만
리뷰/블로그보다 신뢰도가 높고 requests로 접근 가능하다. 이 경로로 채운 성분은
ingredient_source="incidecoder"로 태깅해 추적한다. (올리브영 직접 스크래핑은 WAF로 차단됨.)

products 컬렉션에서 status=PENDING인 상품의 성분을 찾아 채워 넣고, 결과에 따라
status를 다음 중 하나로 갱신한다 (이후 STEP6은 READY_FOR_ANALYSIS만 가져다 쓴다).

    - NOT_FOUND              : 공식 사이트/올리브영/incidecoder 어디서도 성분을 찾지 못함
    - INSUFFICIENT_INGREDIENTS: 성분은 찾았지만 MIN_INGREDIENTS_FOR_ANALYSIS(10개) 미만이라
                                INCI 분석 신뢰도가 낮음
    - READY_FOR_ANALYSIS     : 성분을 충분히 찾음 → STEP6의 INCI API 분석 대상
"""
import asyncio
import datetime
import html
import re

import requests
from loguru import logger

from steps.db import get_collection
from steps.gemini_client import get_client
from steps.llm_utils import parse_json_response

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

# 동시에 처리할 상품 수(비동기 병렬).
CONCURRENCY = 8

# incidecoder 폴백 설정.
INCIDECODER_BASE = "https://incidecoder.com"
INCIDECODER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


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
        "Do NOT use user reviews, blogs, or unofficial retailer descriptions.\n\n"
        "Return raw JSON only:\n"
        "{\n"
        '  "ingredients": [full INCI ingredient list, in label order, as individual strings],\n'
        '  "source": "official_site" | "oliveyoung" | null\n'
        "}\n"
        "If the ingredient list cannot be found on either source, return ingredients: [] and source: null.\n"
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

            ingredients = parsed.get("ingredients") or []
            if isinstance(ingredients, str):
                ingredients = [i.strip() for i in re.split(r"[,\n]", ingredients) if i.strip()]
            if not ingredients:
                return {"ingredients": [], "source": None}

            source = parsed.get("source")
            return {"ingredients": ingredients, "source": source if source in ALLOWED_SOURCES else None}
        except Exception as exc:
            logger.warning(f"[STEP5] {name!r} 시도 {attempt + 1}/{MAX_ATTEMPTS} 실패: {exc}")
    return None


def _fetch_from_incidecoder_sync(name: str) -> list[str]:
    """incidecoder에서 제품명을 검색해 첫 매칭 상품의 라벨순 전성분을 가져온다.

    실패(차단/미검색/파싱실패)하면 빈 리스트를 반환한다. 공식 소스로 성분을 못 구했을
    때만 호출하는 폴백이므로, 예외는 조용히 삼키고 로그만 남긴다.
    """
    try:
        search = requests.get(
            f"{INCIDECODER_BASE}/search", params={"query": name},
            headers=INCIDECODER_HEADERS, timeout=15,
        )
        if search.status_code != 200:
            return []
        product_links = re.findall(r'href="(/products/[^"]+)"', search.text)
        if not product_links:
            return []

        page = requests.get(
            INCIDECODER_BASE + product_links[0], headers=INCIDECODER_HEADERS, timeout=15,
        )
        if page.status_code != 200:
            return []
        # 성분이 페이지에 두 번(요약 리스트 + 상세 표) 나오므로 라벨순 요약 블록만 파싱한다.
        block = re.search(
            r'<div[^>]*class="[^"]*ingredlist[^"]*"[^>]*>(.*?)</div>', page.text, re.S,
        )
        if not block:
            return []
        names = re.findall(
            r'href="/ingredients/[^"]+"[^>]*>([^<]+)</a>', block.group(1),
        )
        # zero-width space 등 잔여문자 정리.
        return [html.unescape(n).replace("​", "").strip() for n in names if n.strip()]
    except requests.exceptions.RequestException as exc:
        logger.warning(f"[STEP5][incidecoder] {name!r} 조회 실패: {exc}")
        return []


def _extract_with_fallback_sync(name: str, brand: str | None) -> dict | None:
    """Gemini(공식 소스) 우선, 부족하면 incidecoder 폴백으로 보강한다.

    반환: {"ingredients", "source"} 또는 None(둘 다 일시 실패 → PENDING 유지 재시도).
    공식 소스로 충분(>=MIN)하면 그대로 쓰고, incidecoder는 그보다 많이 찾은 경우에만 채택한다.
    """
    gemini = _extract_ingredients_sync(name, brand)
    gemini_ings = gemini["ingredients"] if gemini else []
    if len(gemini_ings) >= MIN_INGREDIENTS_FOR_ANALYSIS:
        return gemini

    inci_ings = _fetch_from_incidecoder_sync(name)
    if len(inci_ings) > len(gemini_ings):
        return {"ingredients": inci_ings, "source": "incidecoder"}

    # 폴백도 개선 못함: Gemini 결과(빈/부족)를 그대로 두되, Gemini가 일시실패(None)였다면
    # None을 유지해 다음 실행에 재시도한다.
    return gemini


async def _save_result(asin: str, name: str, result: dict | None) -> None:
    col = _get_products_collection()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if result is None:
        # 일시적 실패(Gemini 예외/미설정)는 상태를 바꾸지 않고 PENDING으로 남겨 다음 실행에 재시도한다.
        logger.warning(f"[STEP5][RETRY] {name!r}: 추출 일시 실패 — PENDING 유지(다음 실행에 재시도).")
        return

    if not result["ingredients"]:
        await col.update_one({"asin": asin}, {"$set": {"status": "NOT_FOUND"}})
        logger.warning(f"[STEP5][NOT_FOUND] {name!r}: 공식 사이트/올리브영에서 성분을 찾지 못함.")
        return

    ingredient_count = len(result["ingredients"])
    status = "READY_FOR_ANALYSIS" if ingredient_count >= MIN_INGREDIENTS_FOR_ANALYSIS else "INSUFFICIENT_INGREDIENTS"

    await col.update_one(
        {"asin": asin},
        {"$set": {
            "ingredients": result["ingredients"],
            "ingredient_source": result["source"],
            "status": status,
            "ingredients_fetched_at": now,
        }},
    )
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
                    asyncio.to_thread(_extract_with_fallback_sync, name, brand), timeout=150
                )
            except asyncio.TimeoutError:
                # Gemini 호출이 멈춘 경우: 일시 실패로 보고 PENDING 유지(다음 실행 재시도).
                logger.warning(f"[STEP5][TIMEOUT] 성분추출 타임아웃: {name!r}")
                result = None
            await _save_result(product["asin"], name, result)

    await asyncio.gather(*(_process(p) for p in products))


if __name__ == "__main__":
    asyncio.run(run())
