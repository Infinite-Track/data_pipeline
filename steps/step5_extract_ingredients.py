"""
STEP 5 · 성분 추출 (상품에 성분 추가)

원래는 Amazon 상품 링크를 확보한 상품에 한해 Amazon API/크롤링으로 성분을 가져올 예정이었으나,
현재 단계에서는 Amazon API를 아직 사용할 수 없어 Gemini 웹서치로 대체한다.

    - 우선순위 1: 브랜드 공식 사이트
    - 우선순위 2: 올리브영(Olive Young) 상품 페이지
    - 리뷰/블로그 등 3rd party 콘텐츠는 사용하지 않는다 (성분 정보 신뢰도 문제)

products 컬렉션에서 ingredients가 비어 있는 상품을 찾아 채워 넣고,
성공 여부에 따라 status를 COMPLETE / EXCEPTION으로 갱신한다.

TODO(추후 Amazon API 확보 시): products_catalog 컬렉션을 별도로 만들어
Amazon 판매 링크가 있는 상품은 API/크롤링 결과로 성분을 보강하는 경로를 추가한다.
"""
import asyncio
import datetime
import re

from loguru import logger

from steps.db import get_collection
from steps.gemini_client import get_client
from steps.llm_utils import parse_json_response

GEMINI_MODEL = "gemini-2.5-flash"
MAX_ATTEMPTS = 3


def _get_products_collection():
    return get_collection("products")


async def fetch_products_needing_ingredients() -> list[dict]:
    col = _get_products_collection()
    return [
        doc async for doc in col.find(
            {"ingredients": {"$in": [None, []]}, "status": {"$ne": "EXCEPTION"}},
            {"_id": 0, "name": 1, "brand": 1},
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
    if not client:
        return None

    for attempt in range(MAX_ATTEMPTS):
        try:
            response = client.models.generate_content(model=GEMINI_MODEL, contents=_build_prompt(name, brand))
            parsed = parse_json_response(response.text or "")
            if not isinstance(parsed, dict):
                continue

            ingredients = parsed.get("ingredients") or []
            if isinstance(ingredients, str):
                ingredients = [i.strip() for i in re.split(r"[,\n]", ingredients) if i.strip()]
            if not ingredients:
                return {"ingredients": [], "source": None}

            return {"ingredients": ingredients, "source": parsed.get("source")}
        except Exception as exc:
            logger.warning(f"[STEP5] {name!r} 시도 {attempt + 1}/{MAX_ATTEMPTS} 실패: {exc}")
    return None


async def _save_result(name: str, result: dict | None) -> None:
    col = _get_products_collection()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if result and result["ingredients"]:
        await col.update_one(
            {"name": name},
            {"$set": {"ingredients": result["ingredients"], "ingredient_source": result["source"],
                       "status": "COMPLETE", "ingredients_fetched_at": now}},
        )
        logger.info(f"[STEP5][SAVE] {name!r}: 성분 {len(result['ingredients'])}개 ({result['source']})")
    else:
        await col.update_one({"name": name}, {"$set": {"status": "EXCEPTION"}})
        logger.warning(f"[STEP5][FAIL] {name!r}: 공식 사이트/올리브영에서 성분을 찾지 못함.")


async def run() -> None:
    products = await fetch_products_needing_ingredients()
    if not products:
        logger.info("[STEP5] 성분이 필요한 상품이 없습니다.")
        return

    for product in products:
        name, brand = product["name"], product.get("brand")
        result = await asyncio.to_thread(_extract_ingredients_sync, name, brand)
        await _save_result(name, result)


if __name__ == "__main__":
    asyncio.run(run())
