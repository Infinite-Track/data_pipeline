"""
STEP 6 · 성분 특징 추출 (상품의 특징/이론 추가)

step5에서 채운 ingredients를 INCI API(POST /analyze)에 전달해
성분 기반 효능·안전성 분석 데이터를 받아온다. (필드 의미는 description/INCI_FIELD_REFERENCE.txt 참고)

원본 응답은 products.inciapi_raw_data에 그대로 저장한다 (요약/가공은 다음 step7 담당).
추가로 파이프라인 자체 계산값인 inci_coverage(성분 인식률)를 함께 저장해
step7이 "성분 데이터를 얼마나 신뢰할 수 있는지" 가중치로 쓸 수 있게 한다.
"""
import asyncio
import datetime
import os

import requests
from dotenv import load_dotenv
from loguru import logger

from steps.db import get_collection

load_dotenv()

INCI_API_KEY = os.getenv("INCI_API_KEY")
INCI_API_BASE = "https://inciapi.com/v1"

# 성분이 이 개수 이하면 INCI 분석 신뢰도가 낮다고 보고 스킵한다 (products.status는 PENDING 유지).
MIN_INGREDIENTS_FOR_ANALYSIS = 10

if not INCI_API_KEY:
    logger.warning("INCI_API_KEY가 설정되지 않았습니다. STEP6은 실행되지 않습니다.")


def _get_products_collection():
    return get_collection("products")


async def fetch_products_needing_analysis() -> list[dict]:
    col = _get_products_collection()
    return [
        doc async for doc in col.find(
            {"status": "COMPLETE", "inciapi_raw_data": None},
            {"_id": 0, "name": 1, "ingredients": 1},
        )
    ]


def _analyze_via_inci_api_sync(ingredients: list[str]) -> dict | None:
    try:
        response = requests.post(
            f"{INCI_API_BASE}/analyze",
            headers={"X-API-Key": INCI_API_KEY, "Content-Type": "application/json"},
            json={"inci": ingredients[:200]},
            timeout=30,
        )
        if response.status_code != 200:
            logger.warning(f"[STEP6] INCI API {response.status_code}: {response.text[:200]}")
            return None
        return response.json()
    except Exception as exc:
        logger.warning(f"[STEP6] INCI API 요청 실패: {exc}")
        return None


def _calc_coverage(raw: dict) -> float | None:
    """parsedIngredients 중 found=true 비율 = 성분 데이터 신뢰도(coverage)."""
    data = raw.get("analysis") or raw
    parsed = data.get("parsedIngredients") or []
    if not parsed:
        return None
    found = sum(1 for i in parsed if i.get("found"))
    return round(found / len(parsed), 3)


async def _save_analysis(name: str, raw: dict) -> None:
    col = _get_products_collection()
    data = raw.get("analysis") or raw
    await col.update_one(
        {"name": name},
        {"$set": {
            "inciapi_raw_data": data,
            "inci_coverage": _calc_coverage(raw),
            "analyzed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }},
    )
    logger.info(f"[STEP6][SAVE] {name!r}: INCI 분석 저장 완료.")


async def run() -> None:
    if not INCI_API_KEY:
        logger.error("[STEP6] INCI_API_KEY 누락으로 스킵합니다.")
        return

    products = await fetch_products_needing_analysis()
    if not products:
        logger.info("[STEP6] 분석이 필요한 상품이 없습니다.")
        return

    for product in products:
        name = product["name"]
        ingredients = product.get("ingredients") or []
        if len(ingredients) <= MIN_INGREDIENTS_FOR_ANALYSIS:
            logger.warning(f"[STEP6][SKIP] {name!r}: 성분 {len(ingredients)}개(≤{MIN_INGREDIENTS_FOR_ANALYSIS}) → 신뢰도 낮아 스킵.")
            continue

        raw = await asyncio.to_thread(_analyze_via_inci_api_sync, ingredients)
        if not raw:
            continue

        await _save_analysis(name, raw)


if __name__ == "__main__":
    asyncio.run(run())
