"""
STEP 6 · 성분 특징 추출 (상품의 특징/이론 추가)

step5에서 status=READY_FOR_ANALYSIS로 남긴(성분을 충분히 찾은) 상품만 가져와
INCI API(POST /analyze)에 전달해 성분 기반 효능·안전성 분석 데이터를 받아온다.
(필드 의미는 description/INCI_FIELD_REFERENCE.txt 참고)

원본 응답은 products.inciapi_raw_data에 그대로 저장한다 (요약/가공은 다음 step7 담당).
API 응답에 이미 포함된 coverage(성분 인식 비율, 0~1)를 inci_coverage로 그대로 저장해
step7이 "성분 데이터를 얼마나 신뢰할 수 있는지" 가중치로 쓸 수 있게 한다.
분석까지 끝나면 status를 COMPLETE로 갱신한다 (성분 개수 기준 필터링은 STEP5가 이미 끝냈으므로
여기서는 다시 확인하지 않는다).
"""
import asyncio
import datetime
import os
import time

import requests
from dotenv import load_dotenv
from loguru import logger

from steps.db import get_collection
from steps.step5_extract_ingredients import MIN_INGREDIENTS_FOR_ANALYSIS

load_dotenv()

INCI_API_KEY = os.getenv("INCI_API_KEY")
INCI_API_BASE = "https://inciapi.com/v1"
MAX_ATTEMPTS = 5
THROTTLE_SECONDS = 2.0  # INCI API 요청 간 최소 간격(레이트리밋 예방)

if not INCI_API_KEY:
    logger.warning("INCI_API_KEY가 설정되지 않았습니다. STEP6은 실행되지 않습니다.")


def _get_products_collection():
    return get_collection("products")


async def fetch_products_needing_analysis() -> list[dict]:
    col = _get_products_collection()
    return [
        doc async for doc in col.find(
            {"status": "READY_FOR_ANALYSIS"},
            {"_id": 0, "asin": 1, "name": 1, "ingredients": 1},
        )
    ]


def _analyze_via_inci_api_sync(ingredients: list[str]) -> dict | None:
    """INCI API를 호출한다. 실패(오류 상태코드/예외)하면 MAX_ATTEMPTS번까지 재시도한다.

    성분이 MIN_INGREDIENTS_FOR_ANALYSIS 미만이면 호출 자체를 하지 않는다. STEP5가 이미
    이 기준으로 걸러서(READY_FOR_ANALYSIS만 STEP6에 넘어옴) 정상적으로는 여기 걸릴 일이
    없어야 하지만, 이 함수만 따로 호출되는 경우에도 안전하도록 방어적으로 한 번 더 확인한다.
    """
    if len(ingredients) < MIN_INGREDIENTS_FOR_ANALYSIS:
        logger.warning(
            f"[STEP6] 성분 {len(ingredients)}개(<{MIN_INGREDIENTS_FOR_ANALYSIS}) → INCI API 호출 스킵."
        )
        return None

    for attempt in range(MAX_ATTEMPTS):
        try:
            response = requests.post(
                f"{INCI_API_BASE}/analyze",
                headers={"X-API-Key": INCI_API_KEY, "Content-Type": "application/json"},
                json={"inci": ingredients[:200]},
                timeout=30,
            )
            if response.status_code == 429:
                # 레이트리밋: 서버가 알려준 retryAfter(초)만큼 기다렸다 재시도한다.
                retry_after = response.headers.get("Retry-After")
                if retry_after is None:
                    try:
                        retry_after = (response.json() or {}).get("retryAfter")
                    except Exception:
                        retry_after = None
                wait = int(retry_after) if retry_after else min(60, 2 ** (attempt + 2))
                logger.warning(f"[STEP6] INCI API 429 — {wait}s 대기 후 재시도 ({attempt + 1}/{MAX_ATTEMPTS})")
                time.sleep(wait)
                continue
            if response.status_code != 200:
                logger.warning(
                    f"[STEP6] INCI API {response.status_code} (시도 {attempt + 1}/{MAX_ATTEMPTS}): {response.text[:200]}"
                )
                time.sleep(2 ** attempt)
                continue
            return response.json()
        except Exception as exc:
            logger.warning(f"[STEP6] INCI API 요청 실패 (시도 {attempt + 1}/{MAX_ATTEMPTS}): {exc}")
            time.sleep(2 ** attempt)
    return None


async def _save_analysis(asin: str, name: str, raw: dict) -> None:
    col = _get_products_collection()
    data = raw.get("analysis") or raw
    await col.update_one(
        {"asin": asin},
        {"$set": {
            "inciapi_raw_data": data,
            # coverage(성분 인식 비율, 0~1)는 API가 이미 계산해서 주므로 직접 재계산하지 않는다.
            "inci_coverage": data.get("coverage"),
            "analyzed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "status": "COMPLETE",
        }},
    )
    logger.info(f"[STEP6][COMPLETE] {name!r}: INCI 분석 저장 완료.")


async def run() -> None:
    if not INCI_API_KEY:
        logger.error("[STEP6] INCI_API_KEY 누락으로 스킵합니다.")
        return

    products = await fetch_products_needing_analysis()
    if not products:
        logger.info("[STEP6] 분석이 필요한 상품이 없습니다.")
        return

    for i, product in enumerate(products):
        name = product["name"]
        ingredients = product.get("ingredients") or []

        if i > 0:
            await asyncio.sleep(THROTTLE_SECONDS)  # 요청 간 간격으로 레이트리밋 예방
        raw = await asyncio.to_thread(_analyze_via_inci_api_sync, ingredients)
        if not raw:
            continue

        await _save_analysis(product["asin"], name, raw)


if __name__ == "__main__":
    asyncio.run(run())
