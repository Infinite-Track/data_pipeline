import asyncio
import datetime
import json
import os
import re
from typing import Any

import requests
from dotenv import load_dotenv
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

try:
    from google import genai
    from google.genai import types as genai_types
except ImportError:
    genai = None
    genai_types = None

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
INCI_API_KEY = os.getenv("INCI_API_KEY")
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")
GEMINI_MODEL = "gemini-2.5-flash"
INCI_API_BASE = "https://inciapi.com/v1"

_genai_client = None
if GEMINI_API_KEY and genai:
    _genai_client = genai.Client(api_key=GEMINI_API_KEY)

_client: AsyncIOMotorClient | None = None
_products_col = None


def _get_products_collection():
    global _client, _products_col
    if _products_col is None:
        _client = AsyncIOMotorClient(MONGODB_URI)
        _products_col = _client[MONGODB_DB_NAME]["products"]
    return _products_col


async def ensure_indexes() -> None:
    col = _get_products_collection()
    await col.create_index("product_name", unique=True)
    logger.info("[STEP5] Ensured unique index on products.product_name.")


# products 컬렉션에서 성분 정보가 없는 제품 목록을 가져옵니다.
async def fetch_products_without_ingredients() -> list[dict]:
    col = _get_products_collection()
    products = []
    async for doc in col.find(
        {"ingredients": {"$in": [None, []]}},
        {"_id": 0, "product_name": 1, "brand": 1, "category": 1},
    ):
        products.append(doc)
    logger.info(f"[STEP5] Found {len(products)} products without ingredients.")
    return products


def _strip_code_blocks(text: str) -> str:
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if match:
        return match.group(1).strip()
    return text.strip()


def _parse_json(raw_text: str) -> Any:
    cleaned = _strip_code_blocks(raw_text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(cleaned[start:end + 1])
        except json.JSONDecodeError:
            pass
    return None


# Gemini 웹서치로 제품의 INCI 성분 리스트와 기본 정보를 가져옵니다.
def _fetch_ingredients_via_gemini_sync(product_name: str, brand: str | None) -> dict | None:
    import time
    if not _genai_client:
        return None
    brand_hint = f"Brand: {brand}" if brand else ""
    prompt = (
        f'K-beauty product: "{product_name}"\n'
        f"{brand_hint}\n\n"
        "Search the web for this product's official ingredient list. "
        "Prioritize the brand's official website, then official retailer pages (Olive Young, Sephora, YesStyle). "
        "Do NOT use user reviews or third-party blogs.\n\n"
        "Return a JSON object:\n"
        "{\n"
        '  "ingredients": [full INCI ingredient list in order, as individual strings],\n'
        '  "description": "official 1-2 sentence product description",\n'
        '  "skin_concerns": [list of skin concerns this product targets],\n'
        '  "price_usd": float or null\n'
        "}\n"
        "- ingredients must be the complete INCI list from the official source\n"
        "- If not found on official sources, use null\n"
        "- Return raw JSON only, no markdown\n"
    )
    for attempt in range(3):
        try:
            response = _genai_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=genai_types.GenerateContentConfig(temperature=0.2),
            )
            raw_text = response.text
            if not raw_text:
                try:
                    parts = response.candidates[0].content.parts
                    raw_text = "".join(p.text for p in parts if hasattr(p, "text") and p.text)
                except Exception:
                    pass
            if not raw_text:
                return None
            parsed = _parse_json(raw_text)
            if not isinstance(parsed, dict):
                return None
            ingredients = parsed.get("ingredients") or []
            if isinstance(ingredients, str):
                ingredients = [i.strip() for i in re.split(r"[,\n]", ingredients) if i.strip()]
            return {
                "ingredients": ingredients,
                "description": parsed.get("description") or None,
                "skin_concerns": parsed.get("skin_concerns") or [],
                "price_usd": parsed.get("price_usd") or None,
            }
        except Exception as exc:
            wait = 10 * (attempt + 1)
            logger.warning(f"[STEP5] Gemini attempt {attempt+1} failed for {product_name!r}: {exc}. Retrying in {wait}s...")
            time.sleep(wait)
    return None


# INCI API의 POST /analyze 엔드포인트로 성분 리스트를 분석합니다.
def _analyze_via_inci_api_sync(ingredients: list[str]) -> dict | None:
    if not INCI_API_KEY or not ingredients:
        return None
    try:
        response = requests.post(
            f"{INCI_API_BASE}/analyze",
            headers={
                "X-API-Key": INCI_API_KEY,
                "Content-Type": "application/json",
            },
            json={"inci": ingredients[:200]},
            timeout=30,
        )
        if response.status_code != 200:
            logger.warning(f"[STEP5] INCI API returned {response.status_code}: {response.text[:200]}")
            return None
        return response.json()
    except Exception as exc:
        logger.warning(f"[STEP5] INCI API request failed: {exc}")
        return None


# INCI API 응답에서 필요한 필드만 추출합니다.
def _parse_inci_analysis(raw: dict) -> dict:
    data = raw.get("analysis") or raw
    parsed = data.get("parsedIngredients") or []
    found_count = sum(1 for i in parsed if i.get("found"))
    coverage = round(found_count / len(parsed), 3) if parsed else None
    return {
        "inci_overall_safety_score": data.get("overallSafetyScore"),
        "inci_safety_level": data.get("safetyLevel"),
        "inci_allergen_flags": data.get("allergenFlags") or [],
        "inci_pregnancy_safe": data.get("pregnancySafe"),
        "inci_clean_beauty_score": data.get("cleanBeautyScore"),
        "inci_comedogenicity_score": data.get("comedogenicityScore"),
        "inci_skin_type_compatibility": data.get("skinTypeCompatibility") or {},
        "inci_efficacy_summary": data.get("efficacySummary") or {},
        "inci_parsed_ingredients": parsed,
        "inci_coverage": coverage,
    }


async def _save_product(product_data: dict) -> None:
    col = _get_products_collection()
    record = {
        **product_data,
        "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    await col.update_one(
        {"product_name": record["product_name"]},
        {"$set": record},
        upsert=True,
    )
    logger.info(f"[STEP5] Saved: {record['product_name']}")


async def run() -> None:
    await ensure_indexes()

    products = await fetch_products_without_ingredients()
    if not products:
        logger.info("[STEP5] All products already have ingredients.")
        return

    for product in products:
        name = product["product_name"]
        brand = product.get("brand")

        # 1단계: Gemini 웹서치로 INCI 성분 리스트 추출
        gemini_info = await asyncio.to_thread(_fetch_ingredients_via_gemini_sync, name, brand)
        if not gemini_info or not gemini_info.get("ingredients"):
            logger.warning(f"[STEP5] No ingredients found for {name!r}, skipping.")
            continue
        logger.info(f"[STEP5] Got {len(gemini_info['ingredients'])} ingredients for {name!r}")

        # 2단계: INCI API로 성분 분석 (성분 10개 이하 제품은 분석 신뢰도가 낮아 스킵)
        ingredients = gemini_info["ingredients"]
        if len(ingredients) <= 10:
            logger.warning(f"[STEP5] {name!r}: 성분 {len(ingredients)}개 (≤10) → INCI API 스킵")
            inci_analysis = {}
        else:
            inci_raw = await asyncio.to_thread(_analyze_via_inci_api_sync, ingredients)
            inci_analysis = _parse_inci_analysis(inci_raw) if inci_raw else {}

        merged = {**product, **gemini_info, **inci_analysis}
        await _save_product(merged)
