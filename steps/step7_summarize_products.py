"""
STEP 7 · 개인화 추천을 위한 스키마로 요약

products 컬렉션(원본 + INCI 분석 결과)을 종합해 추천 알고리즘이 바로 쓸 수 있는
products_summarized 컬렉션으로 변환한다.

    - 11개 효능 축 점수 : acne, blackhead, dark_circle, water, melanin,
                          oily_intensity, pores, red_spot, rough, sensitivity, wrinkle
    - 4개 피부 타입 적합도 : skin_type_dry / oily / normal / combination
    - 5개 성분 기반 리스크 : allergen_risk, comedogenicity_risk, fragrance_risk,
                             irritancy_risk, pregnancy_risk

점수 산출 로직(효능 매핑 테이블 등)은 1차 스프린트에서 검증된 방식을 그대로 사용하고,
입력 데이터 위치만 step6이 저장한 products.inciapi_raw_data 구조에 맞춰 읽는다.

products.status가 COMPLETE(=step6의 INCI 분석까지 끝남)인 상품만 대상으로 하고,
요약을 마치면 SUMMARIZED로 갱신해 다음 실행에서 같은 상품을 다시 처리하지 않는다.
(products.status 전체 흐름: PENDING → NOT_FOUND/INSUFFICIENT_INGREDIENTS/READY_FOR_ANALYSIS
→ COMPLETE → SUMMARIZED)
"""
import asyncio
import datetime

from loguru import logger

from steps.db import get_collection

# ── 피부 고민 → INCI efficacy target 매핑 ──────────────────────────────────
# 실제 INCI API topEffects에서 나오는 target만 사용 (공식 문서엔 전체 목록이 없어 실제 호출로 확인, 16개).
# 같은 target이 여러 고민에 겹쳐 들어가는 건 의도된 것이다 (예: antioxidant는 dark_circle/wrinkle/
# melanin 모두에 기여, sebum_control은 acne/oily_intensity 모두에 기여).
# pores는 대응 target이 없어 아래에서 별도(proxy) 계산한다.
CONCERN_TARGETS: dict[str, list[str]] = {
    "dark_circle": ["anti_hyperpigmentation", "brightening", "antioxidant", "photoprotection"],
    "wrinkle": ["anti_wrinkle", "anti_aging", "collagen_stimulating", "elasticity", "antioxidant", "photoprotection"],
    "blackhead": ["exfoliating"],
    "acne": ["anti_acne", "sebum_control"],
    "sensitivity": ["anti_inflammatory", "wound_healing", "microbiome_balance"],
    "melanin": ["anti_hyperpigmentation", "brightening", "antioxidant"],
    "water": ["hydration"],
    "rough": ["exfoliating", "barrier_repair"],
    "red_spot": ["anti_inflammatory", "microbiome_balance"],
    "oily_intensity": ["sebum_control"],
}

STRENGTH_SCORE: dict[str, float] = {"strong": 0.9, "moderate": 0.55, "weak": 0.25}
EVIDENCE_WEIGHT: dict[str, float] = {"meta_analysis": 1.0, "rct": 0.85, "in_vitro": 0.6, "anecdotal": 0.4}
COMPAT_SCORE: dict[str, float] = {"good": 1.0, "neutral": 0.5, "caution": 0.1, "avoid": 0.0}
IRRITANCY_SCORE: dict[str, float] = {"none": 0.0, "low": 0.25, "moderate": 0.6, "high": 1.0}
FRAGRANCE_NAMES = {"FRAGRANCE", "PARFUM", "FRAGRANCE/PARFUM", "AROMA",'FLAVOR'}


def _get_products_collection():
    return get_collection("products")


def _get_summarized_collection():
    return get_collection("products_summarized")


def _calc_concern_scores(product: dict) -> dict[str, float]:
    """INCI topEffects → 11개 효능 점수. (targets 매칭 강도 합 × 성분 신뢰도 가중치)"""
    inci = product.get("inciapi_raw_data") or {}
    top_effects = (inci.get("efficacySummary") or {}).get("topEffects") or []

    effect_map = {
        e.get("target", "").lower(): (e.get("compositeStrength", ""), e.get("bestEvidenceLevel", ""))
        for e in top_effects
    }

    clean_beauty = (inci.get("cleanBeautyScore") or 0) / 100
    coverage = product.get("inci_coverage") or 0.0
    ingredient_weight = clean_beauty * coverage

    scores: dict[str, float] = {}
    for concern, targets in CONCERN_TARGETS.items():
        total = sum(
            STRENGTH_SCORE.get(effect_map[t][0], 0.0) * EVIDENCE_WEIGHT.get(effect_map[t][1], 0.5)
            for t in targets if t in effect_map
        )
        scores[concern] = round(total * ingredient_weight, 3)

    comedo = inci.get("comedogenicityScore") or 0.0
    scores["pores"] = round((1 - comedo / 5) * ingredient_weight, 3)  # proxy: 낮은 모공막힘 지수일수록 좋음

    return scores


def _calc_skin_type_compat(product: dict) -> dict[str, float]:
    # 데이터가 없어 모르는 경우는 "중립(0.5)"이 아니라 0으로 처리한다.
    # 모르는 걸 추천 대상으로 볼 수는 없기 때문이다.
    compat = (product.get("inciapi_raw_data") or {}).get("skinTypeCompatibility") or {}
    return {
        "skin_type_oily": COMPAT_SCORE.get(compat.get("oily") or "", 0),
        "skin_type_dry": COMPAT_SCORE.get(compat.get("dry") or "", 0),
        "skin_type_normal": COMPAT_SCORE.get(compat.get("normal") or "", 0),
        "skin_type_combination": COMPAT_SCORE.get(compat.get("combination") or "", 0),
    }


def _calc_risk_flags(product: dict) -> dict:
    inci = product.get("inciapi_raw_data") or {}
    parsed = inci.get("parsedIngredients") or []

    max_irritancy = max(
        (IRRITANCY_SCORE.get(i.get("irritancyPotential") or "", 0.0) for i in parsed), default=0.0
    )
    comedo = inci.get("comedogenicityScore")
    ingredients_upper = {str(i).upper().strip() for i in (product.get("ingredients") or [])}

    return {
        "irritancy_risk": round(max_irritancy, 3),
        "comedogenicity_risk": round(comedo / 5, 3) if comedo is not None else 0.0,
        "fragrance_risk": 1 if ingredients_upper & FRAGRANCE_NAMES else 0,
        "allergen_risk": 1 if inci.get("allergenFlags") else 0,
        "pregnancy_risk": 1 if inci.get("pregnancySafe") is False else 0,
    }


async def run() -> None:
    products_col, summarized_col = _get_products_collection(), _get_summarized_collection()
    await summarized_col.create_index("product_name", unique=True)

    products = [doc async for doc in products_col.find({"status": "COMPLETE"}, {"_id": 0})]
    if not products:
        logger.info("[STEP7] 요약할 상품(status=COMPLETE)이 없습니다.")
        return

    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for product in products:
        record = {
            "product_name": product["name"],
            "brand": product.get("brand"),
            "category": product.get("category"),
            **_calc_concern_scores(product),
            **_calc_skin_type_compat(product),
            **_calc_risk_flags(product),
            "summarized_at": now,
        }
        await summarized_col.update_one({"product_name": record["product_name"]}, {"$set": record}, upsert=True)
        await products_col.update_one({"name": product["name"]}, {"$set": {"status": "SUMMARIZED"}})

    logger.info(f"[STEP7] {len(products)}개 상품 → products_summarized 저장 완료 (status: COMPLETE → SUMMARIZED).")


if __name__ == "__main__":
    asyncio.run(run())
