"""
step6: products 컬렉션의 INCI efficacy 데이터를 바탕으로
피부 분석 11개 고민 점수 + 4개 피부 타입 호환성 + 5개 위험도를 계산해
products_summarized 컬렉션에 저장합니다.

피부 분석 카테고리 (입력값):
    skin_type: 0=지성, 1=건성, 2=중성, 3=복합성
    dark_circle_score, wrinkle_score, oily_intensity_score, pores_score,
    blackhead_score, acne_score, sensitivity_score, melanin_score,
    water_score, rough_score, red_spot_score

처리 조건: inci_overall_safety_score가 존재하고 가중치 필드(ad_weighted_score,
           inci_clean_beauty_score, inci_coverage)가 모두 있는 제품만 처리
"""
import asyncio
import datetime
import os

from dotenv import load_dotenv
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")

_client: AsyncIOMotorClient | None = None
_products_col = None
_summarized_col = None


def _get_cols():
    global _client, _products_col, _summarized_col
    if _client is None:
        _client = AsyncIOMotorClient(MONGODB_URI)
    db = _client[MONGODB_DB_NAME]
    _products_col = db["products"]
    _summarized_col = db["products_summarized"]
    return _products_col, _summarized_col


# ── 피부 고민 → INCI efficacy target 매핑 ──────────────────────────────────────
# 각 피부 분석 점수와 대응되는 INCI API topEffects target 목록
# 실제 INCI API topEffects에서 나오는 target만 사용 (15개 확인)
# oily / pore는 해당 target이 API에서 안 나오므로 _calc_concern_scores에서 별도 처리
CONCERN_TARGETS: dict[str, list[str]] = {
    # 다크서클: 색소침착 억제, 항산화, 자외선 차단
    "dark_circle": [
        "anti_hyperpigmentation", "brightening", "antioxidant", "photoprotection",
    ],
    # 주름: 항노화, 콜라겐, 항산화, 광노화 방지
    "wrinkle": [
        "anti_wrinkle", "anti_aging", "collagen_stimulating",
        "antioxidant", "photoprotection",
    ],
    # 블랙헤드: 각질 제거
    "blackhead": [
        "exfoliating",
    ],
    # 여드름: 항균, 항여드름
    "acne": [
        "anti_acne", "antimicrobial",
    ],
    # 민감성: 항염, 상처 회복, 마이크로바이옴
    "sensitivity": [
        "anti_inflammatory", "wound_healing", "microbiome_balance",
    ],
    # 멜라닌(색소): 미백, 항산화
    "melanin": [
        "anti_hyperpigmentation", "brightening", "antioxidant",
    ],
    # 수분: 보습
    "hydration": [
        "hydration",
    ],
    # 거칠기: 각질 제거 + 장벽 강화
    "rough": [
        "exfoliating", "barrier_repair",
    ],
    # 홍조·붉은점: 항염, 마이크로바이옴
    "red_spot": [
        "anti_inflammatory", "microbiome_balance",
    ],
    # oily (유분): acne 점수 그대로 사용 — _calc_concern_scores에서 처리
    # pore (모공): comedogenicity 역수 proxy — _calc_concern_scores에서 처리
}

# INCI API compositeStrength → 효능 기본 점수
STRENGTH_SCORE: dict[str, float] = {
    "strong":   0.9,
    "moderate": 0.55,
    "weak":     0.25,
}

# INCI API bestEvidenceLevel → 근거 수준 가중치
EVIDENCE_WEIGHT: dict[str, float] = {
    "meta_analysis": 1.0,
    "rct":           0.85,
    "in_vitro":      0.6,
    "anecdotal":     0.4,
}

# INCI API skinTypeCompatibility 값 → 수치 변환
# 피부 타입: 지성=oily, 건성=dry, 중성=normal, 복합성=combination
COMPAT_SCORE: dict[str, float] = {
    "good":    1.0,
    "neutral": 0.5,
    "caution": 0.1,
    "avoid":   0.0,
}

# INCI API irritancyPotential → 자극 점수
IRRITANCY_SCORE: dict[str, float] = {
    "none":     0.0,
    "low":      0.25,
    "moderate": 0.6,
    "high":     1.0,
}

FRAGRANCE_NAMES = {"FRAGRANCE", "PARFUM", "FRAGRANCE/PARFUM", "AROMA"}


def _calc_concern_scores(product: dict) -> dict[str, float]:
    """INCI topEffects를 11개 피부 고민 효능 점수로 변환.

    점수 산출 (9개 기본 concern):
        효능 합산 = 카테고리 내 매칭 target의 (compositeStrength × bestEvidenceLevel) 합계
        최종 점수 = 효능 합산 × (inci_clean_beauty_score/100 × inci_coverage)

    proxy concern:
        oily  = acne 점수 그대로 (API에 유분 관련 target 없음)
        pore  = (1 - comedogenicityScore/5) × ingredient_weight (낮을수록 모공에 좋음)
    """
    efficacy = product.get("inci_efficacy_summary") or {}
    top_effects = efficacy.get("topEffects") or []

    effect_map: dict[str, tuple[str, str]] = {}
    for eff in top_effects:
        target = eff.get("target", "").lower()
        effect_map[target] = (
            eff.get("compositeStrength", ""),
            eff.get("bestEvidenceLevel", ""),
        )

    clean_beauty = product["inci_clean_beauty_score"] / 100
    coverage = product["inci_coverage"]
    ingredient_weight = clean_beauty * coverage

    scores: dict[str, float] = {}
    for concern, targets in CONCERN_TARGETS.items():
        total = 0.0
        for t in targets:
            if t in effect_map:
                strength, evidence = effect_map[t]
                total += STRENGTH_SCORE.get(strength, 0.0) * EVIDENCE_WEIGHT.get(evidence, 0.5)
        scores[concern] = round(total * ingredient_weight, 3)

    # oily: API에 유분 관련 target 없음 → acne 점수 동일 사용
    scores["oily"] = scores["acne"]

    # pore: API에 모공 관련 target 없음 → comedogenicity 역수 proxy
    # comedogenicityScore(0~5)가 낮을수록 모공에 좋음 → (1 - comedo/5) × ingredient_weight
    comedo = product.get("inci_comedogenicity_score") or 0.0
    scores["pore"] = round((1 - comedo / 5) * ingredient_weight, 3)

    return scores


def _calc_skin_type_compat(product: dict) -> dict[str, float]:
    """INCI skinTypeCompatibility를 4개 피부 타입 호환성 점수로 변환.

    피부 타입 enum: 0=지성(oily), 1=건성(dry), 2=중성(normal), 3=복합성(combination)
    good=1.0 / neutral=0.5 / caution or avoid=0.0 / 데이터 없음=0.5
    """
    compat = product.get("inci_skin_type_compatibility") or {}

    def _score(key: str) -> float:
        return COMPAT_SCORE.get(compat.get(key) or "", 0.5)

    return {
        "skin_type_oily":        _score("oily"),        # 지성 (0)
        "skin_type_dry":         _score("dry"),         # 건성 (1)
        "skin_type_normal":      _score("normal"),      # 중성 (2)
        "skin_type_combination": _score("combination"), # 복합성 (3)
    }


def _calc_risk_flags(product: dict) -> dict:
    """5가지 위험도 플래그 계산."""
    parsed = product.get("inci_parsed_ingredients") or []

    max_irritancy = 0.0
    for ingr in parsed:
        val = IRRITANCY_SCORE.get(ingr.get("irritancyPotential") or "", 0.0)
        max_irritancy = max(max_irritancy, val)

    comedo = product.get("inci_comedogenicity_score")
    comedogenicity_risk = round(comedo / 5, 3) if comedo is not None else 0.0

    ingredients_upper = {str(i).upper().strip() for i in (product.get("ingredients") or [])}
    fragrance_risk = 1 if ingredients_upper & FRAGRANCE_NAMES else 0

    allergen_risk = 1 if product.get("inci_allergen_flags") else 0

    pregnancy_safe = product.get("inci_pregnancy_safe")
    pregnancy_risk = 1 if pregnancy_safe is False else 0

    return {
        "irritancy_risk":      round(max_irritancy, 3),
        "comedogenicity_risk": comedogenicity_risk,
        "fragrance_risk":      fragrance_risk,
        "allergen_risk":       allergen_risk,
        "pregnancy_risk":      pregnancy_risk,
    }


def _has_weights(p: dict) -> bool:
    return (
        p.get("inci_clean_beauty_score") is not None
        and p.get("inci_coverage") is not None
    )


async def run() -> None:
    products_col, summarized_col = _get_cols()
    await summarized_col.create_index("product_name", unique=True)

    all_products = []
    async for doc in products_col.find({}, {"_id": 0}):
        all_products.append(doc)

    if not all_products:
        logger.warning("[STEP6] No products found.")
        return

    products = [p for p in all_products if p.get("inci_overall_safety_score") is not None]
    skipped_inci = len(all_products) - len(products)

    products = [p for p in products if _has_weights(p)]
    skipped_weights = len(all_products) - skipped_inci - len(products)

    logger.info(
        f"[STEP6] Processing {len(products)} products "
        f"(skipped {skipped_inci} without INCI data, {skipped_weights} without weight fields)..."
    )

    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for p in products:
        scores = _calc_concern_scores(p)
        compat = _calc_skin_type_compat(p)
        risks = _calc_risk_flags(p)

        record = {
            "product_name": p.get("product_name"),
            "brand":        p.get("brand"),
            "category":     p.get("category"),
            # 11개 피부 고민 효능 점수
            "dark_circle":  scores["dark_circle"],   # 다크서클
            "wrinkle":      scores["wrinkle"],       # 주름
            "oily":         scores["oily"],          # 유분
            "pore":         scores["pore"],          # 모공
            "blackhead":    scores["blackhead"],     # 블랙헤드
            "acne":         scores["acne"],          # 여드름
            "sensitivity":  scores["sensitivity"],   # 민감성
            "melanin":      scores["melanin"],       # 멜라닌(색소)
            "hydration":    scores["hydration"],     # 수분
            "rough":        scores["rough"],         # 거칠기
            "red_spot":     scores["red_spot"],      # 홍조·붉은점
            # 4개 피부 타입 호환성 (1.0=good / 0.5=neutral / 0.0=caution)
            **compat,
            # 5개 위험도 플래그
            **risks,
            "summarized_at": now,
        }

        await summarized_col.update_one(
            {"product_name": record["product_name"]},
            {"$set": record},
            upsert=True,
        )
        logger.info(
            f"[STEP6] {record['product_name']} | "
            f"다크서클{scores['dark_circle']} 주름{scores['wrinkle']} "
            f"유분{scores['oily']} 모공{scores['pore']} 블랙헤드{scores['blackhead']} "
            f"여드름{scores['acne']} 민감{scores['sensitivity']} 멜라닌{scores['melanin']} "
            f"수분{scores['hydration']} 거칠기{scores['rough']} 홍조{scores['red_spot']}"
        )

    logger.info(
        f"[STEP6] Done. Saved {len(products)} records to products_summarized. "
        f"(skipped {skipped_inci} without INCI data, {skipped_weights} without weight fields)"
    )


async def fetch_all() -> list[dict]:
    _, summarized_col = _get_cols()
    records = []
    async for doc in summarized_col.find({}, {"_id": 0}):
        records.append(doc)
    return records


if __name__ == "__main__":
    asyncio.run(run())
