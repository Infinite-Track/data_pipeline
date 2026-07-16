"""
STEP 8 · 정규화

products_summarized의 11개 효능 점수를 0~100 스케일로 정규화해 products_normalized에 저장한다.

1차 스프린트(v1)에서는 "현재 DB에 있는 상품들의 min/max"로 정규화했는데,
이 방식은 새 상품이 들어올 때마다 min/max가 바뀌어 과거에 계산된 점수가 조용히 변한다는 문제가 있다.

그래서 v2는 "고정 기준(fixed reference)" 방식을 쓴다:
    각 효능 점수의 이론적 최댓값을 step7의 산출 공식(target 개수 × 최대 강도 0.9)에서
    미리 계산해 상수로 고정해두고, 그 값 기준으로만 정규화한다.
    → 데이터가 새로 쌓여도 과거 상품의 정규화 점수는 변하지 않는다.

FIXED_MAX 상수는 step7의 CONCERN_TARGETS 개수를 기반으로 한 이론적 상한이며,
실제 운영 데이터 분포를 보고 재보정이 필요할 수 있다 (지금은 개요 단계의 잠정값).
"""
import asyncio

from loguru import logger

from steps.db import get_collection
from steps.step7_summarize_products import CONCERN_TARGETS

MAX_STRENGTH = 0.9  # STRENGTH_SCORE["strong"]
MAX_EVIDENCE = 1.0  # EVIDENCE_WEIGHT["meta_analysis"]

# concern별 이론적 최댓값 = 매칭 가능한 target 개수 × 최대 강도 × 최대 근거수준
FIXED_MAX: dict[str, float] = {
    concern: round(len(targets) * MAX_STRENGTH * MAX_EVIDENCE, 3)
    for concern, targets in CONCERN_TARGETS.items()
}
FIXED_MAX["oily_intensity"] = FIXED_MAX["acne"]  # step7에서 acne 점수를 그대로 재사용하는 proxy
FIXED_MAX["pores"] = 1.0  # step7 공식상 (1 - comedo/5) * weight의 이론적 최댓값

CONCERN_FIELDS = list(FIXED_MAX.keys())


def _get_summarized_collection():
    return get_collection("products_summarized")


def _get_normalized_collection():
    return get_collection("products_normalized")


def normalize_score(field: str, raw: float) -> float:
    """고정 기준(FIXED_MAX)으로 0~100 스케일로 변환한다. 이론 최댓값을 넘는 값은 100으로 clamp."""
    max_value = FIXED_MAX[field]
    if max_value <= 0:
        return 0.0
    return round(min(raw, max_value) / max_value * 100, 2)


async def run() -> None:
    src_col, dst_col = _get_summarized_collection(), _get_normalized_collection()
    await dst_col.create_index("product_name", unique=True)

    docs = [doc async for doc in src_col.find({}, {"_id": 0})]
    if not docs:
        logger.warning("[STEP8] products_summarized 컬렉션이 비어 있습니다.")
        return

    for doc in docs:
        record = dict(doc)
        for field in CONCERN_FIELDS:
            if record.get(field) is not None:
                record[field] = normalize_score(field, record[field])
        await dst_col.update_one({"product_name": record["product_name"]}, {"$set": record}, upsert=True)

    logger.info(f"[STEP8] {len(docs)}개 상품 → products_normalized 저장 완료 (고정 기준 정규화).")


if __name__ == "__main__":
    asyncio.run(run())
