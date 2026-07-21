"""
STEP 8 · 정규화

products_summarized의 효능 점수를 0~100 스케일로 정규화해 products_normalized에 저장한다.

매 실행마다 products_summarized 전체를 다시 읽어와, 그 시점에 DB에 있는 상품들의
실제 min/max를 기준으로 정규화한다 (min-max normalization). 새 상품이 추가되면
분포가 바뀌어 과거 상품의 정규화 점수도 함께 재계산될 수 있다.
"""
import asyncio

from loguru import logger

from steps.db import get_collection
from steps.step7_summarize_products import CONCERN_TARGETS

# step7이 실제로 채우는 효능 점수 필드들 (CONCERN_TARGETS 키 + proxy로 계산되는 pores).
CONCERN_FIELDS = list(CONCERN_TARGETS.keys()) + ["pores"]


def _get_summarized_collection():
    return get_collection("products_summarized")


def _get_normalized_collection():
    return get_collection("products_normalized")


def _compute_min_max(docs: list[dict]) -> dict[str, tuple[float, float]]:
    """각 효능 필드별로 현재 products_summarized 데이터의 최솟값/최댓값을 계산한다."""
    min_max: dict[str, tuple[float, float]] = {}
    for field in CONCERN_FIELDS:
        values = [doc[field] for doc in docs if doc.get(field) is not None]
        if values:
            min_max[field] = (min(values), max(values))
    return min_max


def normalize_score(raw: float, min_value: float, max_value: float) -> float:
    """min-max로 0~100 스케일로 변환한다. min==max(전 상품이 같은 값)면 100으로 처리한다."""
    if max_value <= min_value:
        return 100.0
    return round((raw - min_value) / (max_value - min_value) * 100, 2)


async def run() -> None:
    src_col, dst_col = _get_summarized_collection(), _get_normalized_collection()
    await dst_col.create_index("product_name", unique=True)

    docs = [doc async for doc in src_col.find({}, {"_id": 0})]
    if not docs:
        logger.warning("[STEP8] products_summarized 컬렉션이 비어 있습니다.")
        return

    min_max = _compute_min_max(docs)

    for doc in docs:
        record = dict(doc)
        for field in CONCERN_FIELDS:
            if record.get(field) is not None and field in min_max:
                min_value, max_value = min_max[field]
                record[field] = normalize_score(record[field], min_value, max_value)
        await dst_col.update_one({"product_name": record["product_name"]}, {"$set": record}, upsert=True)

    logger.info(f"[STEP8] {len(docs)}개 상품 → products_normalized 저장 완료 (min-max 정규화).")


if __name__ == "__main__":
    asyncio.run(run())
