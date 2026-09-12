"""
STEP 9 · 정규화

products_summarized의 효능 점수를 0~100 스케일로 정규화해 Postgres products_normalized에 저장한다.
(정규화 결과는 서빙 전용이라 Mongo에는 저장하지 않는다 — Postgres에만 발행.)

매 실행마다 products_summarized 전체를 다시 읽어와, 그 시점에 DB에 있는 상품들의
실제 min/max를 기준으로 정규화한다 (min-max normalization). 새 상품이 추가되면
분포가 바뀌어 과거 상품의 정규화 점수도 함께 재계산될 수 있다. 정규화는 PySpark로 수행한다.
"""
import asyncio

from loguru import logger

from steps.db import get_collection
from steps.step8_summarize_products import CONCERN_TARGETS
from steps import postgres_publish as pg
from steps import spark_normalize

# step8이 실제로 채우는 효능 점수 필드들 (CONCERN_TARGETS 키 + proxy로 계산되는 pores).
CONCERN_FIELDS = list(CONCERN_TARGETS.keys()) + ["pores"]


def _get_summarized_collection():
    return get_collection("products_summarized")


async def run() -> None:
    docs = [doc async for doc in _get_summarized_collection().find({}, {"_id": 0})]
    if not docs:
        logger.warning("[STEP9] products_summarized 컬렉션이 비어 있습니다.")
        return

    # PySpark로 전역 min-max 정규화 (집계 + transform). 매 배치 전체 재계산.
    normalized = await asyncio.to_thread(spark_normalize.normalize, docs, CONCERN_FIELDS)

    # products_normalized는 Postgres에만 저장한다(Mongo 저장 없음).
    conn = pg.connect()
    try:
        pg.ensure_catalog_schema(conn)
        pg.ensure_normalized_schema(conn)
        for record in normalized:
            await asyncio.to_thread(pg.publish_normalized, conn, record)
    finally:
        conn.close()

    logger.info(f"[STEP9] {len(normalized)}개 상품 → products_normalized(Postgres) 저장 완료 (Spark min-max 정규화).")


if __name__ == "__main__":
    asyncio.run(run())
