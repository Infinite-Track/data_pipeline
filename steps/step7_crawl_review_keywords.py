"""
STEP 7 · 화해 리뷰 키워드 분석 (오케스트레이터)

products에서 status=READY_FOR_KEYWORD_ANALYSIS인 상품에 대해:
  ① 크롤링 (crawler): name_kr로 화해 검색·매칭 → 리뷰 수집 (순차 · rate-limit이라 Spark 밖)
  ② 분석   (analysis): 전 제품 리뷰를 PySpark로 카테고리별 언급률 상위 5개 산출
  ③ 저장   (postgres_publish): reviews_keyword(Postgres)에 발행 → status를 READY_FOR_ANALYSIS로 넘김

실제 로직은 steps/reviews_keyword_analysis/{crawler,analysis}.py 로 분리되어 있고,
이 파일은 "무엇을 언제" 하는 흐름만 담는다. name_kr이 없으면 그 상품은 건너뛴다.
"""
import asyncio

from loguru import logger

from steps.db import get_collection
from steps import postgres_publish as pg
from steps.reviews_keyword_analysis import crawler, analysis

# 분석에 쓸 리뷰가 이 개수 미만이면 표본이 작아 비율이 불안정하므로 키워드를 뽑지 않는다.
MIN_REVIEWS = 100


async def run() -> None:
    products_col = get_collection("products")
    pending = [
        d async for d in products_col.find(
            {"status": "READY_FOR_KEYWORD_ANALYSIS"},
            {"_id": 0, "asin": 1, "name": 1, "name_kr": 1, "brand": 1, "category": 1,
             "price": 1, "mention_count": 1, "ad_ratio": 1, "thumbnail_url": 1},
        )
    ]
    if not pending:
        logger.info("[STEP7] 키워드 분석 대상(READY_FOR_KEYWORD_ANALYSIS)이 없습니다.")
        return

    # ① 크롤링(순차 · 화해 예의 딜레이) — (asin, review_text) 행을 메모리에 수집.
    rows: list[tuple[str, str]] = []
    for p in pending:
        asin, name_kr, brand = p["asin"], p.get("name_kr"), p.get("brand")
        if not name_kr:
            logger.warning(f"[STEP7][SKIP] name_kr 없음 — 화해 검색 불가: {asin}")
            continue
        try:
            hid = await asyncio.to_thread(crawler.resolve_hwahae_id, name_kr, brand)
            if hid is None:
                logger.info(f"[STEP7][NOMATCH] 화해 매칭 실패: {asin} {name_kr!r}")
                continue
            reviews = await asyncio.to_thread(crawler.fetch_reviews, hid)
        except Exception as exc:
            logger.warning(f"[STEP7][FAIL] {asin} {name_kr!r}: {exc}")
            continue
        texts = [t for t in (crawler.review_text(rv) for rv in reviews) if t.strip()]
        if len(texts) < MIN_REVIEWS:
            logger.info(f"[STEP7][SKIP-FEW] {asin}: 리뷰 {len(texts)}건(<{MIN_REVIEWS}) — 키워드 생략")
            await asyncio.sleep(1.5)
            continue
        rows.extend((asin, t) for t in texts)
        logger.info(f"[STEP7][CRAWL] {asin}: 리뷰 {len(texts)}건 수집")
        await asyncio.sleep(1.5)   # 화해에 대한 예의상 간격

    # ② 분석(PySpark, 전 제품 한 번에). multiple의 분모(카테고리 baseline) 선택용으로 카테고리 전달.
    categories = {p["asin"]: p.get("category") for p in pending}
    result = await asyncio.to_thread(analysis.analyze_all, rows, categories)
    logger.info(f"[STEP7][SPARK] 리뷰 {len(rows)}건 → {len(result)}개 상품 키워드 산출")

    # ③ Postgres 저장 + status 전이(대상은 모두 COMPLETE로 진행 — 키워드는 부가정보).
    conn = pg.connect()
    try:
        for p in pending:
            asin = p["asin"]
            top = result.get(asin, [])
            if top:
                try:
                    product_id = await asyncio.to_thread(pg.get_or_create_product_id, conn, p)
                    await asyncio.to_thread(pg.publish_keywords, conn, product_id, top)
                    logger.info(f"[STEP7][OK] {asin}: 키워드 {len(top)}개 저장")
                except Exception as exc:
                    conn.rollback()
                    logger.warning(f"[STEP7][PG] 키워드 발행 실패 asin={asin}: {exc}")
            else:
                logger.info(f"[STEP7][NOKW] {asin}: 키워드 없음(매칭/리뷰 실패) — 상태만 진행")
            await products_col.update_one({"asin": asin}, {"$set": {"status": "READY_FOR_ANALYSIS"}})
    finally:
        conn.close()


if __name__ == "__main__":
    # 단독 검증: 한국어 제품명으로 화해 검색 → 크롤링 → 단일머신 분석 (DB/Spark/PG 없이).
    import sys
    name_kr = sys.argv[1] if len(sys.argv) > 1 else \
        "아누아 나이아신아마이드 10 트라네사믹애씨드 4 다크 스팟 코렉팅 세럼"
    brand = sys.argv[2] if len(sys.argv) > 2 else None
    category = sys.argv[3] if len(sys.argv) > 3 else "Treatments"
    print(f"▶ 화해 검색: {name_kr!r} (category={category})")
    hid = crawler.resolve_hwahae_id(name_kr, brand)
    if hid is None:
        print("  매칭 실패")
    else:
        texts = [crawler.review_text(rv) for rv in crawler.fetch_reviews(hid)]
        top, n = analysis.analyze_texts(texts, category)
        print(f"  분석 리뷰 {n}건, 상위 {len(top)}개 키워드(카테고리 대비 배수):")
        for row in top:
            print(f"    - {row['keyword']}: x{row['multiple']:.2f}")
