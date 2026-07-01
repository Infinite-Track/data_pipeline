import asyncio
import json
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

import steps.step1_summarize as step1
import steps.step2_extract as step2
import steps.step3_save as step3
import steps.step4_analyze_reviews as step4
import steps.step5_extract_products_info as step5
import steps.step6_summarize_products as step6
import steps.step7_normalization as step7
from steps.step1_summarize import extract_video_id

load_dotenv()

ROOT = Path(__file__).parent
URLS_FILE = ROOT / "urls.txt"
PRODUCTS_SUMMARIZED_FILE = ROOT / "products_summarized.json"


def read_urls(path: Path) -> list[str]:
    if not path.exists():
        logger.error(f"urls.txt not found at {path}")
        return []
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


async def main() -> None:
    logger.info("=" * 50)
    logger.info("파이프라인 시작")
    logger.info("=" * 50)

    urls = read_urls(URLS_FILE)
    if not urls:
        logger.warning("처리할 URL이 없습니다.")
        return

    await step3.ensure_indexes()

    # ──────────────────────────────────────────────────────────────
    #  STEP 1 · 2 · 3   영상 URL → 요약 → 제품 추출 → DB 저장
    # ──────────────────────────────────────────────────────────────
    for url in urls:
        video_id = extract_video_id(url)
        if not video_id:
            logger.warning(f"[SKIP] URL 파싱 실패: {url}")
            continue

        if await step3.already_exists(video_id):
            logger.info(f"[SKIP] 이미 처리된 영상: {video_id}")
            continue

        # STEP 1 ─ 영상 요약
        summary = await step1.run(url)
        if not summary:
            logger.error(f"[FAIL] STEP 1 요약 실패: {video_id}")
            continue

        # STEP 2 ─ 스킨케어 제품 추출
        products = await step2.run(summary)
        if not products:
            logger.warning(f"[SKIP] STEP 2 추출된 제품 없음: {video_id}")
            continue

        # STEP 3 ─ reviews 컬렉션에 저장
        for product in products:
            record = {**summary, **product, "source": "youtube"}
            await step3.save(record)
            logger.info(f"[SAVE] {video_id} → {product.get('product_name')}")

    # ──────────────────────────────────────────────────────────────
    #  STEP 4   reviews 집계 → products 컬렉션
    # ──────────────────────────────────────────────────────────────
    await step4.run()

    # ──────────────────────────────────────────────────────────────
    #  STEP 5   Gemini 성분 검색 + INCI API 분석 → products 업데이트
    # ──────────────────────────────────────────────────────────────
    await step5.run()

    # ──────────────────────────────────────────────────────────────
    #  STEP 6   효능·위험도 점수 요약 → products_summarized 컬렉션
    # ──────────────────────────────────────────────────────────────
    await step6.run()

    # ──────────────────────────────────────────────────────────────
    #  STEP 7   피부 고민 점수 min-max 정규화 → products_summarized 업데이트
    # ──────────────────────────────────────────────────────────────
    await step7.run()

    # products_summarized 컬렉션 → JSON 내보내기
    records = await step6.fetch_all()
    PRODUCTS_SUMMARIZED_FILE.write_text(
        json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(f"[EXPORT] {len(records)}개 → products_summarized.json")

    logger.info("=" * 50)
    logger.info("파이프라인 완료")
    logger.info("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
