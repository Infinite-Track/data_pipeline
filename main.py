"""
seoulection 데이터 파이프라인 v2 (2차 스프린트)

YouTube 채널 자동 추적 → 신규 영상 수집 → 리뷰 검증 → 상품 집계 →
성분 추출(Gemini 크롤링) → 성분 특징 분석(INCI API) → 추천 스키마 요약 → 고정기준 정규화

각 step 모듈의 run()은 DB에만 부작용이 격리된 순수 async 함수로 작성되어 있다.
현재 단계에서는 아래처럼 순차 실행만 하지만, 추후 Prefect + EC2로 옮길 때는
run()을 그대로 @task로 감싸고 main()을 @flow로 감싸기만 하면 되도록 구조를 맞춰뒀다.
"""
import asyncio

from dotenv import load_dotenv
from loguru import logger

import steps.step1_detect_new_videos as step1
import steps.step2_collect_reviews as step2
import steps.step3_validate_reviews as step3
import steps.step4_analyze_products as step4
import steps.step5_extract_ingredients as step5
import steps.step6_extract_ingredient_features as step6
import steps.step7_summarize_products as step7
import steps.step8_normalize_scores as step8

load_dotenv()


async def main() -> None:
    logger.info("=" * 50)
    logger.info("파이프라인 v2 시작")
    logger.info("=" * 50)

    # STEP 1 · 채널별 신규 영상 판별 (YouTube Data API + 규칙 필터 + LLM 리뷰영상 판별)
    await step1.run()

    # STEP 2 · PENDING 영상 → Gemini 요약 → raw_reviews 저장
    await step2.run()

    # STEP 3 · raw_reviews 검증(제품 추출 + 품질 채점) → reviews 저장
    await step3.run()
    '''
    # STEP 4 · reviews 집계(품질 통과분만, 상품명 정규화) → products 저장
    await step4.run()

    # STEP 5 · Gemini 크롤링(브랜드 공식 사이트 / 올리브영)으로 성분 추출 → products 업데이트
    await step5.run()

    # STEP 6 · INCI API로 성분 특징 분석 → products.inciapi_raw_data 저장
    await step6.run()

    # STEP 7 · 11개 효능 + 피부타입 적합도 + 위험도 점수 산출 → products_summarized 저장
    await step7.run()

    # STEP 8 · 효능 점수 고정 기준 정규화(0~100) → products_normalized 저장
    await step8.run()
    '''
    logger.info("=" * 50)
    logger.info("파이프라인 v2 완료")
    logger.info("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
