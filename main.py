"""
seoulection 데이터 파이프라인 v2 (2차 스프린트)

YouTube 채널 자동 추적 → 신규 영상 수집 → 리뷰 검증 → 상품 집계 →
성분 추출(Gemini 크롤링) → 성분 특징 분석(INCI API) → 추천 스키마 요약 → 고정기준 정규화

각 step 모듈의 run()은 DB에만 부작용이 격리된 순수 async 함수로 작성되어 있다.
현재 단계에서는 아래처럼 순차 실행만 하지만, 추후 Prefect + EC2로 옮길 때는
run()을 그대로 @task로 감싸고 main()을 @flow로 감싸기만 하면 되도록 구조를 맞춰뒀다.
"""
import asyncio
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger
from prefect import flow, task

import steps.step1_detect_new_videos as step1
import steps.step2_collect_reviews as step2
import steps.step3_validate_reviews as step3
import steps.step4_analyze_products as step4
import steps.step5_extract_ingredients as step5
import steps.step6_extract_ingredient_features as step6
import steps.step7_summarize_products as step7
import steps.step8_normalize_scores as step8

load_dotenv()


# Prefect UI에서 단계별로 성공/실패/소요시간을 구분해서 보기 위한 얇은 wrapper.
# 각 step의 run() 내부 로직(재시도, 상태머신 등)은 그대로 두고 오케스트레이션만 감싼다.
@task(name="step1_detect_new_videos", task_run_name="step1-run")
async def run_step1() -> None:
    await step1.run()


@task(name="step2_collect_reviews", task_run_name="step2-run")
async def run_step2() -> None:
    await step2.run()


@task(name="step3_validate_reviews", task_run_name="step3-run")
async def run_step3() -> None:
    await step3.run()


@task(name="step4_analyze_products", task_run_name="step4-run")
async def run_step4() -> None:
    await step4.run()


@task(name="step5_extract_ingredients", task_run_name="step5-run")
async def run_step5() -> None:
    await step5.run()


@task(name="step6_extract_ingredient_features", task_run_name="step6-run")
async def run_step6() -> None:
    await step6.run()


@task(name="step7_summarize_products", task_run_name="step7-run")
async def run_step7() -> None:
    await step7.run()


@task(name="step8_normalize_scores", task_run_name="step8-run")
async def run_step8() -> None:
    await step8.run()


@flow(name="kbeauty-pipeline", flow_run_name=lambda: f"pipeline-{datetime.now():%Y%m%d-%H%M}")
async def main() -> None:
    logger.info("=" * 50)
    logger.info("파이프라인 v2 시작")
    logger.info("=" * 50)

    # STEP 1 · 채널별 신규 영상 판별 (YouTube Data API + 규칙 필터 + LLM 리뷰영상 판별)
    await run_step1()

    # STEP 2 · PENDING 영상 → Gemini 요약 → raw_reviews 저장
    await run_step2()

    # STEP 3 · raw_reviews 검증(제품 추출 + 품질 채점) → reviews 저장
    await run_step3()

    # STEP 4 · reviews 집계(품질 통과분만, 상품명 정규화) → products 저장
    await run_step4()

    # STEP 5 · Gemini 크롤링(브랜드 공식 사이트 / 올리브영)으로 성분 추출 → products 업데이트
    await run_step5()

    # STEP 6 · INCI API로 성분 특징 분석 → products.inciapi_raw_data 저장
    await run_step6()

    # STEP 7 · 11개 효능 + 피부타입 적합도 + 위험도 점수 산출 → products_summarized 저장
    await run_step7()

    # STEP 8 · 효능 점수 고정 기준 정규화(0~100) → products_normalized 저장
    await run_step8()

    logger.info("=" * 50)
    logger.info("파이프라인 v2 완료")
    logger.info("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
