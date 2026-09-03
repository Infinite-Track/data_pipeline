"""
STEP 3 · 리뷰 검증

raw_reviews는 이미 STEP2에서 제품 단위로 나뉘어 있으므로(문서 1개 = 영상 1개 속 제품 1개),
STEP3는 그 제품별 content를 검증 AI에 전달해 두 가지를 수행한다.

    1) 브랜드/카테고리 추출: content에서 이 제품의 brand를 뽑고, category는 ALLOWED_CATEGORIES
       6개 중 하나로만 분류한다 (Cleansers/Toners/Treatments/Moisturizers/Sunscreens/Face Masks).
    2) 품질 채점: 3축 품질 지표를 1~5점으로 산출
        - specificity : 상품 정보가 얼마나 구체적인가
        - relevance   : 채널/영상 주제와 얼마나 관련 있는가
        - skin_info   : 피부 타입/고민 관련 정보가 얼마나 유용한가

품질 점수(통과/탈락)와 무관하게 검증된 제품은 reviews 컬렉션에 저장한다.
품질 필터링은 다음 step4에서 수행한다.

단, category가 ALLOWED_CATEGORIES 6개 중 하나로 분류되지 않으면(스킨케어가 아니거나
6개 범주 밖의 제품) 이 리뷰는 아예 reviews에 저장하지 않고 버린다. 이건 "품질 미달"과는
다른 문제라 quality_passed 판단으로 미루지 않고 여기서 바로 걸러낸다.

검증 메타데이터(validator_model, validator_prompt_version, validated_at)를 함께 기록해
추후 프롬프트를 바꿨을 때 어떤 버전으로 채점된 데이터인지 추적할 수 있게 한다.

저장 시 status="PENDING"으로 시작한다. 이후 STEP4가 품질 통과 여부 판정 + Amazon Global
검증을 거쳐 DISQUALIFIED(품질 미달) / NOTFOUND(Amazon에 없음) / COMPLETED(집계 대상)로 갱신한다.

멱등성: raw_reviews 문서에 processed_at이 이미 있으면 재처리하지 않는다.
"""
import asyncio
import datetime
import time

from loguru import logger

from steps.db import get_collection
from steps.gemini_client import get_client
from steps.llm_utils import parse_json_response

GEMINI_MODEL = "gemini-2.5-flash"
VALIDATOR_PROMPT_VERSION = "v1"  # 프롬프트를 바꿀 때마다 올려서 채점 기준 변경 이력을 남긴다

MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 5  # 재시도 간격. 시도할 때마다 배수로 늘어난다 (5초 → 10초).

# 동시에 처리할 리뷰 수(비동기 병렬). 네트워크 대기가 겹쳐 처리량이 크게 오른다.
# 429(rate limit)가 잦으면 낮추면 된다.
CONCURRENCY = 8

# 허용하는 카테고리 6개. 이 중 하나로 분류되지 않으면 리뷰 자체를 저장하지 않는다.
ALLOWED_CATEGORIES = (
    "Cleansers",     # 클렌저
    "Toners",        # 토너
    "Treatments",    # 트리트먼트 (세럼/앰플/에센스류)
    "Moisturizers",  # 모이스처라이저 (보습제/크림/로션)
    "Sunscreens",    # 선크림 (자외선 차단제)
    "Face Masks",    # 페이스 마스크 (마스크팩)
)


def _get_raw_reviews_collection():
    return get_collection("raw_reviews")


def _get_reviews_collection():
    return get_collection("reviews")


async def ensure_indexes() -> None:
    col = _get_reviews_collection()
    await col.create_index([("video_id", 1), ("product_name", 1)], unique=True)
    logger.info("[STEP3] reviews.(video_id, product_name) 유니크 인덱스 확인 완료.")


def _build_prompt(product_name: str, content: str) -> str:
    categories_list = "\n".join(f"- {c}" for c in ALLOWED_CATEGORIES)
    return (
        "You are a strict quality reviewer for K-beauty review content extracted from YouTube videos.\n\n"
        f'The text below describes ONE product: "{product_name}".\n'
        "Extract its brand and classify it into EXACTLY ONE of these categories:\n"
        f"{categories_list}\n"
        "If it clearly does not belong to any of these 6 categories (e.g. makeup, haircare, "
        "bodycare, supplements, tools/devices), set category to null.\n\n"
        "Also score how useful this description is.\n\n"
        "Return raw JSON only (no markdown):\n"
        "{\n"
        '  "brand": string or null,\n'
        '  "category": one of the 6 category names above, or null,\n'
        '  "specificity": integer 1-5,   // how concrete/specific the product info is\n'
        '  "relevance": integer 1-5,     // how relevant this mention is to the review topic\n'
        '  "skin_info": integer 1-5      // how useful the skin-type/concern info is\n'
        "}\n\n"
        f"Description:\n{content}\n"
    )


def _validate_content_sync(product_name: str, content: str) -> dict | None:
    """검증 AI를 호출한다. 실패(API 오류/형식 오류)하면 MAX_ATTEMPTS번까지, 시도 사이에
    잠깐 텀을 두고 재시도한다 (Gemini 쪽 일시적 과부하(503 UNAVAILABLE) 등을 감안).
    """
    client = get_client()
    if not client:
        return None

    for attempt in range(MAX_ATTEMPTS):
        try:
            response = client.models.generate_content(model=GEMINI_MODEL, contents=_build_prompt(product_name, content))
            parsed = parse_json_response(response.text or "")
            if isinstance(parsed, dict):
                return {
                    "brand": parsed.get("brand") or None,
                    "category": parsed.get("category") or None,
                    "specificity": int(parsed.get("specificity", 1)),
                    "relevance": int(parsed.get("relevance", 1)),
                    "skin_info": int(parsed.get("skin_info", 1)),
                }
            logger.warning(f"[STEP3] 출력 형식 이상 (시도 {attempt + 1}/{MAX_ATTEMPTS})")
        except Exception as exc:
            logger.warning(f"[STEP3] 검증 실패 (시도 {attempt + 1}/{MAX_ATTEMPTS}): {exc}")

        if attempt < MAX_ATTEMPTS - 1:
            wait_seconds = RETRY_DELAY_SECONDS * (attempt + 1)
            logger.info(f"[STEP3] {wait_seconds}초 대기 후 재시도합니다.")
            time.sleep(wait_seconds)

    return None


async def _save_review(raw_review: dict, scored: dict) -> None:
    col = _get_reviews_collection()
    record = {
        "raw_review_id": raw_review["_id"],
        "video_id": raw_review["video_id"],
        "product_name": raw_review["product_name"],
        "ad_likelihood": raw_review.get("ad_likelihood", 0.0),
        "validator_model": GEMINI_MODEL,
        "validator_prompt_version": VALIDATOR_PROMPT_VERSION,
        "validated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        **scored,
    }
    await col.update_one(
        {"video_id": record["video_id"], "product_name": record["product_name"]},
        # status는 STEP4가 관리한다 (PENDING → DISQUALIFIED/NOTFOUND/COMPLETED).
        # 이미 처리된 리뷰의 진행 상태를 덮어쓰지 않도록 최초 삽입 시에만 PENDING을 넣는다.
        {"$set": record, "$setOnInsert": {"status": "PENDING"}},
        upsert=True,
    )


async def _mark_processed(raw_review_id) -> None:
    await _get_raw_reviews_collection().update_one(
        {"_id": raw_review_id},
        {"$set": {"processed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}},
    )


async def run() -> None:
    await ensure_indexes()

    raw_col = _get_raw_reviews_collection()
    unprocessed = [doc async for doc in raw_col.find({"processed_at": {"$exists": False}})]
    if not unprocessed:
        logger.info("[STEP3] 검증할 raw_reviews가 없습니다.")
        return

    sem = asyncio.Semaphore(CONCURRENCY)

    async def _process(raw_review: dict) -> None:
        async with sem:
            try:
                scored = await asyncio.wait_for(
                    asyncio.to_thread(_validate_content_sync, raw_review["product_name"], raw_review["content"]),
                    timeout=150,
                )
            except asyncio.TimeoutError:
                # Gemini 호출이 멈춘 경우: 미처리 유지(재시도용)하고 넘어간다.
                logger.warning(f"[STEP3][TIMEOUT] 검증 타임아웃: {raw_review['video_id']} / {raw_review['product_name']}")
                scored = None
            if not scored:
                # 기술적 실패 → processed_at을 남기지 않아 다음 실행에서 재시도한다.
                logger.warning(f"[STEP3][RETRY] 검증 실패: {raw_review['video_id']} / {raw_review['product_name']}")
                return
            if scored["category"] not in ALLOWED_CATEGORIES:
                # 허용 6개 카테고리 밖 → 영구 제외. processed_at만 남기고 reviews엔 저장 안 함.
                await _mark_processed(raw_review["_id"])
                logger.info(
                    f"[STEP3][DROP] 허용 카테고리 아님({scored['category']!r}): "
                    f"{raw_review['video_id']} / {raw_review['product_name']}"
                )
                return
            await _save_review(raw_review, scored)
            await _mark_processed(raw_review["_id"])
            logger.info(f"[STEP3][SAVE] {raw_review['video_id']} / {raw_review['product_name']} 검증 완료.")

    # 동시 CONCURRENCY개씩 병렬 처리 (세마포어로 상한).
    await asyncio.gather(*(_process(r) for r in unprocessed))


if __name__ == "__main__":
    asyncio.run(run())
