"""
STEP 2 · 신규 수집 (영상 → 리뷰 원본)

videos 컬렉션에서 status=PENDING인 영상을 Gemini 3.5 Flash에 URL 그대로 전달해
"이 영상이 어떤 제품들을 다뤘고, 각 제품에 대해 뭐라고 설명했는지"를 제품 단위로 뽑아낸다.

영상 하나에 제품이 여러 개 언급될 수 있으므로, Gemini에게 제품별로 나눠서
그 제품에 대한 설명만 담은 content를 각각 만들어달라고 요청한다.
(다른 제품 얘기가 섞인 뭉뚱그린 요약이 아니라, "이 제품에 대해 유튜버가 뭐라고 했는지"만 담김)

출력은 raw_reviews 컬렉션에 저장하며, 영상 1개 → raw_reviews 문서는 "언급된 제품 개수"만큼 생긴다.
product_name/content/ad_likelihood는 모두 제품 단위로 매겨진다 (제품마다 광고성 정도가 다를 수 있어서).
review_summary는 content를 영어 1문장으로 줄인 것으로, Postgres youtuber_reviews.review_summary에 그대로 발행된다.

product_name은 항상 영어로만 추출한다 (한국어 리뷰 영상이라 한글/영어가 섞여서 나오기 쉬운데,
STEP3 브랜드·카테고리 추출, STEP4 Amazon 검색/그룹핑이 전부 이 값을 기준으로 동작하므로
여기서부터 형식을 통일해두면 이후 단계에서 표기 차이로 인한 문제가 줄어든다).

출력 형식이 깨지거나(JSON 파싱 실패) 모델이 과부하로 실패하면 1회 재시도한다.
재시도까지 실패하면 video.status를 EXCEPTION으로 남기고, 제품이 하나도 언급되지 않은 경우엔
COMPLETED로 남기고(정상 처리됐지만 다룰 게 없었던 경우) 다음 영상으로 넘어간다.
"""
import asyncio

from loguru import logger

from steps.db import get_collection
from steps.gemini_client import get_client
from steps.llm_utils import parse_json_response

try:
    from google.genai import types as genai_types
except ImportError:
    genai_types = None

GEMINI_MODEL = "gemini-3.5-flash"
MAX_ATTEMPTS = 2  # 최초 시도 1회 + 재시도 1회


def _get_videos_collection():
    return get_collection("videos")


def _get_raw_reviews_collection():
    return get_collection("raw_reviews")


async def ensure_indexes() -> None:
    col = _get_raw_reviews_collection()
    await col.create_index([("video_id", 1), ("product_name", 1)], unique=True)
    logger.info("[STEP2] raw_reviews.(video_id, product_name) 유니크 인덱스 확인 완료.")


def _build_prompt() -> str:
    return (
        "You are a K-beauty content analyst.\n\n"
        "Watch this video and identify EVERY distinct skincare/beauty product mentioned.\n"
        "For EACH product, write a SEPARATE summary describing ONLY what this video said about "
        "that product (claims, how it was used, key ingredients) — do not mix in other products.\n"
        "Also estimate, per product, how likely its mention is a paid ad / sponsored segment "
        "(explicit: 'gifted by', 'sponsored', 'discount code'; implicit: overly positive, brand-first language).\n\n"
        'Return raw JSON only, no markdown: an ARRAY where each element is:\n'
        '{"product_name": string, "content": string, "review_summary": string, "ad_likelihood": float between 0.0 and 1.0}\n'
        "- product_name: the product name in ENGLISH ONLY (include brand). If the video shows/says "
        "both a Korean and an English name, use the English name. If only a Korean name is shown, "
        "romanize/translate it to the closest official English product name — never output Korean "
        "characters or a mix of Korean and English in this field.\n"
        "- content: a thorough description about THIS product only\n"
        "- review_summary: ONE English sentence summarizing the reviewer's overall verdict on THIS product\n"
        "- ad_likelihood: 0.0 = definitely organic, 1.0 = definitely a paid ad\n"
        "If no skincare/beauty product is mentioned, return an empty array [].\n"
    )


def _summarize_video_sync(video_url: str) -> list[dict] | None:
    """영상을 제품 단위로 분리해 요약한다. 실패(파싱 불가) 시 None, 언급된 제품이 없으면 [] 반환."""
    client = get_client()
    if not client or not genai_types:
        return None

    for attempt in range(MAX_ATTEMPTS):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[
                    genai_types.Part(file_data=genai_types.FileData(file_uri=video_url, mime_type="video/*")),
                    genai_types.Part(text=_build_prompt()),
                ],
                config=genai_types.GenerateContentConfig(temperature=0.2),
            )
            parsed = parse_json_response(response.text or "")
            if isinstance(parsed, dict):
                parsed = [parsed]
            if isinstance(parsed, list):
                results = []
                for item in parsed:
                    name = item.get("product_name")
                    if not name or not str(name).strip():
                        continue
                    results.append({
                        "product_name": str(name).strip(),
                        "content": str(item.get("content", "")).strip(),
                        "review_summary": str(item.get("review_summary", "")).strip(),
                        "ad_likelihood": max(0.0, min(1.0, float(item.get("ad_likelihood", 0.0)))),
                    })
                return results
            logger.warning(f"[STEP2] 출력 형식 이상 (attempt {attempt + 1}/{MAX_ATTEMPTS}), 재시도.")
        except Exception as exc:
            logger.warning(f"[STEP2] Gemini 호출 실패 (attempt {attempt + 1}/{MAX_ATTEMPTS}): {exc}")
    return None


async def _save_raw_reviews(video: dict, products: list[dict]) -> None:
    col = _get_raw_reviews_collection()
    for product in products:
        record = {
            "video_id": video["video_id"],
            "product_name": product["product_name"],
            "content": product["content"],
            "review_summary": product["review_summary"],
            "ad_likelihood": product["ad_likelihood"],
        }
        await col.update_one(
            {"video_id": record["video_id"], "product_name": record["product_name"]},
            {"$set": record},
            upsert=True,
        )


async def _update_video_status(video_id: str, status: str) -> None:
    await _get_videos_collection().update_one({"video_id": video_id}, {"$set": {"status": status}})


async def run() -> None:
    await ensure_indexes()

    videos_col = _get_videos_collection()
    pending_videos = [doc async for doc in videos_col.find({"status": "PENDING"})]
    if not pending_videos:
        logger.info("[STEP2] 처리할 PENDING 영상이 없습니다.")
        return

    for video in pending_videos:
        products = await asyncio.to_thread(_summarize_video_sync, video["url"])
        if products is None:
            logger.error(f"[STEP2][FAIL] 요약 실패: {video['video_id']}")
            await _update_video_status(video["video_id"], "EXCEPTION")
            continue

        if not products:
            logger.info(f"[STEP2][SKIP] 언급된 제품 없음: {video['video_id']}")
            await _update_video_status(video["video_id"], "COMPLETED")
            continue

        await _save_raw_reviews(video, products)
        await _update_video_status(video["video_id"], "COMPLETED")
        logger.info(f"[STEP2][SAVE] {video['video_id']} → raw_reviews {len(products)}개 제품")


if __name__ == "__main__":
    asyncio.run(run())
