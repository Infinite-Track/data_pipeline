"""
STEP 4 · 리뷰 분석 (리뷰 → 상품)

reviews 컬렉션(STEP3에서 status=PENDING으로 저장된 제품별 리뷰)을 아래 순서로 처리해
상품 단위로 집계한 products 컬렉션을 만든다.

    1) 품질 판정: specificity·relevance·skin_info 3축이 모두 QUALITY_MIN_SCORE 이상이 아니면
       DISQUALIFIED로 표시하고 이후 단계에서 제외한다.
    2) Amazon Global 검증 + 이름/브랜드/카테고리 통일: 품질 통과 리뷰를 (product_name, brand)
       원문 표기 기준으로 묶어서, 고유한 조합당 Gemini + Google 검색 그라운딩으로
       Amazon Global(amazon.com)에서 딱 1번만 검색한다. 최종 목표가 아마존 제휴링크로 소비자를
       유도하는 것이라, Amazon에 없는 상품은 NOTFOUND로 표시하고 제외한다. 찾으면 그 조합의
       모든 리뷰에 Amazon 리스팅 기준 product_name/brand/category(ALLOWED_CATEGORIES 6개 중
       하나)를 반영하고 COMPLETED로 표시한다 — 리뷰가 자체적으로 추출한 brand/category보다
       Amazon 공식 정보를 우선시한다.
       (같은 상품이 "슈퍼앰플"/"super ample"처럼 리뷰마다 다르게 표기돼도, Amazon 검증 이후에는
       같은 공식 명칭으로 통일되므로 다음 집계 단계에서 자연스럽게 하나로 합쳐진다.)
    3) 상품 단위 집계: 이번 실행에서 새로 COMPLETED된 리뷰(원래 PENDING이었던 것)만 골라
       product_name 기준으로 이번 배치분의 mention_count, ad_likelihood_sum을 계산한다.
    4) 저장: products 컬렉션에 $inc로 누적한다 (기존 누적치 + 이번 배치분). 매번 reviews
       전체를 다시 긁어와 처음부터 재계산하지 않고, 이번에 새로 처리된 만큼만 더한다.
       ad_ratio(ad_likelihood 평균)는 누적된 mention_count/ad_likelihood_sum으로 다시 계산한다.
       성분/INCI 분석은 아직 없으므로 신규 상품은 status=PENDING으로 시작해 step5/6이 채워나간다
       (이 status는 reviews.status와는 다른, 성분추출 파이프라인 진행 상태다).

reviews.status 값: PENDING → DISQUALIFIED / NOTFOUND / COMPLETED (모두 최종 상태, 재처리 안 함)

주의: 이번 배치 단위로 $inc만 하고 있어, "이미 COMPLETED된 리뷰가 나중에 수정/삭제되는 경우"는
반영되지 않는다 (지금은 리뷰가 한 번 COMPLETED되면 안 바뀐다는 전제). 진짜 증분 파이프라인
설계(워터마크 관리, 재계산 트리거 등)는 다음 스프린트에서 다룬다.
"""
import asyncio
import datetime
import re

from loguru import logger
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from steps.db import get_collection
from steps.gemini_client import get_client
from steps.llm_utils import parse_json_response
from steps.step3_validate_reviews import ALLOWED_CATEGORIES

try:
    from google.genai import types as genai_types
except ImportError:
    genai_types = None

GEMINI_MODEL = "gemini-2.5-flash"

# quality_passed 판정 기준: 3축 점수(1~5) 모두 이 값 이상이어야 통과.
# 값 자체는 운영하면서 조정될 수 있는 튜닝 파라미터다.
QUALITY_MIN_SCORE = 3


def _get_reviews_collection():
    return get_collection("reviews")


def _get_products_collection():
    return get_collection("products")


async def ensure_indexes() -> None:
    products_col = _get_products_collection()
    await products_col.create_index("name", unique=True)

    reviews_col = _get_reviews_collection()
    await reviews_col.create_index("status")

    logger.info("[STEP4] 인덱스 확인 완료 (products.name unique / reviews.status).")


def _quality_passed(review: dict) -> bool:
    return (
        review.get("specificity", 0) >= QUALITY_MIN_SCORE
        and review.get("relevance", 0) >= QUALITY_MIN_SCORE
        and review.get("skin_info", 0) >= QUALITY_MIN_SCORE
    )


def _normalize_field(text: str) -> str:
    """소문자로 바꾸고 공백을 전부 제거한다. (product_name/brand/category 최종 저장 형식)"""
    return re.sub(r"\s+", "", text.strip().lower())


async def _disqualify_low_quality_reviews(pending_reviews: list[dict]) -> list[dict]:
    """quality_passed 안 되는 리뷰는 DISQUALIFIED로 표시하고, 통과한 리뷰만 반환한다.

    Amazon 검색(비용이 드는 작업)을 하기 전에 먼저 걸러내, 어차피 집계에 안 쓰일
    리뷰에 검색 비용을 쓰지 않는다.
    """
    col = _get_reviews_collection()
    qualified = []
    for review in pending_reviews:
        if _quality_passed(review):
            qualified.append(review)
        else:
            await col.update_one({"_id": review["_id"]}, {"$set": {"status": "DISQUALIFIED"}})
    return qualified


def _lookup_amazon_global_sync(candidate_name: str, brand: str | None) -> dict | None:
    """Google 검색 그라운딩으로 Amazon Global(amazon.com)에서 이 상품의 공식 리스팅을 찾아
    이름/브랜드/카테고리를 함께 가져온다.

    찾으면 {"name": ..., "brand": ..., "category": ...}를 반환해, 리뷰 원문 대신 Amazon
    공식 정보로 세 필드를 전부 통일한다. 찾지 못하면 None을 반환하며, 호출부
    (_resolve_amazon_names)는 이 경우 해당 리뷰들을 NOTFOUND로 표시해 집계 대상에서 제외한다.
    """
    client = get_client()
    if not client or not genai_types:
        return None

    brand_hint = f"Brand: {brand}" if brand else ""
    categories_list = "\n".join(f"- {c}" for c in ALLOWED_CATEGORIES)
    prompt = (
        f'K-beauty product: "{candidate_name}"\n{brand_hint}\n\n'
        "The product name above may contain Korean text or a mix of Korean and English "
        "(e.g., \"한글이름 (English Name)\"). Ignore the Korean part and search using the "
        "English name only — Amazon Global is an English-language marketplace, so searching "
        "with Korean text will fail to find the listing.\n\n"
        "Search Amazon Global (amazon.com) for this exact product's official listing.\n"
        "If found, also classify it into EXACTLY ONE of these categories:\n"
        f"{categories_list}\n\n"
        'Return raw JSON only: {"found": boolean, "amazon_title": string or null, '
        '"brand": string or null, "category": string or null}\n'
        "- found: true only if a specific matching product listing was found (not a search results page)\n"
        "- amazon_title: the exact product title as shown on the Amazon listing\n"
        "- brand: the brand name as shown on the Amazon listing\n"
        "- category: best matching category from the list above, or null if none fit\n"
    )
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())],
                temperature=0.1,
            ),
        )
        parsed = parse_json_response(response.text or "")
        if isinstance(parsed, dict) and parsed.get("found") and parsed.get("amazon_title"):
            category = parsed.get("category")
            return {
                "name": str(parsed["amazon_title"]).strip(),
                "brand": str(parsed["brand"]).strip() if parsed.get("brand") else None,
                "category": category if category in ALLOWED_CATEGORIES else None,
            }
    except Exception as exc:
        logger.warning(f"[STEP4] Amazon Global 검색 실패 ({candidate_name!r}): {exc}")
    return None


async def _resolve_amazon_names(qualified_reviews: list[dict]) -> list[dict]:
    """품질 통과 리뷰를 원문 (product_name, brand) 표기 기준으로 묶어, 고유 조합당 Amazon 검색을
    1번만 수행하고 결과를 그 조합에 속한 모든 리뷰에 반영한다.

    찾으면: product_name/brand/category를 Amazon 기준으로 정규화하고 status=COMPLETED.
    못 찾으면: status=NOTFOUND (집계에서 제외, 재검색 안 함).

    반환값은 이번 실행에서 COMPLETED로 처리된 리뷰들(정규화된 필드 반영 완료)이다.
    aggregate_by_product가 reviews를 다시 조회하지 않고 이 리스트를 바로 쓸 수 있게 한다.
    """
    col = _get_reviews_collection()
    completed_reviews = []

    groups: dict[tuple[str, str], list[dict]] = {}
    for review in qualified_reviews:
        raw_key = (_normalize_field(review["product_name"]), _normalize_field(review.get("brand") or ""))
        groups.setdefault(raw_key, []).append(review)

    for group_reviews in groups.values():
        sample = group_reviews[0]
        amazon_result = await asyncio.to_thread(
            _lookup_amazon_global_sync, sample["product_name"], sample.get("brand")
        )

        if not amazon_result:
            for review in group_reviews:
                await col.update_one({"_id": review["_id"]}, {"$set": {"status": "NOTFOUND"}})
            logger.info(f"[STEP4][NOTFOUND] Amazon Global에 없음: {sample['product_name']!r} ({len(group_reviews)}개 리뷰)")
            continue

        resolved_name = _normalize_field(amazon_result["name"])
        resolved_brand = _normalize_field(amazon_result["brand"]) if amazon_result["brand"] else None
        resolved_category = _normalize_field(amazon_result["category"]) if amazon_result["category"] else None

        for review in group_reviews:
            try:
                await col.update_one(
                    {"_id": review["_id"]},
                    {"$set": {
                        "product_name": resolved_name,
                        "brand": resolved_brand,
                        "category": resolved_category,
                        "status": "COMPLETED",
                    }},
                )
            except DuplicateKeyError:
                # 같은 영상 안에서 원문 표기가 달랐던 리뷰가(예: 한글/영어 표기) Amazon 정규화 후
                # 같은 product_name으로 밝혀진 경우. (video_id, product_name) 유니크 인덱스와
                # 충돌하므로, 이미 COMPLETED된 다른 리뷰와 같은 상품을 중복 언급한 것으로 보고
                # 별도 종결 상태로 표시한다 (집계에는 포함하지 않음, 재처리도 안 함).
                await col.update_one({"_id": review["_id"]}, {"$set": {"status": "DUPLICATE"}})
                logger.info(
                    f"[STEP4][DUPLICATE] 같은 영상 내 중복 언급으로 제외: {resolved_name!r} "
                    f"(video_id={review['video_id']!r})"
                )
                continue

            completed_reviews.append({
                **review,
                "product_name": resolved_name,
                "brand": resolved_brand,
                "category": resolved_category,
            })
        logger.info(f"[STEP4][COMPLETED] {resolved_name!r} 확인됨 ({len(group_reviews)}개 리뷰)")

    return completed_reviews


def aggregate_by_product(completed_reviews: list[dict]) -> list[dict]:
    """이번 실행에서 새로 COMPLETED된(=Amazon 기준으로 정규화된) 리뷰만 product_name 기준으로
    집계한다. 여기서 나온 mention_count/ad_likelihood_sum은 "이번 배치분"이며, 기존 누적치와의
    합산은 _save_products가 $inc로 처리한다 (reviews 전체를 다시 긁어와 재계산하지 않는다).
    """
    groups: dict[str, dict] = {}

    for review in completed_reviews:
        name = review["product_name"]
        if name not in groups:
            groups[name] = {
                "brand": review.get("brand"),
                "category": review.get("category"),
                "mention_count": 0,
                "ad_likelihood_sum": 0.0,
            }
        group = groups[name]
        group["mention_count"] += 1
        group["ad_likelihood_sum"] += review.get("ad_likelihood", 0.0)

    results = []
    for name, group in groups.items():
        results.append({
            "name": name,
            "brand": group["brand"],
            "category": group["category"],
            "mention_count": group["mention_count"],
            "ad_likelihood_sum": round(group["ad_likelihood_sum"], 3),
        })

    results.sort(key=lambda p: p["mention_count"], reverse=True)
    return results


async def _save_products(products: list[dict]) -> None:
    """이번 배치분 mention_count/ad_likelihood_sum을 기존 누적치에 $inc로 더하고,
    그 결과로 ad_ratio(누적 평균)를 다시 계산해서 저장한다.

    한 상품 저장에 실패해도(DB 오류 등) 나머지 상품 저장까지 막히지 않도록 개별적으로 예외를
    잡는다. 실패한 상품의 원본 reviews는 이미 status=COMPLETED로 확정된 뒤라 재집계 대상에서
    빠지므로, 여기서 전체를 멈추면 그 상품만 영구히 products에 반영 안 되는 고아 데이터가 된다.
    """
    col = _get_products_collection()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    saved = 0
    for product in products:
        try:
            # 이미 존재하는 상품이면 ingredients/inciapi_raw_data/status(성분추출 진행상태)는 건드리지 않는다.
            updated = await col.find_one_and_update(
                {"name": product["name"]},
                {
                    "$inc": {
                        "mention_count": product["mention_count"],
                        "ad_likelihood_sum": product["ad_likelihood_sum"],
                    },
                    "$set": {"brand": product["brand"], "category": product["category"], "analyzed_at": now},
                    "$setOnInsert": {"ingredients": None, "inciapi_raw_data": None, "status": "PENDING"},
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
            ad_ratio = round(updated["ad_likelihood_sum"] / updated["mention_count"], 3) if updated["mention_count"] else 0.0
            await col.update_one({"_id": updated["_id"]}, {"$set": {"ad_ratio": ad_ratio}})
            saved += 1
        except Exception as exc:
            logger.error(f"[STEP4][SAVE_FAIL] {product['name']!r} 저장 실패, 다음 상품으로 계속: {exc}")

    logger.info(f"[STEP4] {saved}/{len(products)}개 상품에 이번 배치분을 누적 저장했습니다.")


async def run() -> list[dict]:
    await ensure_indexes()

    reviews_col = _get_reviews_collection()

    pending_reviews = [doc async for doc in reviews_col.find({"status": "PENDING"})]
    if not pending_reviews:
        logger.info("[STEP4] 처리할 PENDING 리뷰가 없습니다.")
        return []

    qualified_reviews = await _disqualify_low_quality_reviews(pending_reviews)
    logger.info(
        f"[STEP4] PENDING {len(pending_reviews)}개 중 품질 미달 "
        f"{len(pending_reviews) - len(qualified_reviews)}개 제외, {len(qualified_reviews)}개 Amazon 검증 대상."
    )

    completed_reviews = await _resolve_amazon_names(qualified_reviews)
    if not completed_reviews:
        logger.warning("[STEP4] 이번 배치에서 COMPLETED된 리뷰가 없습니다 (전부 NOTFOUND).")
        return []

    products = aggregate_by_product(completed_reviews)
    await _save_products(products)
    return products


if __name__ == "__main__":
    asyncio.run(run())
