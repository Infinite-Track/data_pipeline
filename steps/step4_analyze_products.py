"""
STEP 4 · 리뷰 분석 (리뷰 → 상품)

reviews 컬렉션(STEP3에서 status=PENDING으로 저장된 제품별 리뷰)을 처리해 상품 단위로 집계한
products 컬렉션을 만든다. 상품의 정체성(identity)은 Amazon ASIN이다.

    1) 품질 판정: specificity·relevance·skin_info 3축이 모두 QUALITY_MIN_SCORE 이상이 아니면
       DISQUALIFIED로 표시하고 제외한다.
    2) ASIN 해결(resolution): 품질 통과 리뷰를 원문 (product_name, brand)로 묶어, 고유 조합당
       아래 순서로 asin을 정한다 (Amazon Creators API는 더 이상 사용 불가 → Serper 구글검색, Amazon 무요청).
       a. 같은 원문 표기가 예전에 해결된 적 있으면(reviews 캐시) 그 asin 재사용.
       b. 이름이 채워진 기존 products 목록에서 Gemini가 "동일 상품"을 찾으면 그 asin 재사용
          (변형 ASIN으로 같은 상품이 갈라지는 것을 줄이기 위한 카탈로그 우선 매칭).
       c. 구글 검색 결과의 Amazon 리스팅 후보를 Gemini가 판정(asin_resolver.resolve). 동일 상품 없으면
          NOTFOUND, 한국 브랜드가 아니면(is_korean=False) 그 그룹 리뷰 삭제. 검색 API 실패/상한
          (SearchUnavailable)이면 그 뒤로는 검색만 건너뛰고(a·b 경로는 계속), 검색이 필요한 그룹은
          PENDING으로 두어 다음 실행에 재시도한다.
       그룹 간에는 순서 의존이 없어 CONCURRENCY개씩 병렬로 처리한다(저장 구간만 락으로 직렬화).
    3) 저장(products, asin 기준): 각 그룹을 해결하는 즉시 그 asin의 상품을 upsert한다.
       mention_count/ad_ratio는 "그 asin의 COMPLETED 리뷰"에서 video_id 중복제거 후 재계산하므로
       멱등하다 — 중간에 크래시/재시작해도 유실이나 중복 없이 이어서 완성된다.
       신규 asin은 제휴링크(asin + 파트너 태그)만 저장하고 status=NEED_PRODUCT_INFO로 시작한다.
       name/brand/category/price/description/thumbnail_url은 어드민이 입력하고 PENDING으로 바꾸면
       step5부터 이어진다. (ASIN만 자동 확보한다 — 이미지는 약관상 긁지 않음.)

reviews.status 값: PENDING → DISQUALIFIED / NOTFOUND / COMPLETED (모두 최종 상태)
"""
import asyncio
import datetime
import re

from steps.asin_resolver import SearchUnavailable, resolve
from steps.db import get_collection
from steps.gemini_client import get_client
from steps.llm_utils import parse_json_response
from steps import postgres_publish as pg
from steps import youtuber_reviews
from loguru import logger

# quality_passed 판정 기준: 3축 점수(1~5) 모두 이 값 이상이어야 통과. (튜닝 파라미터)
QUALITY_MIN_SCORE = 2

GEMINI_MODEL = "gemini-2.5-flash"
MAX_CATALOG_CANDIDATES = 400  # 카탈로그 우선 매칭 프롬프트에 넣을 기존 상품 수 상한
MAX_JUDGE_RETRY = 3

# 동시에 처리할 (product_name, brand) 그룹 수. 그룹당 Gemini 1~2회 + Serper 2회의 네트워크 대기가
# 겹쳐 처리량이 크게 오른다. 429(rate limit)가 잦으면 낮추면 된다.
CONCURRENCY = 8


def _get_reviews_collection():
    return get_collection("reviews")


def _get_products_collection():
    return get_collection("products")


async def ensure_indexes() -> None:
    products_col = _get_products_collection()
    # 상품 정체성은 asin. asin이 있는 문서에 한해 unique.
    await products_col.create_index(
        "asin", unique=True, partialFilterExpression={"asin": {"$exists": True}}
    )
    try:
        await products_col.drop_index("name_1")  # 과거 name unique 인덱스 제거
    except Exception:
        pass

    reviews_col = _get_reviews_collection()
    await reviews_col.create_index("status")
    logger.info("[STEP4] 인덱스 확인 완료 (products.asin unique / reviews.status).")


def _normalize_field(text: str) -> str:
    return re.sub(r"\s+", "", (text or "").strip().lower())


def _raw_key(review: dict) -> tuple[str, str]:
    return (_normalize_field(review.get("product_name")), _normalize_field(review.get("brand")))


async def _disqualify_low_quality() -> int:
    """품질 미달 PENDING 리뷰를 서버측에서 한 번에 DISQUALIFIED로 표시하고, 처리한 개수를 반환한다.
    3축(specificity/relevance/skin_info)이 모두 QUALITY_MIN_SCORE 이상인 경우만 통과($nor로 그 여집합을 제외).
    값이 없는 축도 통과 조건을 못 만족하므로 미달로 간주된다."""
    col = _get_reviews_collection()
    q = QUALITY_MIN_SCORE
    result = await col.update_many(
        {
            "status": "PENDING",
            "$nor": [{"specificity": {"$gte": q}, "relevance": {"$gte": q}, "skin_info": {"$gte": q}}],
        },
        {"$set": {"status": "DISQUALIFIED"}},
    )
    return result.modified_count


async def _build_resolved_cache() -> dict[tuple[str, str], str]:
    """이미 COMPLETED된 리뷰들에서 (원문 name, brand) → asin 캐시를 만든다.
    같은 원문 표기가 다시 들어오면 검색을 재호출하지 않기 위함.
    products에 문서가 없는 asin(어드민이 삭제한 상품 등)은 제외한다 — 캐시로 되살리지 않고
    검색·한국 브랜드 판정을 다시 거치게 해서, 삭제 결정을 파이프라인이 뒤집지 않도록 한다."""
    col = _get_reviews_collection()
    existing = {p["asin"] async for p in _get_products_collection().find({"asin": {"$exists": True}}, {"asin": 1})}
    cache: dict[tuple[str, str], str] = {}
    async for r in col.find({"status": "COMPLETED", "asin": {"$exists": True}},
                            {"product_name": 1, "brand": 1, "asin": 1}):
        if r["asin"] in existing:
            cache.setdefault(_raw_key(r), r["asin"])
    return cache


async def _load_catalog() -> list[dict]:
    """카탈로그 우선 매칭 후보: 이름이 채워진 기존 상품(asin/name/brand). 어드민이 아직 이름을
    안 채운 NEED_PRODUCT_INFO 상품은 이름이 없어 후보에서 빠진다(원문 캐시로만 중복이 잡힘)."""
    col = _get_products_collection()
    return [
        doc async for doc in col.find(
            {"asin": {"$exists": True}, "name": {"$nin": [None, ""]}},
            {"_id": 0, "asin": 1, "name": 1, "brand": 1},
        ).sort("analyzed_at", -1).limit(MAX_CATALOG_CANDIDATES)
    ]


def _match_existing_sync(product_name: str, brand: str | None, catalog: list[dict]) -> str | None:
    """기존 상품 목록에서 product_name과 동일한 상품을 Gemini가 고른다. 없으면(-1/실패) None.
    brand는 step3가 리뷰에서 뽑은 브랜드(없을 수 있음) — 단어 겹침만으로 엮이는 오판을 줄이는 힌트."""
    client = get_client()
    if not client or not catalog:
        return None
    lines = "\n".join(f"[{i}] {p.get('name')} (brand: {p.get('brand') or '?'})" for i, p in enumerate(catalog))
    prompt = (
        "You deduplicate Korean beauty products against an existing catalog.\n\n"
        f'Target product (as mentioned in a YouTube review): "{product_name}"\n'
        f"Target brand (extracted from the review, may be missing): {brand or 'unknown'}\n\n"
        f"Existing catalog:\n{lines}\n\n"
        "Return the index of the catalog entry that is THE SAME product as the target "
        "(same brand AND same item; size/packaging differences are fine).\n"
        "Rules:\n"
        "- Shared words are NOT enough. A catalog entry that merely mentions the target in its title "
        "(e.g. 'alternative to X', 'inspired by X', 'compare to X') is a DIFFERENT product.\n"
        "- If the target is not a specific branded product (a generic term, an ingredient, a treatment or "
        "procedure such as 'Botox', a category like 'sunscreen'), return -1.\n"
        "- If no entry is clearly the same product, return -1.\n"
        'Return raw JSON only: {"best_index": <int>}\n'
    )
    for attempt in range(MAX_JUDGE_RETRY):
        try:
            resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
            parsed = parse_json_response(resp.text or "")
            if isinstance(parsed, dict) and "best_index" in parsed:
                bi = parsed["best_index"]
                if isinstance(bi, int) and 0 <= bi < len(catalog):
                    return catalog[bi]["asin"]
                return None
        except Exception as exc:
            logger.warning(f"[STEP4][Gemini] 카탈로그 매칭 시도 {attempt + 1}/{MAX_JUDGE_RETRY} 실패: {exc}")
    return None


async def _upsert_product(asin: str, enrich: dict | None, now: str) -> None:
    """asin의 COMPLETED 리뷰에서 (video_id 중복제거) 카운트를 재계산해 products에 upsert한다.
    재계산 기반이라 멱등 — 같은 asin을 여러 번 저장해도 결과가 같다.
    신규 asin은 제휴링크까지 저장하고, 기존 asin은 카운트만 갱신한다."""
    reviews_col = _get_reviews_collection()
    products_col = _get_products_collection()

    per_video: dict[str, float] = {}
    async for r in reviews_col.find({"asin": asin, "status": "COMPLETED"},
                                    {"video_id": 1, "ad_likelihood": 1}):
        per_video[r.get("video_id")] = r.get("ad_likelihood") or 0.0  # 영상당 1회
    mention_count = len(per_video)
    ad_sum = round(sum(per_video.values()), 3)
    ad_ratio = round(ad_sum / mention_count, 3) if mention_count else 0.0
    base = {"mention_count": mention_count, "ad_likelihood_sum": ad_sum,
            "ad_ratio": ad_ratio, "analyzed_at": now}

    existing = await products_col.find_one({"asin": asin}, {"_id": 1})
    if existing is None:
        if not enrich:
            logger.error(f"[STEP4] 신규 asin={asin} enrich 없음 — 상품 생성 건너뜀")
            return
        await products_col.update_one(
            {"asin": asin},
            {
                "$set": {
                    **base,
                    # name/brand/category/price/description/thumbnail_url은 어드민이 입력한다(여기서는 넣지 않음).
                    "product_url": enrich.get("product_url"),  # 제휴링크(asin + 파트너 태그)
                },
                # 어드민이 상품 정보를 채운 뒤 PENDING으로 바꿔야 step5가 집는다.
                # created_at은 최초 생성 시각(ISO, UTC) — 이후 갱신에서 바뀌지 않는다.
                "$setOnInsert": {"ingredients": None, "inciapi_raw_data": None,
                                 "status": "NEED_PRODUCT_INFO", "created_at": now},
            },
            upsert=True,
        )
    else:
        # 기존 상품: 카운트만 갱신 (enrich 재요청 없음).
        await products_col.update_one({"asin": asin}, {"$set": base})


async def _refresh_youtuber_reviews(pg_conn, asin: str) -> None:
    """이미 발행된 상품(products_catalog에 존재)이면, 새로 붙은 리뷰를 youtuber_reviews에
    upsert한다(기존 행 유지, 새 영상만 추가). 아직 catalog가 없는 신규 상품은 step5가 발행하므로 건너뛴다.
    (catalog/url/성분/유튜버리뷰의 최초 발행은 step5 — 성분 크롤링 후.)"""
    if pg_conn is None:
        return
    try:
        product_id = await asyncio.to_thread(pg.get_product_id, pg_conn, asin)
        if product_id is None:
            return
        rows = await youtuber_reviews.gather_rows(asin)
        await asyncio.to_thread(pg.publish_youtuber_reviews, pg_conn, product_id, rows)
    except Exception as exc:
        pg_conn.rollback()
        logger.warning(f"[STEP4][PG] youtuber_reviews 갱신 실패 asin={asin}: {exc}")


async def _resolve_and_save(qualified_reviews: list[dict], catalog: list[dict],
                            cache: dict[tuple[str, str], str], pg_conn=None) -> set[str]:
    """원문 (name, brand) 그룹별로 asin을 해결하고, 해결 즉시 상품을 저장한다(멱등).
    캐시 → 카탈로그 매칭 → Amazon 검색 순. 반환: 이번에 저장/갱신한 asin 집합."""
    reviews_col = _get_reviews_collection()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    groups: dict[tuple[str, str], list[dict]] = {}
    for review in qualified_reviews:
        groups.setdefault(_raw_key(review), []).append(review)

    saved: set[str] = set()
    state = {"search_down": False}  # 검색 API 실패/상한 이후에는 검색만 건너뛰고 캐시·카탈로그 경로는 계속 처리
    sem = asyncio.Semaphore(CONCURRENCY)
    # 저장 구간만 직렬화: 같은 asin의 동시 upsert 경쟁과 psycopg2 연결(스레드 비안전) 공유를 막는다.
    # 느린 부분(Gemini/Serper 네트워크 대기)은 세마포어 폭만큼 병렬로 돈다.
    save_lock = asyncio.Lock()

    async def _process(raw_key: tuple[str, str], group_reviews: list[dict]) -> None:
        async with sem:
            sample = group_reviews[0]

            if raw_key in cache:
                asin, enrich = cache[raw_key], None  # 기존 상품 — 검색 재호출 없음
            elif (matched := await asyncio.to_thread(_match_existing_sync, sample["product_name"], sample.get("brand"), catalog)):
                asin, enrich = matched, None  # 기존 카탈로그와 동일 상품 — 변형 ASIN 분산 방지
                cache[raw_key] = asin
                logger.info(f"[STEP4][CATALOG] 기존 상품 매칭: {sample['product_name']!r} → asin={asin}")
            elif state["search_down"]:
                return  # 검색이 필요한 그룹 — PENDING 유지, 다음 실행에 재시도
            else:
                try:
                    enrich = await asyncio.to_thread(resolve, sample["product_name"], sample.get("brand"))
                except SearchUnavailable as exc:
                    # 일시 실패: 검색이 필요한 그룹은 PENDING으로 남겨 다음 실행에 재시도한다.
                    if not state["search_down"]:
                        logger.warning(f"[STEP4][SEARCH_DOWN] {exc} — 이후 그룹은 캐시/카탈로그 매칭만 시도")
                    state["search_down"] = True
                    return
                if not enrich or not enrich.get("asin"):
                    for review in group_reviews:
                        await reviews_col.update_one({"_id": review["_id"]}, {"$set": {"status": "NOTFOUND"}})
                    logger.info(f"[STEP4][NOTFOUND] Amazon에 없음: {sample['product_name']!r} ({len(group_reviews)}개 리뷰)")
                    return
                # 한국 브랜드만 취급 — 명시적 False면 그 그룹 리뷰를 삭제(None/불명은 유지해 오삭제 방지).
                if enrich.get("is_korean") is False:
                    ids = [review["_id"] for review in group_reviews]
                    await reviews_col.delete_many({"_id": {"$in": ids}})
                    logger.info(f"[STEP4][NOT_KOREAN] 비한국 브랜드 삭제: {sample['product_name']!r} "
                                f"(asin={enrich['asin']}, {len(ids)}개 리뷰)")
                    return
                asin = enrich["asin"]
                cache[raw_key] = asin

            async with save_lock:
                for review in group_reviews:
                    await reviews_col.update_one({"_id": review["_id"]}, {"$set": {"asin": asin, "status": "COMPLETED"}})
                await _upsert_product(asin, enrich, now)  # 해결 즉시 저장 → 크래시/재시작에 안전
                await _refresh_youtuber_reviews(pg_conn, asin)  # 기존 발행 상품이면 유튜버리뷰 갱신
                saved.add(asin)
            logger.info(f"[STEP4][COMPLETED] asin={asin} ({len(group_reviews)}개 리뷰)")

    # 동시 CONCURRENCY개씩 병렬 처리 (세마포어로 상한). 그룹 간에는 순서 의존이 없다.
    await asyncio.gather(*(_process(k, g) for k, g in groups.items()))
    return saved


async def run() -> list[str]:
    await ensure_indexes()

    reviews_col = _get_reviews_collection()
    if await reviews_col.count_documents({"status": "PENDING"}) == 0:
        logger.info("[STEP4] 처리할 PENDING 리뷰가 없습니다.")
        return []

    # ① 쓰기: 품질 미달 리뷰를 서버측에서 한 번에 DISQUALIFIED 처리.
    disqualified = await _disqualify_low_quality()
    # ② 읽기: 남은 PENDING(= 품질 통과)만 가져온다.
    qualified_reviews = [doc async for doc in reviews_col.find({"status": "PENDING"})]
    logger.info(
        f"[STEP4] 품질 미달 {disqualified}개 제외, {len(qualified_reviews)}개 Amazon 해결 대상."
    )
    if not qualified_reviews:
        logger.info("[STEP4] 품질 통과 리뷰가 없습니다.")
        return []

    # Postgres 연결(best-effort — 실패해도 Mongo 파이프라인은 진행).
    # step4는 기존 발행 상품의 youtuber_reviews만 갱신한다(최초 발행은 step5).
    pg_conn = None
    try:
        pg_conn = pg.connect()
    except Exception as exc:
        logger.warning(f"[STEP4][PG] Postgres 연결 실패 — youtuber_reviews 갱신 건너뜀: {exc}")
        pg_conn = None

    cache = await _build_resolved_cache()
    catalog = await _load_catalog()
    try:
        saved = await _resolve_and_save(qualified_reviews, catalog, cache, pg_conn)
    finally:
        if pg_conn is not None:
            pg_conn.close()
    logger.info(f"[STEP4] {len(saved)}개 상품 저장/갱신 완료.")
    return list(saved)


if __name__ == "__main__":
    asyncio.run(run())
