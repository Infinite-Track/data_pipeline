"""Amazon Creators API 클라이언트 (SearchItems + Gemini 판정 + enrich 추출).

step4(리뷰→상품 해결)에서 사용한다. 동기 함수이므로 async 스텝에서는 asyncio.to_thread로 감싼다.
- 토큰 캐싱 + 1 TPS 스로틀 + 429 지수 백오프
- resolve(): 검색어 → SearchItems 상위 10개 → Gemini가 동일 상품 판정 → enrich dict
  (asin/name/brand/category/price/description/image_url/product_url) 또는 None
"""
import os
import time
from pathlib import Path

import requests
from loguru import logger

from steps.gemini_client import get_client
from steps.llm_utils import parse_json_response

# Amazon K-Beauty 카테고리 7종. Gemini가 이 중 하나로 분류한다.
CATEGORIES = ["Cleansers", "Toners", "Treatments", "Moisturizers", "Sunscreens", "Sets", "Face masks"]

PICTURES_DIR = Path(__file__).resolve().parent.parent / "product_pictures"
THUMBNAIL_DB_PREFIX = "product_pictures"

GEMINI_MODEL = "gemini-2.5-flash"
MIN_INTERVAL = 1.1         # Amazon API 호출 최소 간격(초). 초기 한도 1 TPS 준수
MAX_RETRY = 5              # 429(스로틀) 재시도 횟수
MAX_JUDGE_RETRY = 3

TOKEN_URL = "https://api.amazon.com/auth/o2/token"
API_BASE = "https://creatorsapi.amazon/catalog/v1"
SEARCH_RESOURCES = [
    "itemInfo.title",
    "itemInfo.byLineInfo",
    "itemInfo.features",
    "images.primary.large",
    "offersV2.listings.price",
    "browseNodeInfo.browseNodes",
]


class AmazonClient:
    """토큰 캐싱 + 1 TPS 스로틀 + 429 백오프를 담은 SearchItems 클라이언트."""

    def __init__(self):
        self._token = None
        self._token_exp = 0.0
        self._last_call = 0.0

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_exp - 60:
            return self._token
        resp = requests.post(
            TOKEN_URL,
            json={
                "grant_type": "client_credentials",
                "client_id": os.environ["AMAZON_CREDENTIAL_ID"],
                "client_secret": os.environ["AMAZON_CREDENTIAL_SECRET"],
                "scope": "creatorsapi::default",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_exp = time.time() + data.get("expires_in", 3600)
        return self._token

    def _throttle(self):
        wait = MIN_INTERVAL - (time.time() - self._last_call)
        if wait > 0:
            time.sleep(wait)

    def search_items(self, keywords: str) -> dict:
        """SearchItems 호출 (상위 10개). 429면 지수 백오프 후 재시도."""
        body = {
            "partnerTag": os.environ["AMAZON_PARTNER_TAG"],
            "keywords": keywords,
            "itemCount": 10,
            "resources": SEARCH_RESOURCES,
        }
        for attempt in range(MAX_RETRY):
            self._throttle()
            headers = {
                "Authorization": f"Bearer {self._get_token()}",
                "Content-Type": "application/json",
                "x-marketplace": os.environ["AMAZON_MARKETPLACE"],
            }
            try:
                resp = requests.post(f"{API_BASE}/searchItems", headers=headers, json=body, timeout=20)
            except requests.exceptions.RequestException as e:
                # 네트워크 오류(Connection aborted 등)도 재시도한다. (재시도 없으면 파이프라인이 죽는다)
                backoff = MIN_INTERVAL * (2 ** attempt)
                logger.warning(f"[AMAZON][NET] {e} — {backoff:.1f}s 후 재시도 ({attempt + 1}/{MAX_RETRY})")
                time.sleep(backoff)
                continue
            self._last_call = time.time()
            if resp.status_code == 429:
                backoff = MIN_INTERVAL * (2 ** attempt)
                logger.warning(f"[AMAZON][429] 스로틀. {backoff:.1f}s 대기 후 재시도 ({attempt + 1}/{MAX_RETRY})")
                time.sleep(backoff)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"429 재시도 초과: {keywords!r}")


# ── Gemini 판정 (상위 후보 중 동일상품 index + 7종 카테고리) ────────────────
def _candidate_line(i: int, item: dict) -> str:
    info = item.get("itemInfo") or {}
    title = (info.get("title") or {}).get("displayValue") or ""
    brand = ((info.get("byLineInfo") or {}).get("brand") or {}).get("displayValue") or ""
    nodes = [(n.get("displayName") or "") for n in ((item.get("browseNodeInfo") or {}).get("browseNodes") or [])][:3]
    return f"[{i}] {title} (brand: {brand}; amazon_nodes: {', '.join(nodes)})"


def judge_best_match(query: str, items: list[dict]) -> dict | None:
    """후보 중 query와 동일한 상품 index + 카테고리를 Gemini로 판정. 실패 시 None."""
    client = get_client()
    if not client:
        return None
    prompt = (
        "You match a Korean beauty product to Amazon search results.\n\n"
        f'Target product: "{query}"\n\n'
        "Candidates:\n" + "\n".join(_candidate_line(i, it) for i, it in enumerate(items)) + "\n\n"
        "Tasks:\n"
        "1. best_index: index of the candidate that is THE SAME product as the target "
        "(same brand AND same item — not merely a similar or related product). "
        "If none is clearly the same product, return -1.\n"
        f"2. category: if best_index >= 0, classify it into EXACTLY ONE of: {', '.join(CATEGORIES)}. "
        "Otherwise null.\n\n"
        'Return raw JSON only: {"best_index": <int>, "category": "<one of the list>" or null}\n'
    )
    for attempt in range(MAX_JUDGE_RETRY):
        try:
            resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
            parsed = parse_json_response(resp.text or "")
            if isinstance(parsed, dict) and "best_index" in parsed:
                return parsed
        except Exception as exc:
            logger.warning(f"[AMAZON][Gemini] 판정 시도 {attempt + 1}/{MAX_JUDGE_RETRY} 실패: {exc}")
    return None


# ── 필드 추출 ───────────────────────────────────────────────────────────
def _extract_price(item: dict):
    listings = ((item.get("offersV2") or {}).get("listings")) or []
    if not listings:
        return None
    money = (listings[0].get("price") or {}).get("money") or {}
    amount = money.get("amount")
    return float(amount) if isinstance(amount, (int, float)) else None


def extract_fields(item: dict) -> dict:
    info = item.get("itemInfo") or {}
    byline = info.get("byLineInfo") or {}
    brand = (byline.get("brand") or {}).get("displayValue") or (byline.get("manufacturer") or {}).get("displayValue")
    features = (info.get("features") or {}).get("displayValues") or []
    image = (((item.get("images") or {}).get("primary") or {}).get("large") or {}).get("url")
    return {
        "asin": item.get("asin"),
        "name": (info.get("title") or {}).get("displayValue"),
        "brand": brand,
        "price": _extract_price(item),
        "description": "\n".join(features) or None,
        "image_url": image,
        "product_url": item.get("detailPageURL"),  # 제휴링크
    }


def download_image(url: str, asin: str) -> str | None:
    """썸네일을 product_pictures/{ASIN}.jpg 로 저장하고 DB용 상대경로를 반환."""
    if not url or not asin:
        return None
    PICTURES_DIR.mkdir(exist_ok=True)
    dest = PICTURES_DIR / f"{asin}.jpg"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        dest.write_bytes(r.content)
        return f"{THUMBNAIL_DB_PREFIX}/{asin}.jpg"
    except Exception as e:
        logger.warning(f"[AMAZON] 이미지 다운로드 실패 {asin}: {e!r}")
        return None


def resolve(client: AmazonClient, query: str) -> dict | None:
    """검색어 → SearchItems → Gemini 판정 → enrich dict. 동일 상품 없거나 결과 없으면 None."""
    result = client.search_items(query)
    items = (result.get("searchResult") or {}).get("items") or []
    if not items:
        return None
    judge = judge_best_match(query, items)
    if not judge:
        return None
    bi = judge.get("best_index")
    if not isinstance(bi, int) or bi < 0 or bi >= len(items):
        return None
    fields = extract_fields(items[bi])
    fields["category"] = judge.get("category") if judge.get("category") in CATEGORIES else None
    return fields
