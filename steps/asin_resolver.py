"""리뷰 제품명 → Amazon ASIN 해결 (Serper 구글검색 API + Gemini 동일상품 판정). Amazon에는 요청하지 않는다.

step4(리뷰→상품 해결)에서 사용한다. 동기 함수이므로 async 스텝에서는 asyncio.to_thread로 감싼다.
- search(): Serper(google.serper.dev)로 "제품명 site:amazon.com"과 "제품명 amazon.com" 두 번 검색해
  결과 URL에서 ASIN을 뽑은 후보 [{asin, title}]를 합친다(제품당 2크레딧).
  구글은 Amazon 상품을 /dp/ASIN 외에 /clp/ASIN 경로로도 색인하므로 둘 다 인식한다(실측: clp 포함 시 재현율 1/10→6/10).
- judge_best_match(): 후보 중 동일 상품 index + 한국 브랜드 여부를 Gemini로 판정
- resolve(): 검색어 → 후보 → 판정 → {asin, product_url, is_korean} 또는 None
  (name/brand/price/description/썸네일은 어드민이 입력한다.)

경위(2026-09): Amazon Creators API 사용 불가 → Amazon 검색 페이지 스크래핑은 Akamai 차단·어소시에이트
약관 위반 → Google Custom Search JSON API는 신규 고객 차단(2027-01 종료) → Gemini 그라운딩/url_context는
Amazon 페이지를 못 가져옴 → 검색 API(Serper)로 확정. 무료 계정은 검색당 결과 10개까지만 허용된다.

Serper 호출이 실패하면(키 누락/크레딧 소진/네트워크) SearchUnavailable을 던져 호출부가 해당 리뷰를
PENDING으로 남기고 다음 실행에 재시도하도록 한다. 실행당 상품 수 상한(MAX_RESOLVES_PER_RUN)은 크레딧 보호용.
"""
import os
import re
import threading
import time

import requests
from loguru import logger

from steps.gemini_client import get_client
from steps.llm_utils import parse_json_response

try:
    from google.genai import types as genai_types
except ImportError:
    genai_types = None

GEMINI_MODEL = "gemini-2.5-flash"
SERPER_URL = "https://google.serper.dev/search"
SERPER_NUM = 10            # 무료 계정 상한(20 이상은 400 "Query pattern not allowed for free accounts")
SERPER_MAX_RETRY = 3       # 429/5xx/네트워크 오류 재시도 횟수(백오프 1s→2s→4s)
MAX_RESOLVES_PER_RUN = 100  # 프로세스당 resolve 상한(제품당 2크레딧, 크레딧 폭주 방지). 초과분은 SearchUnavailable로 다음 실행에 넘긴다.
MAX_CANDIDATES = 10        # Gemini 판정에 넣을 후보 수
MAX_JUDGE_RETRY = 3
REQUEST_TIMEOUT = 20

AMAZON_BASE = "https://www.amazon.com"
# /dp/, /clp/, /gp/product/, /gp/aw/d/ 뒤의 10자리가 ASIN. 앞에 상품명 슬러그가 한 단계 올 수 있다.
ASIN_RE = re.compile(r"amazon\.com/(?:[^/\s]+/)?(?:dp|clp|gp/product|gp/aw/d)/([A-Z0-9]{10})")

_resolve_count = 0
_count_lock = threading.Lock()  # step4가 to_thread로 병렬 호출하므로 카운터 갱신을 보호


class SearchUnavailable(RuntimeError):
    """검색 API 실패 또는 실행당 상한 도달. 일시적 실패로 보고 호출부가 재시도 대상으로 남긴다."""


def _serper(query: str) -> list[dict]:
    """Serper 검색 1회. 429/5xx/네트워크 오류는 짧게 백오프하며 재시도하고(병렬 호출 중 일시 오류 흡수),
    그래도 실패하거나 4xx(키/크레딧 문제)면 SearchUnavailable."""
    key = os.environ.get("SERPER_API_KEY")
    if not key:
        raise SearchUnavailable("SERPER_API_KEY가 설정되지 않았습니다.")
    last = None
    for attempt in range(SERPER_MAX_RETRY):
        try:
            r = requests.post(
                SERPER_URL,
                headers={"X-API-KEY": key, "Content-Type": "application/json"},
                json={"q": query, "gl": "us", "hl": "en", "num": SERPER_NUM},
                timeout=REQUEST_TIMEOUT,
            )
        except requests.exceptions.RequestException as exc:
            last = f"Serper 네트워크 오류: {exc}"
        else:
            if r.status_code == 200:
                return r.json().get("organic", []) or []
            last = f"Serper {r.status_code}: {r.text[:200]}"
            if r.status_code < 500 and r.status_code != 429:
                break  # 키/크레딧/요청 형식 문제 — 재시도해도 같다
        time.sleep(2 ** attempt)
    raise SearchUnavailable(last or "Serper 호출 실패")


# ── 검색 ──────────────────────────────────────────────────────────────
def search(keywords: str) -> list[dict]:
    """두 쿼리 형태의 결과를 합쳐 ASIN이 있는 후보만 반환(asin 중복 제거, 상위 MAX_CANDIDATES)."""
    items, seen = [], set()
    for q in (f"{keywords} site:amazon.com", f"{keywords} amazon.com"):
        for hit in _serper(q):
            m = ASIN_RE.search(hit.get("link", ""))
            if not m or m.group(1) in seen:
                continue
            seen.add(m.group(1))
            title = re.sub(r"^Amazon\.com\s*:\s*", "", hit.get("title", "")).strip()
            items.append({"asin": m.group(1), "title": title})
    return items[:MAX_CANDIDATES]


# ── Gemini 판정 ────────────────────────────────────────────────────────
def judge_best_match(query: str, items: list[dict], brand: str | None = None) -> dict | None:
    """후보 중 query와 동일한 상품 index + 한국 브랜드 여부를 Gemini로 판정. 실패 시 None.
    brand는 리뷰에서 뽑은 브랜드 힌트(없을 수 있음).
    is_korean 정확도를 위해 google_search 그라운딩을 사용한다(인디 브랜드는 이름만으론 판별 어려움)."""
    client = get_client()
    if not client or not items:
        return None
    lines = "\n".join(f"[{i}] {it['title']}" for i, it in enumerate(items))
    prompt = (
        "You match a Korean beauty product to Amazon product listings found via Google.\n\n"
        f'Target product: "{query}"\n'
        f"Target brand (extracted from the review, may be missing): {brand or 'unknown'}\n\n"
        f"Candidates (Amazon listing titles):\n{lines}\n\n"
        "Tasks:\n"
        "1. best_index: index of the candidate that is THE SAME product as the target "
        "(same brand AND same item — not merely a similar or related product). "
        "Shared words are NOT enough: a listing that merely mentions the target ('alternative to X', "
        "'compare to X') is a different product. If the target is not a specific branded product "
        "(a generic term, an ingredient, a treatment/procedure such as 'Botox'), return -1. "
        "If several candidates are the same product, prefer the standard single-unit listing over "
        "multipacks, sets, refills, or travel sizes. If none is clearly the same product, return -1.\n"
        "2. is_korean: if best_index >= 0, true when the matched product's BRAND is a South Korean "
        "brand (company founded / headquartered in South Korea), false when it is a foreign brand. "
        "Judge by the brand's origin, NOT where it is sold. Some Korean indie brands use "
        "foreign-looking names (e.g. FRANZ, Le' Nubele, ASCE+) — search the web to verify when unsure. "
        "If best_index is -1, return null.\n\n"
        'Return raw JSON only: {"best_index": <int>, "is_korean": <true|false|null>}\n'
    )
    config = None
    if genai_types:
        config = genai_types.GenerateContentConfig(
            tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())], temperature=0.1,
        )
    for attempt in range(MAX_JUDGE_RETRY):
        try:
            resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt, config=config)
            parsed = parse_json_response(resp.text or "")
            if isinstance(parsed, dict) and "best_index" in parsed:
                return parsed
        except Exception as exc:
            logger.warning(f"[ASIN][Gemini] 판정 시도 {attempt + 1}/{MAX_JUDGE_RETRY} 실패: {exc}")
    return None


# ── 제휴링크 ──────────────────────────────────────────────────────────
def affiliate_url(asin: str) -> str:
    """제휴 링크. AMAZON_PARTNER_TAG가 없으면 일반 상품 링크."""
    tag = os.environ.get("AMAZON_PARTNER_TAG")
    return f"{AMAZON_BASE}/dp/{asin}" + (f"?tag={tag}" if tag else "")


# ── 해결 ──────────────────────────────────────────────────────────────
def resolve(query: str, brand: str | None = None) -> dict | None:
    """검색어 → Serper 검색 → Gemini 판정 → enrich dict. 동일 상품 없거나 결과 없으면 None.
    반환: asin/product_url/is_korean. 검색 실패/상한 도달 시 SearchUnavailable을 그대로 올린다."""
    global _resolve_count
    with _count_lock:
        if _resolve_count >= MAX_RESOLVES_PER_RUN:
            raise SearchUnavailable(f"실행당 해결 상한({MAX_RESOLVES_PER_RUN}) 도달 — 나머지는 다음 실행에 처리")
        _resolve_count += 1

    items = search(query)
    if not items:
        return None
    judge = judge_best_match(query, items, brand)
    if not judge:
        return None
    bi = judge.get("best_index")
    if not isinstance(bi, int) or bi < 0 or bi >= len(items):
        return None
    asin = items[bi]["asin"]

    return {
        "asin": asin,
        "product_url": affiliate_url(asin),
        "is_korean": judge.get("is_korean"),  # True/False/None(불명) — step4가 비한국 드롭에 사용
    }
