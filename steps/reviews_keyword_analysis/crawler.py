"""화해(hwahae.co.kr) 크롤링 + 제품 매칭 전담.

- fetch_search: 한국어 제품명으로 화해 검색 → 후보 목록(__NEXT_DATA__).
- judge_hwahae_match / _score: 후보 중 동일 상품 판정 (LLM 우선, 토큰점수 폴백).
- resolve_hwahae_id: name_kr → 화해 product id.
- fetch_reviews / review_text: 리뷰 API 크롤링 + 분석 텍스트 추출.

상세페이지는 AWS WAF로 막히지만 검색 페이지와 리뷰 API(gateway.hwahae.co.kr)는
익명 헤더로 접근된다(헤드리스 브라우저 불필요). 제품 매핑 파일 없이 매번 검색으로 매칭한다.
"""
import json
import re

import requests
from loguru import logger

from steps.gemini_client import get_client
from steps.llm_utils import parse_json_response

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# 화해 리뷰 API가 요구하는 익명 헤더(로그인 없이 통과).
GATEWAY_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json",
    "Accept-Language": "ko-KR",
    "authorization": "Bearer",
    "hwahae-user-id": "anonymous",
    "hwahae-device-id": "anonymous",
    "referer": "https://www.hwahae.co.kr/",
}

GEMINI_MODEL = "gemini-2.5-flash"
MAX_JUDGE_RETRY = 3        # 화해 매칭 Gemini 판정 재시도
JUDGE_MAX_CANDIDATES = 20  # 프롬프트에 넣을 후보 수(리뷰수 상위)
REVIEW_LIMIT = 500         # 제품당 최대 수집 리뷰 수 (0=전량)


def fetch_search(query: str) -> list[dict]:
    """화해 제품검색 페이지의 __NEXT_DATA__(Next.js)에서 후보 제품 목록을 뽑는다.
    상세페이지는 WAF로 막히지만 검색 페이지는 평범한 GET으로 접근된다."""
    r = requests.get(
        "https://www.hwahae.co.kr/search",
        params={"q": query, "type": "products"},
        headers={"User-Agent": UA, "Accept-Language": "ko-KR"}, timeout=20,
    )
    r.raise_for_status()
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
    if not m:
        return []
    data = json.loads(m.group(1))
    return data.get("props", {}).get("pageProps", {}).get("products", {}).get("products", [])


def _score(cand: dict, expect_tokens: list[str], brand: str | None) -> float:
    """후보가 정답에 얼마나 가까운지 점수화(높을수록 좋음). LLM 판정 실패 시 폴백용.
    expect_tokens는 name_kr을 공백으로 자른 토큰, brand는 영문/한글 브랜드(부분일치 가산)."""
    name = (cand.get("productName") or "").replace(" ", "")
    hit = sum(1 for tok in expect_tokens if tok and tok.replace(" ", "") in name)
    s = float(hit)
    if brand and brand.lower() in (cand.get("brand") or "").lower():
        s += 1.5   # 화해 brand는 "아누아 (Anua)" 형태라 영문 브랜드도 부분일치됨
    if expect_tokens and hit == len(expect_tokens):
        s -= 0.02 * len(name)          # 다 맞췄으면 군더더기 없는(짧은) 본품 우선
    if not cand.get("reviewCount"):
        s -= 0.5                        # 리뷰 없는 항목 후순위
    return s


def judge_hwahae_match(name_kr: str, cands: list[dict]) -> int | None:
    """화해 검색 후보 중 name_kr과 동일 상품의 화해 product id를 Gemini로 판정.
    같은 게 없으면(-1)/판정 실패면 None. step4의 amazon judge_best_match와 동일 패턴."""
    client = get_client()
    if not client or not cands:
        return None
    ranked = sorted(cands, key=lambda c: c.get("reviewCount") or 0, reverse=True)[:JUDGE_MAX_CANDIDATES]
    lines = "\n".join(
        f'[{i}] {c.get("productName","")} (브랜드: {c.get("brand","")}, 리뷰 {c.get("reviewCount")})'
        for i, c in enumerate(ranked)
    )
    prompt = (
        "한국 화장품을 화해(Hwahae) 검색결과와 매칭한다.\n\n"
        f'타겟 제품: "{name_kr}"\n\n'
        f"후보:\n{lines}\n\n"
        "타겟과 THE SAME 제품(같은 브랜드 AND 같은 품목)인 후보의 index를 고른다. "
        "기획세트/리필/다른 용량은 타겟이 그걸 명시하지 않는 한 제외한다. "
        "확실히 같은 제품이 없으면 -1.\n"
        'raw JSON만: {"best_index": <int>}'
    )
    for attempt in range(MAX_JUDGE_RETRY):
        try:
            resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
            parsed = parse_json_response(resp.text or "")
            if isinstance(parsed, dict) and "best_index" in parsed:
                bi = parsed["best_index"]
                if isinstance(bi, int) and 0 <= bi < len(ranked):
                    return ranked[bi].get("id")
                return None   # -1 또는 범위 밖 → 매칭 없음
        except Exception as exc:
            logger.warning(f"[STEP7][Gemini] 매칭 판정 시도 {attempt + 1}/{MAX_JUDGE_RETRY} 실패: {exc}")
    return None


def _norm_name(s: str | None) -> str:
    return re.sub(r"\s+", "", (s or "")).lower()


def _review_api_total(hwahae_product_id: int) -> int:
    """리뷰 API가 실제로 보유한 리뷰 수(totalCount). 검색 reviewCount와 다를 수 있다:
    제품이 리뉴얼되면 새 id가 생겨 검색엔 누적 리뷰수가 보여도 새 id의 API엔 신규 몇 건만 있다."""
    try:
        base = f"https://gateway.hwahae.co.kr/v14/products/{hwahae_product_id}/reviews"
        r = requests.get(base, headers=GATEWAY_HEADERS, params={"limit": 1}, timeout=20)
        r.raise_for_status()
        return r.json().get("productMetaData", {}).get("totalCount", 0) or 0
    except requests.exceptions.RequestException:
        return 0


def _pick_best_duplicate(chosen: dict, cands: list[dict]) -> dict:
    """리뉴얼 중복 등록 보정: chosen과 같은 브랜드·같은 상품명 후보가 여럿이면
    리뷰 API totalCount가 가장 많은 후보를 고른다(검색 reviewCount는 리뉴얼 시 부정확).
    중복이 없으면 chosen 그대로 반환(추가 API 호출 없음)."""
    if not chosen:
        return chosen
    siblings = [c for c in cands
                if _norm_name(c.get("productName")) == _norm_name(chosen.get("productName"))
                and (c.get("brand") or "") == (chosen.get("brand") or "")]
    if len(siblings) <= 1:
        return chosen
    best = max(siblings, key=lambda c: _review_api_total(c.get("id")))
    if best.get("id") != chosen.get("id"):
        logger.info(f"[STEP7][DEDUP] 리뉴얼 중복 보정: id={chosen.get('id')}"
                    f"(검색 {chosen.get('reviewCount')}) → id={best.get('id')} (리뷰API 최다)")
    return best


def resolve_hwahae_id(name_kr: str, brand: str | None = None, min_score: float = 2.0) -> int | None:
    """name_kr로 화해를 검색해 동일 상품의 product id를 고른다. 신뢰 매칭 없으면 None.
    1순위 Gemini 판정(judge_hwahae_match), 실패/미설정 시 2순위 토큰 점수(_score) 폴백."""
    if not name_kr:
        return None
    cands = fetch_search(name_kr)
    if not cands:
        return None

    # ① LLM 판정 우선.
    hid = judge_hwahae_match(name_kr, cands)
    if hid is not None:
        matched = next((c for c in cands if c.get("id") == hid), {})
        matched = _pick_best_duplicate(matched, cands)   # 리뉴얼 중복 등록 보정
        logger.info(f"[STEP7][MATCH-LLM] {name_kr!r} → {matched.get('productName')!r} "
                    f"(리뷰 {matched.get('reviewCount')})")
        return matched.get("id")

    # ② 폴백: 토큰 부분일치 점수.
    expect = [t for t in name_kr.split() if t]
    best = max(cands, key=lambda c: _score(c, expect, brand))
    if _score(best, expect, brand) < min_score:
        logger.info(f"[STEP7][MATCH?] 신뢰 매칭 없음: {name_kr!r} (best={best.get('productName')!r})")
        return None
    best = _pick_best_duplicate(best, cands)             # 리뉴얼 중복 등록 보정
    logger.info(f"[STEP7][MATCH-SCORE] {name_kr!r} → {best.get('productName')!r} "
                f"(리뷰 {best.get('reviewCount')})")
    return best.get("id")


def fetch_reviews(hwahae_product_id: int, limit: int = REVIEW_LIMIT) -> list[dict]:
    """화해 리뷰 API 호출. limit=0이면 전량(totalCount). 각 리뷰의 goodText/badText/additionText."""
    base = f"https://gateway.hwahae.co.kr/v14/products/{hwahae_product_id}/reviews"
    head = requests.get(base, headers=GATEWAY_HEADERS, params={"limit": 1}, timeout=20)
    head.raise_for_status()
    total = head.json().get("productMetaData", {}).get("totalCount", 0)
    want = total if limit == 0 else min(limit, total)
    r = requests.get(base, headers=GATEWAY_HEADERS, params={"limit": max(want, 1)}, timeout=60)
    r.raise_for_status()
    return r.json().get("reviewList", [])


def review_text(rv: dict) -> str:
    """한 리뷰의 좋은점(goodText)만 분석 단위 텍스트로 사용한다. 추후에 다른 리뷰도 사용"""
    return rv.get("goodText") or ""
