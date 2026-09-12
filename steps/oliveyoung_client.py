"""올리브영 글로벌 상품정보 API 클라이언트.

POST https://global.oliveyoung.com/product/description-info  (body: {"prdtNo": "GA..."})
인증/쿠키 불필요(2026-09 기준). 정책 변경 가능성 있으므로 실패는 방어적으로 삼킨다.

description[] 안의 prdtNotcItemCode로 항목을 찾는다:
  "045" = 전성분(콤마 구분 단일 문자열), "046" = 식약처 기능성 심사 여부.
"""
import re

import requests
from loguru import logger

OY_URL = "https://global.oliveyoung.com/product/description-info"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124 Safari/537.36")

ITEM_INGREDIENTS = "045"
ITEM_FUNCTIONAL = "046"


def fetch_description_info(prdt_no: str) -> list[dict] | None:
    """description 배열을 반환. 실패(비200/에러/빈응답)면 None."""
    headers = {
        "Content-Type": "application/json",
        "user-agent": UA,
        "origin": "https://global.oliveyoung.com",
        "referer": f"https://global.oliveyoung.com/product/detail?prdtNo={prdt_no}",
    }
    try:
        r = requests.post(OY_URL, json={"prdtNo": prdt_no}, headers=headers, timeout=15)
        if r.status_code != 200:
            return None
        return r.json().get("description") or None
    except (requests.exceptions.RequestException, ValueError) as exc:
        logger.warning(f"[OY] {prdt_no} 조회 실패: {exc}")
        return None


def _item(items: list[dict], code: str) -> str | None:
    for it in items:
        if it.get("prdtNotcItemCode") == code:
            return it.get("itemCont")
    return None


def parse_ingredients(items: list[dict]) -> tuple[list[str], bool]:
    """"045" 전성분을 리스트로. 반환 (ingredients, truncated).
    - 숫자,숫자(예: 1,2-Hexanediol)는 보호 후 콤마 분리.
    - "...etc."로 끝나면 요약본(부실 기입)이라 truncated=True."""
    raw = _item(items, ITEM_INGREDIENTS)
    if not raw or not raw.strip():
        return [], False
    truncated = raw.rstrip().rstrip(".").lower().endswith("etc")
    protected = re.sub(r"(?<=\d),(?=\d)", "\x00", raw)
    ingredients = [seg.strip().replace("\x00", ",") for seg in protected.split(",") if seg.strip()]
    return ingredients, truncated


def parse_functional(items: list[dict]) -> str | None:
    """"046" 식약처 기능성 심사 여부 → "Y" / "N" / None.
    기능성 제품은 영어 문장("...functional cosmetics review...")으로 오기도 한다."""
    raw = _item(items, ITEM_FUNCTIONAL)
    if not raw:
        return None
    r = raw.strip()
    if r == "Y" or "functional cosmetics" in r.lower():
        return "Y"
    if r == "N":
        return "N"
    return None


def get_ingredients(prdt_no: str) -> dict | None:
    """prdtNo로 전성분·기능성을 추출. 반환 {ingredients, truncated, functional} 또는 None(조회 실패)."""
    items = fetch_description_info(prdt_no)
    if not items:
        return None
    ingredients, truncated = parse_ingredients(items)
    return {"ingredients": ingredients, "truncated": truncated, "functional": parse_functional(items)}
