import asyncio
import json
import os
import re
from typing import Any

from dotenv import load_dotenv
from loguru import logger

try:
    import google.generativeai as genai
except ImportError:
    genai = None

load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")
if API_KEY and genai:
    genai.configure(api_key=API_KEY)
else:
    if not genai:
        logger.warning("google-generativeai is not installed. Gemini calls will fail.")
    if not API_KEY:
        logger.warning("GEMINI_API_KEY is not set.")

GEMINI_MODEL = "gemini-2.5-flash"
ALLOWED_SKIN_TYPES = {"oily", "dry", "combination", "sensitive", "normal"}

# 허용 카테고리 키워드 (소문자, 부분 일치)
_SKINCARE_KEYWORDS = {
    "toner", "essence", "serum", "ampoule", "moisturizer", "cream", "lotion",
    "sunscreen", "spf", "cleanser", "cleansing", "wash", "foam", "mask",
    "sheet mask", "exfoliant", "scrub", "peel", "mist", "spray", "eye cream",
    "eye", "primer", "oil", "balm", "gel", "emulsion", "treatment", "booster",
    "sleeping", "spot", "patches", "aha", "bha", "retinol", "vitamin c",
    "niacinamide", "hyaluronic", "skincare", "skin care", "face",
}
_NON_SKINCARE_KEYWORDS = {
    "foundation", "cushion", "concealer", "lipstick", "lip gloss", "lip balm",
    "mascara", "eyeliner", "eyeshadow", "blush", "bronzer", "highlighter",
    "contour", "powder", "setting spray", "makeup", "hair", "shampoo",
    "conditioner", "body wash", "body lotion", "body cream", "supplement",
    "vitamin", "collagen drink", "food", "beverage",
}


def _is_skincare(category: str | None, product_name: str) -> bool:
    """카테고리·제품명 키워드 기반으로 스킨케어 여부 판단."""
    text = " ".join(filter(None, [category, product_name])).lower()
    if any(kw in text for kw in _NON_SKINCARE_KEYWORDS):
        return False
    if any(kw in text for kw in _SKINCARE_KEYWORDS):
        return True
    # 키워드 매칭 없으면 통과 (Gemini 판단에 맡김)
    return True


# Gemini 출력에서 코드 블록을 제거하고 순수 JSON 텍스트를 얻습니다.
# 모델이 마크다운 코드 블록을 포함할 수 있어 이를 정리합니다.
def _strip_code_blocks(text: str) -> str:
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if match:
        return match.group(1).strip()
    return text.strip()


# 다양한 값 형태를 boolean으로 정규화합니다.
def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        return lowered in {"true", "yes", "y", "1", "ad", "sponsored"}
    return False


# 숫자 또는 문자열을 0.0~1.0 범위의 float로 변환합니다.
def _normalize_float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, result))


# string 또는 list를 문자열 리스트로 정리합니다.
def _normalize_list_of_strings(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        cleaned = [item.strip() for item in re.split(r"[;,\n]", value) if item.strip()]
        return cleaned
    return []


# Gemini에 전달할 JSON 추출용 프롬프트를 생성합니다.
def _build_prompt(summary_data: dict) -> str:
    return (
        "You are a structured data extractor for K-beauty product reviews.\n\n"
        "Extract ALL skincare products mentioned in the summary. Return a JSON ARRAY where each element represents ONE product.\n"
        "If only one product is mentioned, still return an array with one element.\n"
        "Rules:\n"
        "- Only include face skincare products (toner, essence, serum, cream, moisturizer, cleanser, sunscreen, mask, exfoliant, eye cream, ampoule, mist, oil, etc.)\n"
        "- Exclude makeup (foundation, lipstick, eyeshadow, blush, mascara, etc.)\n"
        "- Exclude haircare (shampoo, conditioner, hair mask, hair oil, etc.)\n"
        "- Exclude bodycare, supplements, food, drinks, and any non-skincare items\n"
        "- No explanation, no markdown code block, no extra text — raw JSON array only\n"
        "- If a field cannot be determined, use null\n"
        "- features must be an array of exactly 1 to 4 strings\n"
        "- All float fields must be between 0.0 and 1.0\n\n"
        "JSON schema:\n"
        "[\n"
        "  {\n"
        "    \"brand\": string or null,\n"
        "    \"product_name\": string or null,\n"
        "    \"features\": [string],\n"
        "    \"is_ad\": boolean,\n"
        "    \"ad_confidence\": float,\n"
        "    \"skin_type_fit\": [string],\n"
        "    \"sensitive_skin\": float,\n"
        "    \"irritant_risk\": float,\n"
        "    \"category\": string or null,\n"
        "    \"etc\": [string]\n"
        "  }\n"
        "]\n\n"
        "Field descriptions:\n"
        "- features: up to 4 key product claims or characteristics\n"
        "- is_ad: true if sponsored/gifted/paid partnership detected\n"
        "- ad_confidence: 0.0 = definitely not ad, 1.0 = definitely ad\n"
        "- skin_type_fit: applicable skin types from [\"oily\", \"dry\", \"combination\", \"sensitive\", \"normal\"]\n"
        "- sensitive_skin: 0.0 = not suitable, 1.0 = very suitable for sensitive skin\n"
        "- irritant_risk: 0.0 = safe, 1.0 = high irritation risk\n"
        "- category: string or null (product category or usage category)\n"
        "- etc: optional list of additional meaningful observations\n\n"
        f"Summary:\n{summary_data.get('summary', '')}\n\n"
        f"Channel name: {summary_data.get('channel_name', '')}\n"
    )


# Gemini가 반환한 JSON을 스키마에 맞게 검증하고 정리합니다.
# 필요한 필드가 누락되거나 형식이 맞지 않으면 None을 반환합니다.
def _validate_extracted(parsed: dict) -> dict | None:
    if not isinstance(parsed, dict):
        return None

    brand = parsed.get("brand")
    if not brand or not str(brand).strip():
        return None

    product_name = parsed.get("product_name")
    if not product_name or not str(product_name).strip():
        return None

    if not _is_skincare(parsed.get("category"), str(product_name)):
        return None

    features = _normalize_list_of_strings(parsed.get("features"))[:4]

    skin_type_fit = [item.lower() for item in _normalize_list_of_strings(parsed.get("skin_type_fit"))]
    skin_type_fit = [item for item in skin_type_fit if item in ALLOWED_SKIN_TYPES]

    extra_etc = parsed.get("etc")
    if isinstance(extra_etc, str):
        extra_etc = [extra_etc.strip()] if extra_etc.strip() else []
    elif isinstance(extra_etc, list):
        extra_etc = [str(item).strip() for item in extra_etc if str(item).strip()]
    else:
        extra_etc = []

    return {
        "brand": parsed.get("brand") or None,
        "product_name": str(product_name).strip(),
        "features": features,
        "is_ad": _normalize_bool(parsed.get("is_ad")),
        "ad_confidence": _normalize_float(parsed.get("ad_confidence")),
        "skin_type_fit": skin_type_fit,
        "sensitive_skin": _normalize_float(parsed.get("sensitive_skin")),
        "irritant_risk": _normalize_float(parsed.get("irritant_risk")),
        "category": parsed.get("category") if parsed.get("category") is not None else None,
        "etc": extra_etc,
    }


# Google Search grounding으로 제품명을 공식 영어 소문자 형식으로 정규화합니다.
def _normalize_product_name_sync(raw_name: str) -> str | None:
    if not raw_name:
        return None
    try:
        search_tool = genai.protos.Tool(
            google_search_retrieval=genai.protos.GoogleSearchRetrieval()
        )
        model = genai.GenerativeModel(GEMINI_MODEL, tools=[search_tool])
        prompt = (
            f'K-beauty product: "{raw_name}"\n'
            "Return ONLY the official English product name in lowercase with spaces. "
            "Example: 'cosrx advanced snail 96 mucin power essence'\n"
            "No explanation, no extra text."
        )
        response = model.generate_content(prompt)
        normalized = response.text.strip().lower()
        return normalized if normalized else raw_name.lower()
    except Exception:
        return raw_name.lower()


# Gemini 출력 문자열에서 JSON 배열 또는 객체를 파싱합니다.
def _parse_json(raw_text: str) -> Any:
    cleaned = _strip_code_blocks(raw_text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    # 배열 우선 시도
    start, end = cleaned.find("["), cleaned.rfind("]")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(cleaned[start:end + 1])
        except json.JSONDecodeError:
            pass
    # 단일 객체 fallback
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(cleaned[start:end + 1])
        except json.JSONDecodeError:
            pass
    logger.error(f"[EXTRACT] JSON parsing failed. Raw response: {raw_text!r}")
    return None


# 요약 데이터를 받아 Gemini로부터 제품 목록을 추출합니다.
def _extract_sync(summary_data: dict) -> list[dict] | None:
    if not genai:
        logger.error("[EXTRACT] Gemini client unavailable.")
        return None

    prompt = _build_prompt(summary_data)
    try:
        model = genai.GenerativeModel(GEMINI_MODEL)
        response = model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                temperature=0.2,
                max_output_tokens=65536,
            ),
        )
    except Exception as exc:
        logger.error(f"[EXTRACT] Gemini request failed: {exc}")
        return None

    raw_text = response.text
    parsed = _parse_json(raw_text)
    if parsed is None:
        logger.error("[EXTRACT] Failed to parse extracted JSON from Gemini output.")
        return None

    # 단일 객체면 배열로 감쌈
    if isinstance(parsed, dict):
        parsed = [parsed]
    if not isinstance(parsed, list):
        logger.error("[EXTRACT] Unexpected JSON type from Gemini output.")
        return None

    results = [_validate_extracted(item) for item in parsed]
    results = [r for r in results if r is not None]

    if not results:
        logger.warning("[EXTRACT] No products extracted (empty or all invalid).")
        return None

    for result in results:
        if result.get("product_name"):
            result["product_name"] = _normalize_product_name_sync(result["product_name"])

    return results


# 비동기 호출용 래퍼로, _extract_sync를 스레드에서 실행합니다.
async def extract(summary_data: dict) -> list[dict] | None:
    return await asyncio.to_thread(_extract_sync, summary_data)

run = extract
