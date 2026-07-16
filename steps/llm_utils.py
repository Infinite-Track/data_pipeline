"""LLM(Gemini) 응답에서 JSON을 안전하게 추출하는 공통 유틸리티."""
import json
import re
from typing import Any


def strip_code_fences(text: str) -> str:
    """```json ... ``` 코드블록으로 감싸진 응답에서 본문만 추출한다."""
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    return match.group(1).strip() if match else text.strip()


def parse_json_response(raw_text: str) -> Any | None:
    """LLM 응답 문자열을 JSON(dict 또는 list)으로 파싱한다.

    모델이 코드블록을 씌우거나 앞뒤에 설명을 덧붙이는 경우가 있어,
    1) 그대로 파싱 → 2) 가장 바깥 {} → 3) 가장 바깥 [] 순서로 재시도한다.
    모두 실패하면 None을 반환하며, 호출부에서 재시도 여부를 결정한다.
    """
    cleaned = strip_code_fences(raw_text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    for open_ch, close_ch in (("{", "}"), ("[", "]")):
        start, end = cleaned.find(open_ch), cleaned.rfind(close_ch)
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(cleaned[start:end + 1])
            except json.JSONDecodeError:
                continue
    return None
