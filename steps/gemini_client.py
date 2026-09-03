"""Gemini API 클라이언트 생성 헬퍼.

step1(광고 판별), step2(영상 요약), step3(리뷰 검증), step5(성분 추출)에서 공통으로 사용한다.
"""
import os

from dotenv import load_dotenv
from loguru import logger

try:
    from google import genai
    from google.genai import types as genai_types
except ImportError:
    genai = None
    genai_types = None

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# HTTP 요청 타임아웃(밀리초). 이게 없으면 네트워크가 멎을 때 호출이 무한 대기(hang)해 파이프라인
# 전체가 멈춘다. 타임아웃이 걸리면 예외 → 각 step의 재시도 로직이 처리한다.
GEMINI_TIMEOUT_MS = 90_000


def get_client():
    """Gemini 클라이언트를 생성한다. 패키지 미설치/키 누락 시 None을 반환하고 경고만 남긴다."""
    if not genai:
        logger.warning("google-genai 패키지가 설치되어 있지 않습니다. (pip install google-genai)")
        return None
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY가 설정되지 않았습니다.")
        return None
    return genai.Client(
        api_key=GEMINI_API_KEY,
        http_options=genai_types.HttpOptions(timeout=GEMINI_TIMEOUT_MS),
    )
