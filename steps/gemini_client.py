"""Gemini API 클라이언트 생성 헬퍼.

step1(광고 판별), step2(영상 요약), step3(리뷰 검증), step5(성분 크롤링)에서 공통으로 사용한다.
"""
import os

from dotenv import load_dotenv
from loguru import logger

try:
    from google import genai
except ImportError:
    genai = None

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


def get_client():
    """Gemini 클라이언트를 생성한다. 패키지 미설치/키 누락 시 None을 반환하고 경고만 남긴다."""
    if not genai:
        logger.warning("google-genai 패키지가 설치되어 있지 않습니다. (pip install google-genai)")
        return None
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY가 설정되지 않았습니다.")
        return None
    return genai.Client(api_key=GEMINI_API_KEY)
