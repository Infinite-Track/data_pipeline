import asyncio
import os
import re
import time
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv
from loguru import logger

try:
    from google import genai
    from google.genai import types as genai_types
except ImportError:
    genai = None
    genai_types = None

load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = "gemini-3.5-flash"

_genai_client = None
if API_KEY and genai:
    _genai_client = genai.Client(api_key=API_KEY)
else:
    if not genai:
        logger.warning("google-genai is not installed. Gemini calls will fail.")
    if not API_KEY:
        logger.warning("GEMINI_API_KEY is not set.")


def extract_video_id(url: str) -> str | None:
    parsed = urlparse(url.strip())
    if parsed.hostname in {"www.youtube.com", "youtube.com"}:
        query = parse_qs(parsed.query)
        return query.get("v", [None])[0]
    if parsed.hostname in {"youtu.be", "www.youtu.be"}:
        return parsed.path.lstrip("/")
    return None


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def _create_prompt() -> str:
    return (
        "You are a K-beauty content analyst.\n\n"
        "Watch this video and list ALL skincare or beauty products mentioned. Write one paragraph per product.\n"
        "For each product include:\n"
        "1. Brand name and product name\n"
        "2. Key claims (effects, skin type suitability, key ingredients)\n"
        "3. Any signs of sponsorship or advertisement:\n"
        "   - Explicit: \"gifted by\", \"sponsored\", \"paid partnership\", \"discount code\"\n"
        "   - Implicit: overly positive with no negatives, brand-first language\n\n"
        "Do not skip any product. Write in English.\n"
    )


def _summarize_sync(url: str) -> dict | None:
    video_id = extract_video_id(url)
    if not video_id:
        logger.error(f"[SUMMARIZE] Invalid YouTube URL: {url}")
        return None

    title = ""
    channel_name = ""

    try:
        oembed_url = f"https://www.youtube.com/oembed?url={url}&format=json"
        resp = requests.get(oembed_url, timeout=10)
        if resp.status_code == 200:
            meta = resp.json()
            title = meta.get("title", "")
            channel_name = meta.get("author_name", "")
            logger.info(f"[SUMMARIZE] oEmbed metadata fetched for {video_id}")
        else:
            logger.warning(f"[SUMMARIZE] oEmbed returned {resp.status_code} for {video_id}")
    except Exception as exc:
        logger.warning(f"[SUMMARIZE] oEmbed failed for {video_id}: {exc}")

    if not _genai_client:
        logger.error(f"[SUMMARIZE] Gemini client unavailable for {video_id}")
        return None

    summary = None
    for attempt in range(3):
        model = "gemini-2.5-flash" if attempt == 2 else GEMINI_MODEL
        try:
            logger.info(f"[SUMMARIZE] Sending YouTube URL to Gemini for {video_id} (attempt {attempt + 1}/3, model={model})")
            response = _genai_client.models.generate_content(
                model=model,
                contents=[
                    genai_types.Part(
                        file_data=genai_types.FileData(
                            file_uri=url,
                            mime_type="video/*",
                        )
                    ),
                    genai_types.Part(text=_create_prompt()),
                ],
                config=genai_types.GenerateContentConfig(
                    temperature=0.2,
                ),
            )
            summary = response.text
            if not summary:
                logger.error(f"[SUMMARIZE] Empty response from Gemini for {video_id}")
                return None
            logger.info(f"[SUMMARIZE] Summary generated for {video_id}")
            break
        except Exception as exc:
            wait = 20 * (attempt + 1)
            logger.warning(f"[SUMMARIZE] Attempt {attempt + 1} failed for {video_id}: {exc}. Retrying in {wait}s...")
            time.sleep(wait)

    if not summary:
        logger.error(f"[SUMMARIZE] All attempts failed for {video_id}")
        return None

    return {
        "video_id": video_id,
        "title": title,
        "channel_name": channel_name,
        "summary": _clean_text(summary),
    }


async def summarize(url: str) -> dict | None:
    return await asyncio.to_thread(_summarize_sync, url)

run = summarize
