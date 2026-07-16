"""
STEP 1 · 신규 영상 판별

youtubers 컬렉션에 등록된 채널마다 last_checked_at 이후 새로 올라온 영상을 가져온 뒤,
LLM 기반 판별을 거쳐 videos 컬렉션에 저장한다.

    제목 + 설명 + 태그를 Gemini에 전달해 "스킨케어 리뷰 영상인지"를 YES / NO / UNSURE로 판별
    - NO  → 저장하지 않고 스킵 (로그만 남김)
    - YES, UNSURE → videos 컬렉션에 status=PENDING으로 저장 (다음 step2가 소비)

길이 제한(예: 1분 미만 제외) 같은 규칙 기반 필터는 두지 않는다. 쇼츠 여부도 길이만으로는
정확히 판별할 수 없어(YouTube가 쇼츠 최대 길이를 최대 3분까지 허용) 걸러내지 않는다.

마지막으로 채널별 last_checked_at을 이번에 조회한 영상 중 가장 최신 게시일로 갱신한다.

YouTube API 호출 방식 (쿼터 절약):
    신규 영상 조회에 search.list를 쓰지 않는다. search.list는 호출당 100유닛이고,
    2026-06-01부터는 공용 10,000유닛 풀과 별개로 하루 약 100회로 제한된 전용 버킷에서
    차감되도록 바뀌어, 채널 수가 늘어나면 이 방식만으로 하루 쿼터를 다 쓸 수 있다.
    대신 channels.list(part=contentDetails, 1유닛)로 채널의 "uploads" 재생목록 ID를 얻고,
    playlistItems.list(1유닛)로 그 재생목록의 최신 영상을 가져오는 방식을 쓴다.
    채널 1개를 확인하는 데 약 3유닛(channels + playlistItems + videos)만 들어 훨씬 저렴하다.
    단, playlistItems.list는 publishedAfter 같은 서버 측 날짜 필터를 지원하지 않으므로
    응답을 클라이언트에서 직접 비교해서 거른다 (uploads 재생목록은 최신 업로드가 먼저 오므로,
    last_checked_at보다 오래된 영상을 만나는 즉시 조회를 멈춘다).

전제(사용자 확인 필요):
    - youtubers 컬렉션 문서는 { channel_name, url, channel_id?, last_checked_at } 형태로
      이미 수동으로 채워져 있다고 가정한다. channel_id가 없으면 url에서 추출을 시도한다.
    - 한 번에 최대 50개 영상만 조회한다 (playlistItems.list의 maxResults 한도).
      한 채널에서 확인 주기 사이에 50개 넘게 올라오는 경우는 페이지네이션 보강이 필요하다.
"""
import asyncio
import datetime
import os
import re

import requests
from dotenv import load_dotenv
from loguru import logger

from steps.db import get_collection
from steps.gemini_client import get_client
from steps.llm_utils import parse_json_response

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
GEMINI_MODEL = "gemini-2.5-flash"

if not YOUTUBE_API_KEY:
    logger.warning("YOUTUBE_API_KEY가 설정되지 않았습니다. STEP1은 실행되지 않습니다.")


def _to_datetime(value: str) -> datetime.datetime:
    """RFC3339 문자열(YouTube가 주는 'Z' 표기 포함)을 비교 가능한 datetime으로 변환한다.

    문자열끼리 그냥 비교(max() 등)하면 'Z'와 '+00:00' 표기가 섞였을 때 부정확할 수 있어
    실제 datetime으로 파싱해서 비교한다.
    """
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _get_youtubers_collection():
    return get_collection("youtubers")


def _get_videos_collection():
    return get_collection("videos")


async def ensure_indexes() -> None:
    videos_col = _get_videos_collection()
    await videos_col.create_index("video_id", unique=True)
    logger.info("[STEP1] videos.video_id 유니크 인덱스 확인 완료.")


# ── 채널 ID 해석 ──────────────────────────────────────────────────────────
# youtuber 문서의 url이 "/channel/UC..." 형태면 바로 추출하고,
# "@handle" 형태면 YouTube API(forHandle)로 채널 ID를 조회한다.
def _resolve_channel_id(youtuber_doc: dict) -> str | None:
    if youtuber_doc.get("channel_id"):
        return youtuber_doc["channel_id"]

    url = youtuber_doc.get("url", "")
    match = re.search(r"/channel/(UC[\w-]{10,})", url)
    if match:
        return match.group(1)

    handle_match = re.search(r"/@([\w.-]+)", url)
    if handle_match:
        try:
            resp = requests.get(
                f"{YOUTUBE_API_BASE}/channels",
                params={"key": YOUTUBE_API_KEY, "part": "id", "forHandle": f"@{handle_match.group(1)}"},
                timeout=10,
            )
            items = resp.json().get("items", [])
            if items:
                return items[0]["id"]
        except Exception as exc:
            logger.warning(f"[STEP1] 채널 ID 조회 실패 ({url}): {exc}")

    logger.warning(f"[STEP1] 채널 ID를 확인할 수 없습니다: {url}")
    return None


# ── uploads 재생목록 ID 조회 ────────────────────────────────────────────────
def _get_uploads_playlist_id(channel_id: str) -> str | None:
    """channels.list(part=contentDetails, 1유닛)로 채널의 uploads 재생목록 ID를 조회한다.

    이 재생목록에는 채널이 올린 모든 공개 영상이 최신순으로 자동으로 쌓이므로,
    search.list 없이도 "이 채널의 최신 영상 목록"을 얻을 수 있다.
    """
    resp = requests.get(
        f"{YOUTUBE_API_BASE}/channels",
        params={"key": YOUTUBE_API_KEY, "part": "contentDetails", "id": channel_id},
        timeout=10,
    )
    items = resp.json().get("items", [])
    if not items:
        return None
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


# ── 신규 영상 목록 조회 ────────────────────────────────────────────────────
def _fetch_new_videos_sync(playlist_id: str, published_after_dt: datetime.datetime) -> list[dict]:
    """uploads 재생목록(최신순)에서 published_after_dt 이후 영상만 추린 뒤,
    videos.list로 상세 정보(설명/태그)를 채운다.

    playlistItems.list는 publishedAfter 서버 필터가 없어 직접 비교하며,
    최신순 정렬이므로 published_after_dt보다 오래된 영상을 만나면 즉시 순회를 멈춘다.
    """
    playlist_resp = requests.get(
        f"{YOUTUBE_API_BASE}/playlistItems",
        params={
            "key": YOUTUBE_API_KEY,
            "playlistId": playlist_id,
            "part": "snippet",
            "maxResults": 50,
        },
        timeout=15,
    )
    video_ids = []
    for item in playlist_resp.json().get("items", []):
        snippet = item.get("snippet", {})
        published_at = snippet.get("publishedAt")
        if not published_at:
            continue
        if _to_datetime(published_at) <= published_after_dt:
            break  # 최신순 정렬이므로 여기부터는 이미 처리된(오래된) 영상들
        video_ids.append(snippet["resourceId"]["videoId"])

    if not video_ids:
        return []

    details_resp = requests.get(
        f"{YOUTUBE_API_BASE}/videos",
        params={
            "key": YOUTUBE_API_KEY,
            "id": ",".join(video_ids),
            "part": "snippet",
        },
        timeout=15,
    )
    return details_resp.json().get("items", [])


# ── LLM 기반 리뷰 영상 판별 ─────────────────────────────────────────────────
def _classify_review_video_sync(title: str, description: str, tags: list[str]) -> str:
    """제목/설명/태그만 보고 스킨케어 리뷰 영상인지 판별한다. 반환값: YES / NO / UNSURE."""
    client = get_client()
    if not client:
        return "UNSURE"

    prompt = (
        "You are a YouTube content classifier for a skincare review pipeline.\n"
        "Decide whether this video likely reviews or discusses skincare/beauty products.\n\n"
        f"Title: {title}\n"
        f"Description: {description[:1000]}\n"
        f"Tags: {', '.join(tags[:20])}\n\n"
        'Return raw JSON only: {"classification": "YES" | "NO" | "UNSURE"}\n'
        "- YES: clearly a skincare/beauty product review or mention\n"
        "- NO: unrelated content (vlog, gaming, music, unrelated news, etc.)\n"
        "- UNSURE: ambiguous, let a later step decide with full video content\n"
    )
    try:
        response = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
        parsed = parse_json_response(response.text or "")
        classification = (parsed or {}).get("classification", "UNSURE")
        return classification if classification in {"YES", "NO", "UNSURE"} else "UNSURE"
    except Exception as exc:
        logger.warning(f"[STEP1] 분류 실패, UNSURE로 처리: {exc}")
        return "UNSURE"


async def _save_pending_video(channel_id: str, item: dict) -> None:
    videos_col = _get_videos_collection()
    video_id = item["id"]
    snippet = item.get("snippet", {})
    record = {
        "video_id": video_id,
        "youtuber_id": channel_id,
        "title": snippet.get("title", ""),
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "status": "PENDING",
    }
    await videos_col.update_one({"video_id": video_id}, {"$set": record}, upsert=True)


async def _update_last_checked_at(channel_object_id, latest_published_at: str) -> None:
    youtubers_col = _get_youtubers_collection()
    await youtubers_col.update_one(
        {"_id": channel_object_id}, {"$set": {"last_checked_at": latest_published_at}}
    )


async def _save_channel_id(channel_object_id, channel_id: str) -> None:
    """처음 알아낸 channel_id를 youtubers 문서에 캐싱한다.

    다음 실행부터는 _resolve_channel_id가 이 값을 바로 반환해, url 파싱/forHandle API 호출을 반복하지 않는다.
    """
    youtubers_col = _get_youtubers_collection()
    await youtubers_col.update_one({"_id": channel_object_id}, {"$set": {"channel_id": channel_id}})


async def run() -> None:
    if not YOUTUBE_API_KEY:
        logger.error("[STEP1] YOUTUBE_API_KEY 누락으로 스킵합니다.")
        return

    await ensure_indexes()

    youtubers_col = _get_youtubers_collection()
    channels = [doc async for doc in youtubers_col.find({})]
    if not channels:
        logger.warning("[STEP1] youtubers 컬렉션에 등록된 채널이 없습니다.")
        return

    for channel in channels:
        channel_id = await asyncio.to_thread(_resolve_channel_id, channel)
        if not channel_id:
            continue

        if not channel.get("channel_id"):
            await _save_channel_id(channel["_id"], channel_id)
            logger.info(f"[STEP1] {channel.get('channel_name')}: channel_id 캐싱 완료 → {channel_id}")

        playlist_id = await asyncio.to_thread(_get_uploads_playlist_id, channel_id)
        if not playlist_id:
            logger.warning(f"[STEP1] {channel.get('channel_name')}: uploads 재생목록을 찾을 수 없습니다.")
            continue

        # last_checked_at이 없으면 24시간 전부터 조회 (최초 등록 채널 대비)
        published_after = channel.get("last_checked_at") or (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        ).isoformat()
        published_after_dt = _to_datetime(published_after)

        items = await asyncio.to_thread(_fetch_new_videos_sync, playlist_id, published_after_dt)
        if not items:
            logger.info(f"[STEP1] {channel.get('channel_name')}: 신규 영상 없음.")
            continue

        latest_published_at, latest_dt = published_after, published_after_dt
        for item in items:
            snippet = item.get("snippet", {})
            published_at = snippet.get("publishedAt", published_after)
            published_dt = _to_datetime(published_at)
            if published_dt > latest_dt:
                latest_dt, latest_published_at = published_dt, published_at

            # LLM 기반 리뷰 영상 판별
            classification = await asyncio.to_thread(
                _classify_review_video_sync,
                snippet.get("title", ""),
                snippet.get("description", ""),
                snippet.get("tags", []),
            )
            if classification == "NO":
                logger.info(f"[STEP1][SKIP] 리뷰 영상 아님(NO): {snippet.get('title')}")
                continue

            await _save_pending_video(channel_id, item)
            logger.info(f"[STEP1][SAVE] ({classification}) {snippet.get('title')}")

        await _update_last_checked_at(channel["_id"], latest_published_at)
        logger.info(f"[STEP1] {channel.get('channel_name')}: last_checked_at → {latest_published_at}")


if __name__ == "__main__":
    asyncio.run(run())
