"""유튜버 리뷰(youtuber_reviews) 행 수집 — step5(신규 상품)와 step4(기존 상품 갱신)가 공유.

한 상품(asin)의 COMPLETED 리뷰들을 (video_url, youtuber_name, review_summary) 행으로 만든다.
필요 조인: reviews → videos(video_url·youtuber_id) → youtubers(channel_name),
           reviews → raw_reviews(review_summary, 캐시된 영어 1문장 요약).
"""
from steps.db import get_collection


async def gather_rows(asin: str) -> list[dict]:
    """asin의 COMPLETED 리뷰들을 유튜버 리뷰 행 리스트로 모은다(배치 조회)."""
    reviews = [
        r async for r in get_collection("reviews").find(
            {"asin": asin, "status": "COMPLETED"},
            {"_id": 0, "video_id": 1, "raw_review_id": 1, "ad_likelihood": 1},
        )
    ]
    if not reviews:
        return []

    video_ids = list({r["video_id"] for r in reviews if r.get("video_id")})
    raw_ids = [r["raw_review_id"] for r in reviews if r.get("raw_review_id")]

    videos = {
        v["video_id"]: v
        async for v in get_collection("videos").find(
            {"video_id": {"$in": video_ids}}, {"_id": 0, "video_id": 1, "url": 1, "youtuber_id": 1}
        )
    }
    yt_ids = list({v.get("youtuber_id") for v in videos.values() if v.get("youtuber_id")})
    youtubers = {
        y["channel_id"]: y
        async for y in get_collection("youtubers").find(
            {"channel_id": {"$in": yt_ids}}, {"_id": 0, "channel_id": 1, "channel_name": 1}
        )
    }
    raws = {
        r["_id"]: r
        async for r in get_collection("raw_reviews").find(
            {"_id": {"$in": raw_ids}}, {"review_summary": 1}
        )
    }

    rows = []
    for r in reviews:
        v = videos.get(r.get("video_id")) or {}
        yt = youtubers.get(v.get("youtuber_id")) or {}
        raw = raws.get(r.get("raw_review_id")) or {}
        rows.append({
            "video_url": v.get("url"),
            "youtuber_name": yt.get("channel_name"),
            "review_summary": raw.get("review_summary"),
            "ad_likelihood": r.get("ad_likelihood"),
        })
    return rows
