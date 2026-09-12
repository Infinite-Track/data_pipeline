"""리뷰 텍스트 → 카테고리 대비 배수(multiple) 상위 TOP_N 분석 전담.

이모지 제거 → Kiwi 형태소 → lexicon(reactions) 매칭 → 리뷰 단위 언급 집계로
제품별 언급률(product_rate)을 구한 뒤, baseline(제품 카테고리별 평균 언급률)으로 나눈
    multiple = product_rate / baseline.by_category[제품카테고리][키워드]
를 지표로 쓴다. 같은 카테고리 평균 대비 얼마나 더 언급되는지(=lift)를 뜻한다.
product_rate가 MIN_MENTION_RATE 미만인 키워드는 표본이 작아 배수가 튀므로 후보에서 빼고,
multiple ≤ 1(카테고리 평균 이하)인 키워드는 저장하지 않는다. 그래서 제품당 0~TOP_N개가 된다.

두 가지 경로:
  - analyze_texts(texts, category): 단일 머신(Kiwi 배치). 소규모/단독 테스트용.
  - analyze_all(rows, categories):  PySpark(local[*]). 학습용 map-reduce (parquet 없이 메모리 입력).
"""
import json
from collections import defaultdict
from pathlib import Path

LEXICON_PATH = Path(__file__).resolve().parent.parent.parent / "lexicons" / "lexicon_v1.json"
BASELINE_PATH = Path(__file__).resolve().parent.parent.parent / "lexicons" / "baseline_v1.json"
EMOJI_PATTERN = "[\U00010000-\U0010FFFF\u200d\ufe0e\ufe0f]"
TOP_N = 5
# product_rate가 이 값 미만인 키워드는 표본이 작아 multiple이 과장되므로 top-N 후보에서 뺀다.
MIN_MENTION_RATE = 0.05

_w2r = None
_baseline = None


def _load_w2r() -> dict[str, str]:
    """lexicon의 form/tag → 카테고리명 역인덱스. (예: '보습/NNG' → '보습·장벽')"""
    global _w2r
    if _w2r is None:
        lex = json.loads(LEXICON_PATH.read_text(encoding="utf-8"))
        _w2r = {w: name for name, words in lex["reactions"].items() for w in words}
    return _w2r


def _norm_cat(category: str | None) -> str:
    """제품 category와 baseline by_category 키를 대소문자 무시로 맞추기 위한 정규화."""
    return (category or "").strip().lower()


def _load_baseline() -> dict[str, dict[str, float]]:
    """baseline_v1.json의 by_category → {정규화 카테고리: {키워드: 평균 언급률}}."""
    global _baseline
    if _baseline is None:
        b = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
        _baseline = {_norm_cat(cat): obj["rates"] for cat, obj in b["by_category"].items()}
    return _baseline


# ── 단일 머신 (Kiwi 배치) ──────────────────────────────────────────────
import re  # noqa: E402  (EMOJI_RE 컴파일용, 단일머신 경로에서만 사용)

EMOJI_RE = re.compile(EMOJI_PATTERN)
_kiwi = None


def _get_kiwi():
    global _kiwi
    if _kiwi is None:
        from kiwipiepy import Kiwi
        _kiwi = Kiwi()
    return _kiwi


def analyze_texts(texts: list[str], category: str) -> tuple[list[dict], int]:
    """리뷰 텍스트 목록 → 카테고리 대비 배수(multiple) 상위 TOP_N (단일 머신).
    category = 제품 카테고리(분모 baseline 선택용).
    반환: ([{keyword, multiple}], 분석에 쓴 리뷰 수). 리뷰 단위로 키워드 중복 제거."""
    w2r = _load_w2r()
    base_rates = _load_baseline().get(_norm_cat(category), {})
    texts = [EMOJI_RE.sub("", t) for t in texts if t and t.strip()]
    total = len(texts)
    if total == 0 or not base_rates:              # 리뷰 없음 / baseline에 없는 카테고리
        return [], total

    counts: dict[str, int] = defaultdict(int)
    for tokens in _get_kiwi().tokenize(texts):   # 배치 토크나이즈
        mentioned = {w2r[f"{t.form}/{t.tag}"] for t in tokens if f"{t.form}/{t.tag}" in w2r}
        for kw in mentioned:                      # 리뷰 1건당 키워드 1회만
            counts[kw] += 1

    scored = []
    for kw, c in counts.items():
        rate = c / total
        base = base_rates.get(kw)
        if rate < MIN_MENTION_RATE or not base:   # 표본 작은 키워드 / baseline 없는 키워드 제외
            continue
        mult = rate / base
        if mult <= 1:                             # 카테고리 평균 이하(배수 ≤ 1)는 저장하지 않음
            continue
        scored.append({"keyword": kw, "multiple": round(mult, 6)})
    scored.sort(key=lambda r: -r["multiple"])
    return scored[:TOP_N], total


# ── PySpark (local[*]) ────────────────────────────────────────────────
def analyze_all(rows: list[tuple[str, str]], categories: dict[str, str]) -> dict[str, list[dict]]:
    """rows = [(asin, review_text), ...], categories = {asin: 제품카테고리}
       → {asin: [{keyword, multiple} ... 상위 TOP_N]}.
    multiple = product_rate / baseline.by_category[제품카테고리][keyword].
    product_rate < MIN_MENTION_RATE, baseline에 없는 키워드, multiple ≤ 1은 모두 제외. 빈 입력이면 {}.
    broadcast(lexicon/baseline/카테고리맵) → mapPartitions(파티션당 Kiwi 1회) → groupBy/join → Window(top-N)."""
    rows = [(a, t) for a, t in rows if a and t and t.strip()]
    if not rows:
        return {}

    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType
    from pyspark.sql.window import Window
    from steps.spark_utils import build_session

    cat_of = {a: _norm_cat(c) for a, c in categories.items()}

    spark = build_session("review-keywords")
    try:
        w2r_b = spark.sparkContext.broadcast(_load_w2r())
        baseline_b = spark.sparkContext.broadcast(_load_baseline())
        cat_of_b = spark.sparkContext.broadcast(cat_of)
        emoji_pattern = EMOJI_PATTERN   # 컴파일 regex 대신 패턴 문자열을 워커로 넘겨 재컴파일

        reviews = spark.createDataFrame(rows, ["asin", "text"])
        totals = reviews.groupBy("asin").count().withColumnRenamed("count", "total")

        def tok_partition(part):
            import re as _re
            from kiwipiepy import Kiwi
            kiwi = Kiwi()
            emoji = _re.compile(emoji_pattern)
            w2r = w2r_b.value
            for row in part:
                text = emoji.sub("", row["text"])
                cats = {w2r[f"{t.form}/{t.tag}"] for t in kiwi.tokenize(text)
                        if f"{t.form}/{t.tag}" in w2r}
                for c in cats:
                    yield (row["asin"], c)

        def to_multiple(asin, keyword, product_rate):
            base = baseline_b.value.get(cat_of_b.value.get(asin), {}).get(keyword)
            return product_rate / base if base else None
        multiple_udf = F.udf(to_multiple, DoubleType())

        hits = reviews.rdd.mapPartitions(tok_partition).toDF(["asin", "keyword"])
        rate = (
            hits.groupBy("asin", "keyword").count()
            .join(totals, "asin")
            .withColumn("product_rate", F.col("count") / F.col("total"))
            .filter(F.col("product_rate") >= MIN_MENTION_RATE)
            .withColumn("multiple", multiple_udf("asin", "keyword", "product_rate"))
            .filter(F.col("multiple").isNotNull())
            .filter(F.col("multiple") > 1)   # 카테고리 평균 이하(배수 ≤ 1)는 저장하지 않음
            .withColumn("multiple", F.round(F.col("multiple"), 3))
        )
        w = Window.partitionBy("asin").orderBy(F.desc("multiple"), F.asc("keyword"))
        top = (
            rate.withColumn("rk", F.row_number().over(w))
            .filter(F.col("rk") <= TOP_N)
            .select("asin", "keyword", "multiple")
        )

        out: dict[str, list[dict]] = {}
        for r in top.collect():
            out.setdefault(r["asin"], []).append(
                {"keyword": r["keyword"], "multiple": float(r["multiple"])})
        for asin in out:
            out[asin].sort(key=lambda x: -x["multiple"])
        return out
    finally:
        spark.stop()
