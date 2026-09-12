"""step9 정규화 PySpark 버전 (학습용).

products_summarized 전체(docs) + 정규화 대상 필드(fields) → min-max 정규화된 docs.
min/max는 전역 집계(agg), 변환은 withColumn — Spark의 "집계 + transform" 전형 패턴.
규모가 작아 성능 이득은 없고 학습용(단일 머신이 더 빠름).

step9 normalize_score와 동일 규칙:
  - max <= min : 0(max==0) / 100(그 외)   (변별력 없는 필드)
  - 그 외      : round((raw-min)/(max-min)*100, 2)
  - null 값    : 그대로 유지(집계·변환에서 제외)

스키마 추론 오류(같은 컬럼이 행마다 int/float 섞임) 방지를 위해 명시적 스키마로 만든다.
문자열 필드 외 모든 숫자 필드는 double로 통일한다(위험 플래그 2 → 2.0 등, 값 의미는 동일).
"""
from steps.spark_utils import build_session

# 정규화 대상이 아닌 문자열(메타) 필드 — 스키마에서 StringType으로 둔다.
STRING_FIELDS = {"asin", "summarized_at"}


def normalize(docs: list[dict], fields: list[str]) -> list[dict]:
    """docs(products_summarized) → fields를 min-max 정규화한 docs 리스트. 빈 입력이면 []."""
    if not docs:
        return []

    spark = build_session("normalize-scores")
    try:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType

        keys = list({k for d in docs for k in d})
        schema = StructType([
            StructField(k, StringType() if k in STRING_FIELDS else DoubleType(), True)
            for k in keys
        ])
        rows = [
            {k: (d.get(k) if k in STRING_FIELDS
                 else (float(d[k]) if d.get(k) is not None else None))
             for k in keys}
            for d in docs
        ]
        df = spark.createDataFrame(rows, schema)

        present = [f for f in fields if f in keys]

        # 전역 min/max (null 제외 — F.min/F.max가 자동으로 무시).
        mm = {}
        if present:
            aggs = []
            for f in present:
                aggs += [F.min(f).alias(f + "__min"), F.max(f).alias(f + "__max")]
            mm = df.agg(*aggs).collect()[0].asDict()

        # 각 필드 정규화 (null은 유지).
        for f in present:
            mn, mx = mm.get(f + "__min"), mm.get(f + "__max")
            if mn is None or mx is None:
                continue   # 전부 null → 그대로 둔다
            if mx <= mn:
                val = 0.0 if mx == 0 else 100.0
                df = df.withColumn(
                    f, F.when(F.col(f).isNull(), F.lit(None).cast("double")).otherwise(F.lit(val)))
            else:
                df = df.withColumn(
                    f, F.round((F.col(f) - F.lit(mn)) / (F.lit(mx) - F.lit(mn)) * 100, 2))

        return [r.asDict() for r in df.collect()]
    finally:
        spark.stop()
