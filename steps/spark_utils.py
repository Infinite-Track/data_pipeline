"""PySpark 공용 유틸 (로컬 local[*] 모드).

- ensure_java_home: JAVA_HOME 없으면 brew openjdk 탐지해 설정(로컬 편의).
- build_session: 로컬 SparkSession 생성(작은 데이터용 파티션 축소).
"""
import os

_JAVA_CANDIDATES = [
    "/opt/homebrew/opt/openjdk@17", "/usr/local/opt/openjdk@17",
    "/opt/homebrew/opt/openjdk", "/usr/local/opt/openjdk",
]


def ensure_java_home() -> None:
    """JAVA_HOME이 이미 있으면(쉘/.env) 그대로 두고, 없으면 brew openjdk 경로를 찾아 설정한다."""
    if os.environ.get("JAVA_HOME"):
        return
    for cand in _JAVA_CANDIDATES:
        if os.path.exists(os.path.join(cand, "bin", "java")):
            os.environ["JAVA_HOME"] = cand
            os.environ["PATH"] = os.path.join(cand, "bin") + os.pathsep + os.environ.get("PATH", "")
            return


def build_session(app_name: str):
    """로컬 SparkSession. 데이터가 작아 셔플 파티션을 8로 낮춘다."""
    ensure_java_home()
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
