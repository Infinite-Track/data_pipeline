# Debian 12(bookworm) 고정: 3.11-slim이 Debian 13(trixie)로 올라가면서 openjdk-17 패키지가 사라져 빌드가 깨짐(2026-09).
FROM python:3.11-slim-bookworm

# PySpark용 JVM (step7·step9 Spark). 심볼릭 링크로 JAVA_HOME을 아키텍처(amd64/arm64) 무관하게 고정.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY steps/ steps/
COPY lexicons/ lexicons/

# 시크릿/DB 접속정보는 이미지에 넣지 않고 런타임 env로 주입한다:
#   docker run --env-file .env <image>   (POSTGRES_URI/MONGODB_URI는 실제 DB 호스트로)
CMD ["python", "main.py"]
