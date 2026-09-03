"""
Amazon Creators API - SearchItems 호출 예시 (v3.x / Login with Amazon)

호출 흐름:
  1) 액세스 토큰 발급 (client_credentials)  ->  api.amazon.com/auth/o2/token
  2) 발급받은 토큰으로 SearchItems 호출     ->  creatorsapi.amazon/catalog/v1/searchItems

실행: python test.py
"""

import os
import json

import requests
from dotenv import load_dotenv

load_dotenv()

# --- 환경변수 (.env 에서 로드) ---
CREDENTIAL_ID = os.environ["AMAZON_CREDENTIAL_ID"]
CREDENTIAL_SECRET = os.environ["AMAZON_CREDENTIAL_SECRET"]
PARTNER_TAG = os.environ["AMAZON_PARTNER_TAG"]
MARKETPLACE = os.environ["AMAZON_MARKETPLACE"]

# --- 엔드포인트 (v3.x 기준) ---
TOKEN_URL = "https://api.amazon.com/auth/o2/token"
API_BASE = "https://creatorsapi.amazon/catalog/v1"


def get_access_token() -> str:
    """client_credentials 방식으로 액세스 토큰 발급 (1시간 유효)."""
    resp = requests.post(
        TOKEN_URL,
        json={
            "grant_type": "client_credentials",
            "client_id": CREDENTIAL_ID,
            "client_secret": CREDENTIAL_SECRET,
            "scope": "creatorsapi::default",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def call(token: str, operation: str, body: dict) -> None:
    """지정 오퍼레이션을 호출하고 상태코드/본문을 출력 (에러여도 멈추지 않음)."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-marketplace": MARKETPLACE,
    }
    body = {"partnerTag": PARTNER_TAG, **body}
    resp = requests.post(f"{API_BASE}/{operation}", headers=headers, json=body, timeout=15)
    print(f"===== {operation} =====")
    print(f"[status] {resp.status_code}")
    print(f"[body] {resp.text}\n")


if __name__ == "__main__":
    token = get_access_token()
    print(f"[토큰 발급 완료] {token[:20]}...\n")

    # 4개 오퍼레이션을 각각 호출해서 계정 자격이 엔드포인트별로 다른지 확인
    call(token, "searchItems", {
        "keywords": "vitamin c serum",
        "itemCount": 3,
        "resources": ["itemInfo.title"],
    })
    call(token, "getItems", {
        "itemIds": ["B09B2SBHQK"],
        "itemIdType": "ASIN",
        "resources": ["itemInfo.title"],
    })
    call(token, "getVariations", {
        "asin": "B09B2SBHQK",
        "resources": ["itemInfo.title"],
    })
    call(token, "getBrowseNodes", {
        "browseNodeIds": ["283155"],
        "resources": ["browseNodes.children", "browseNodes.ancestor"],
    })
