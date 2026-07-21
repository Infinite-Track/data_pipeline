# data_pipeline (v2 · 2차 스프린트)

YouTube 채널을 자동으로 추적해 스킨케어 제품 리뷰를 분석하고, 제품별 피부 고민 효능 점수를 산출하는 데이터 파이프라인입니다.

## 파이프라인 흐름

```
[채널 추적: youtubers 컬렉션]
    │
    ▼
[Step 1] 신규 영상 판별
    │   channels.list(uploads 재생목록 ID 조회) + playlistItems.list로 채널별 신규 영상 조회
    │   (search.list는 쿼터가 비싸고 2026-06-01부터 일일 호출 제한이 걸려 사용하지 않음)
    │   제목/설명/태그로 리뷰 영상 여부 판별(LLM: YES/NO/UNSURE) — 길이 제한 없음(쇼츠 포함)
    │   NO는 스킵, YES/UNSURE는 videos 컬렉션에 저장(status=PENDING)
    ▼
[Step 2] 신규 수집 (영상 → 리뷰 원본)
    │   status=PENDING 영상 URL을 Gemini에 전달해 "언급된 제품별로" 설명 + ad_likelihood 산출
    │   (영상 하나에 제품이 여러 개면 raw_reviews 문서도 그 개수만큼 생성됨)
    │   출력 형식이 깨지면 1회 재시도 → raw_reviews 컬렉션에 저장
    ▼
[Step 3] 리뷰 검증
    │   raw_reviews의 제품별 content에서 brand를 추출하고 category를 6개 중 하나로 분류
    │   (Cleansers/Toners/Treatments/Moisturizers/Sunscreens/Face Masks 외에는 리뷰 자체를 버림)
    │   specificity / relevance / skin_info 3축을 1~5점으로 채점
    │   reviews 컬렉션에 status=PENDING으로 저장 (품질/Amazon 검증은 다음 단계)
    ▼
[Step 4] 리뷰 분석 (리뷰 → 상품)
    │   PENDING 리뷰 중 3축 점수 미달은 DISQUALIFIED 처리 후 제외
    │   나머지를 (product_name, brand) 원문 표기 기준으로 묶어, 고유 조합당 Amazon Global에서
    │   공식 상품명/브랜드/카테고리 1회 검색(Gemini + Google 검색 그라운딩) 후 그 정보로 통일
    │   (리뷰가 자체 추출한 brand/category보다 Amazon 공식 정보를 우선시함)
    │   (다르게 표기된 동일 상품도 Amazon 검증 후 같은 이름으로 합쳐짐)
    │   Amazon에 없으면 NOTFOUND 처리 후 제외 (최종 목표가 아마존 제휴링크 유도라
    │    아마존에 없는 상품은 추천 대상이 될 수 없음), 찾으면 COMPLETED
    │   product_name/brand/category는 전부 소문자 + 공백 제거로 저장
    │   이번에 새로 COMPLETED된 리뷰만 집계해 mention_count / ad_likelihood_sum 계산 후
    │   products에 $inc로 누적(기존 누적치 + 이번 배치분), ad_ratio는 누적치 기준으로 재계산
    │   (reviews 전체를 매번 다시 긁어와 처음부터 재계산하지 않음)
    ▼
[Step 5] 성분 추출 (상품에 성분 추가)
    │   ※ 현재 Amazon API 미확보 → Gemini 웹서치로 대체
    │   status=PENDING 상품 대상, 브랜드 공식 사이트 → 올리브영 순으로 전성분(INCI) 리스트 크롤링
    │   결과에 따라 status: NOT_FOUND(못 찾음) / INSUFFICIENT_INGREDIENTS(10개 미만) /
    │   READY_FOR_ANALYSIS(충분히 찾음, STEP6 대상)로 갱신
    ▼
[Step 6] 성분 특징 추출
    │   status=READY_FOR_ANALYSIS 상품만 INCI API(POST /analyze)로 효능/안전성 분석
    │   → inciapi_raw_data 저장, status=COMPLETE로 갱신
    ▼
[Step 7] 개인화 추천 스키마 요약
    │   status=COMPLETE 상품만 대상, 11개 효능 점수 + 4개 피부타입 적합도 + 5개 위험도 플래그 산출
    │   → products_summarized 컬렉션 저장, status=SUMMARIZED로 갱신(재처리 방지)
    ▼
[Step 8] 정규화
        11개 효능 점수를 "고정 기준(fixed reference)"으로 0~100 정규화
        (v1과 달리 새 데이터가 들어와도 과거 상품의 점수가 변하지 않음)
        → products_normalized 컬렉션 저장 (추천 알고리즘이 소비하는 최종 데이터)
```

> 이전 버전(1차 스프린트) 다이어그램은 `description/seoulection_datapipeline.drawio.png` 참고.

## 환경 설정

### 필수 패키지 설치
```bash
pip install -r requirements.txt
```

### 환경 변수 (`.env`)
```
MONGODB_URI=mongodb+srv://...
MONGODB_DB_NAME=your_db_name
GEMINI_API_KEY=...
INCI_API_KEY=...
YOUTUBE_API_KEY=...       # STEP1: YouTube Data API v3 키 (신규 추가 필요)
```

### 채널 등록 (`youtubers` 컬렉션)
STEP1은 `youtubers` 컬렉션에 이미 등록된 채널만 조회합니다. 채널 추가/등록 자동화는
이번 스프린트 범위에 없으므로, 아래 형태로 문서를 직접 넣어줘야 합니다.
```json
{ "channel_name": "채널명", "url": "https://www.youtube.com/channel/UCxxxx" }
```
(`url`이 `@handle` 형태면 STEP1이 YouTube API로 channel_id를 자동 조회합니다. `last_checked_at`이
없으면 최초 실행 시 최근 24시간을 기준으로 조회합니다.)

## 실행

```bash
python main.py
```

## MongoDB 컬렉션 구조

| 컬렉션 | 설명 |
|--------|------|
| `youtubers` | 추적 대상 채널 목록 + 채널별 마지막 확인 시각(`last_checked_at`) |
| `videos` | STEP1이 판별한 신규 영상. `status`: PENDING → COMPLETED / EXCEPTION |
| `raw_reviews` | 제품 단위 Gemini 요약 원본(영상 1개 → 언급 제품 수만큼 문서) + ad_likelihood |
| `reviews` | 제품 단위 리뷰 + 품질 채점(specificity/relevance/skin_info). `status`: PENDING → DISQUALIFIED(품질 미달) / NOTFOUND(Amazon에 없음) / COMPLETED(집계 대상, product_name/brand/category가 Amazon 기준으로 정규화됨) |
| `products` | COMPLETED 리뷰 집계 + 성분(ingredients) + INCI 원본 분석(inciapi_raw_data). `status`는 reviews와 별개로 이 상품이 파이프라인 어디까지 처리됐는지 나타냄: `PENDING` → `NOT_FOUND`/`INSUFFICIENT_INGREDIENTS`/`READY_FOR_ANALYSIS` → `COMPLETE` → `SUMMARIZED` |
| `products_summarized` | 11개 효능 점수(원본) + 위험도 + 피부 타입 적합도 |
| `products_normalized` | `products_summarized`와 동일 스키마, 11개 효능 점수만 고정 기준으로 0~100 정규화 |

## 추후 계획 (이번 스프린트 범위 아님)

- **Prefect + EC2**: 각 step 모듈의 `run()`은 이미 부작용이 DB로 격리된 순수 async 함수라
  나중에 `@task`/`@flow`로 감싸고 EC2에 스케줄러로 올리기만 하면 되도록 구조를 맞춰뒀습니다.
- **Amazon API 연동**: STEP5는 현재 Gemini 웹서치(브랜드 공식 사이트/올리브영)로 성분을 채우고
  있으며, Amazon 상품 링크를 확보하면 `products_catalog` 컬렉션 경로를 추가할 계획입니다.
