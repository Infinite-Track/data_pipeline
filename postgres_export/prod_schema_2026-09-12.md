# 운영 Postgres 스키마 (2026-09-12)

- 위치: seoulection EC2(192.168.3.38) 도커 `seoulection-pg` (postgres:17), DB `seoulection`. 백엔드 **Flyway** 소유(V19 2026-09-12).
- 파이프라인은 DDL을 실행하지 않는다. `main.py` preflight가 `steps/postgres_publish.REQUIRED_SCHEMA`를 검사해 불일치 시 어떤 step도 실행 전 중단한다.

## 운영 규약과 파이프라인 대응 (postgres_publish.py 재작성 기준)

| 항목 | 운영 규약 | 파이프라인 대응 |
|---|---|---|
| products_catalog.id | VARCHAR 숫자 문자열, 자동 채번 없음 | 테이블 락 후 max(id::int)+1 (`_insert_catalog`) |
| products_catalog.asin | 비유니크 컬럼 | `get_product_id(asin)`로 조회, 없을 때만 생성 |
| products_catalog.content | 설명 텍스트 | Mongo products.description → content |
| 하위 FK | product_id VARCHAR | 모든 발행 함수가 product_id를 받음 |
| products_url | id identity, region/active/updated_at | Amazon US 행만 upsert, 백엔드가 만든 다른 링크 행은 건드리지 않음 |
| youtuber_reviews | id 자동 없음, (product_id, video_url) 유니크 없음, review/ad_likelihood | 락 후 max(id)+1, (product_id, video_url) 수동 upsert |
| product_ingredient(단수) | UNIQUE(product_id, inci_order, raw_name), source NN default 'ADMIN' | 삭제 후 재삽입, source None → 'ADMIN' |
| product_inci_analysis | PK product_id, ingredients {parsed, declared} jsonb, effects jsonb(topEffects), overall_safety_score, inci_coverage, ingredient_source, schema_version=1, analyzed_at | step6가 inciapi_raw_data에서 매핑해 upsert |
| product_efficacies(효능당 행) | **없음** | effects JSON으로 대체. 어드민 function은 Mongo에만(step8이 점수에 반영) |
| reviews_keyword | id identity, UNIQUE(product_id, keyword), multiple | 삭제 후 재삽입 |
| products_normalized | PK product_id, 점수 20종 NOT NULL, summarized_at NN | null 있으면 건너뛰고 경고; 카탈로그 없는 asin 건너뜀 |

## 검증
- 운영 DDL(`pg_dump --schema-only`)을 로컬 `seoulection_prodtest`에 적재해 발행 함수 전체를 실데이터로 통과시킴(채번, 재발행 멱등, JSON 매핑, NOT NULL 거부).
