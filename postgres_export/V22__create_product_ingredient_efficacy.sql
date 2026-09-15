-- 성분 ↔ 효능 다대다 연결 테이블 (ERD: product_ingredient_efficacy).
-- 한 성분이 여러 효능에 기여할 수 있으므로(INCI 분석 성분의 약 46%) 단일 FK 컬럼 대신 연결 테이블을 쓴다.
-- 타입은 참조 대상 실제 PK에 맞춤: product_efficacies.id = INTEGER, product_ingredient.id = BIGINT.
-- ON DELETE CASCADE: 파이프라인이 상품을 재발행(효능·성분 행 삭제 후 재삽입)할 때 연결이 자동으로 정리된다.
-- 백엔드 Flyway에 그대로 넣을 수 있도록 IF NOT EXISTS 로 작성. (2026-09-15 운영 DB에 수동 적용됨)

CREATE TABLE IF NOT EXISTS public.product_ingredient_efficacy (
    product_efficacies INTEGER NOT NULL
        REFERENCES public.product_efficacies(id) ON DELETE CASCADE,
    product_ingredient BIGINT NOT NULL
        REFERENCES public.product_ingredient(id) ON DELETE CASCADE,
    PRIMARY KEY (product_efficacies, product_ingredient)
);

CREATE INDEX IF NOT EXISTS ix_product_ingredient_efficacy_ingredient
    ON public.product_ingredient_efficacy (product_ingredient);
