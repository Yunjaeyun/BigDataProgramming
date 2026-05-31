-- ============================================================
-- src/analyze/validation.sql
-- 추천 파이프라인 결과 검증 및 분석 쿼리 (Q1~Q3)
--
-- 실행: hive -f src/analyze/validation.sql
-- ============================================================

USE mule_reco;

-- ============================================================
-- Q1. 카테고리별 가격 분포 (시세 가이드라인)
-- ============================================================
SELECT '=== Q1: 카테고리별 시세 분포 ===' AS query_label;

SELECT
    category,
    item_count,
    CONCAT(FORMAT_NUMBER(price_min, 0), ' 원')    AS 최저가,
    CONCAT(FORMAT_NUMBER(price_q1, 0), ' 원')     AS Q1,
    CONCAT(FORMAT_NUMBER(price_median, 0), ' 원') AS 중앙값,
    CONCAT(FORMAT_NUMBER(price_q3, 0), ' 원')     AS Q3,
    CONCAT(FORMAT_NUMBER(price_max, 0), ' 원')    AS 최고가
FROM v_price_stats;

-- ------------------------------------------------------------
-- 브랜드 × 카테고리 교차 분석
-- ------------------------------------------------------------
SELECT '=== Q1-2: 브랜드별 평균가 및 판매율 TOP 20 ===' AS query_label;

SELECT
    brand,
    category,
    item_count,
    CONCAT(FORMAT_NUMBER(avg_price_krw, 0), ' 원') AS 평균가,
    CONCAT(sold_rate_pct, '%')                     AS 판매완료율
FROM v_brand_stats
LIMIT 20;

-- ============================================================
-- Q2. 판매 완료 비율 — 카테고리별 수요/공급 분석
-- ============================================================
SELECT '=== Q2: 카테고리별 판매 완료율 ===' AS query_label;

SELECT
    category,
    COUNT(*)                                                        AS total,
    SUM(CASE WHEN is_sold = true THEN 1 ELSE 0 END)                AS sold,
    ROUND(SUM(CASE WHEN is_sold = true THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1)                                   AS sold_pct,
    ROUND(AVG(CASE WHEN is_sold = true THEN price_krw END), 0)     AS avg_sold_price,
    ROUND(AVG(CASE WHEN is_sold = false THEN price_krw END), 0)    AS avg_unsold_price
FROM mule_items
GROUP BY category
ORDER BY sold_pct DESC;

-- ============================================================
-- Q3. 추천 결과 검증 — 적재 현황
-- ============================================================
SELECT '=== Q3: 추천 결과 적재 현황 ===' AS query_label;

SELECT
    COUNT(*)            AS 추천대상유저수,
    MAX(updated_at)     AS 최신갱신시각,
    MIN(updated_at)     AS 최초갱신시각
FROM user_recommendations;

-- 유저별 추천 아이템 수 확인
SELECT '=== Q3-2: 유저별 추천 아이템 수 ===' AS query_label;

SELECT
    user_id,
    SIZE(SPLIT(recommended_item_ids, ',')) AS 추천아이템수,
    recommended_item_ids
FROM user_recommendations
ORDER BY user_id;

-- ============================================================
-- 데이터 품질 검증
-- ============================================================
SELECT '=== 데이터 품질 검증 ===' AS query_label;

SELECT
    COUNT(*)                                             AS 전체행수,
    COUNT(DISTINCT category)                             AS 카테고리수,
    COUNT(DISTINCT brand)                                AS 브랜드수,
    SUM(CASE WHEN price_krw IS NULL OR price_krw <= 0
             THEN 1 ELSE 0 END)                          AS 가격이상행수,
    SUM(CASE WHEN title IS NULL OR title = ''
             THEN 1 ELSE 0 END)                          AS 제목없는행수
FROM mule_items;
