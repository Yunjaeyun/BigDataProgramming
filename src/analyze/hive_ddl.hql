-- ============================================================
-- src/analyze/hive_ddl.hql
-- Mule 추천 파이프라인 Hive 스키마 정의
--
-- 실행: hive -f src/analyze/hive_ddl.hql
-- ============================================================

-- 데이터베이스 생성
CREATE DATABASE IF NOT EXISTS mule_reco
COMMENT 'Mule 중고악기 추천 파이프라인 데이터 웨어하우스';

USE mule_reco;

-- ------------------------------------------------------------
-- 1. 원본 Reverb 수집 데이터 (External Table)
-- ------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS reverb_raw (
    listing_id   STRING,
    title        STRING,
    make         STRING,
    model        STRING,
    year         STRING,
    condition    STRING,
    price_usd    DOUBLE,
    price_krw    INT,
    currency     STRING,
    category     STRING,
    country_code STRING,
    created_at   STRING,
    is_sold      STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/mule_reco/reverb_raw'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ------------------------------------------------------------
-- 2. Spark 전처리 완료 데이터 (External Table — Parquet)
-- ------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS mule_items (
    mule_id      STRING,
    title        STRING,
    brand        STRING,
    category     STRING,
    price_krw    INT,
    condition    STRING,
    country_code STRING,
    reg_date     STRING,
    is_sold      BOOLEAN
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/mule_reco/mule_items';

-- ------------------------------------------------------------
-- 3. 유저 추천 결과 테이블 (Managed Table — ORC)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id              INT,
    recommended_item_ids STRING,
    updated_at           TIMESTAMP
)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ------------------------------------------------------------
-- 4. 분석용 집계 뷰 — Q1: 카테고리별 가격 분포
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_price_stats AS
SELECT
    category,
    COUNT(*)                            AS item_count,
    ROUND(MIN(price_krw), 0)            AS price_min,
    ROUND(PERCENTILE_APPROX(price_krw, 0.25), 0) AS price_q1,
    ROUND(PERCENTILE_APPROX(price_krw, 0.50), 0) AS price_median,
    ROUND(PERCENTILE_APPROX(price_krw, 0.75), 0) AS price_q3,
    ROUND(MAX(price_krw), 0)            AS price_max,
    ROUND(AVG(price_krw), 0)            AS price_avg
FROM mule_items
WHERE price_krw > 0
GROUP BY category
ORDER BY price_median DESC;

-- ------------------------------------------------------------
-- 5. 분석용 집계 뷰 — 브랜드별 매물 수
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_brand_stats AS
SELECT
    brand,
    category,
    COUNT(*)                            AS item_count,
    ROUND(AVG(price_krw), 0)            AS avg_price_krw,
    SUM(CASE WHEN is_sold = true THEN 1 ELSE 0 END) AS sold_count,
    ROUND(
        SUM(CASE WHEN is_sold = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
    )                                   AS sold_rate_pct
FROM mule_items
GROUP BY brand, category
HAVING COUNT(*) >= 10
ORDER BY item_count DESC;

-- 완료 메시지
SELECT 'Hive DDL 완료: mule_reco 데이터베이스 및 테이블 생성' AS status;
