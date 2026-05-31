"""
src/pipeline/spark_cleaner.py

Reverb 증폭 데이터(data/raw/reverb_100MB.csv)를
카테고리/브랜드 정규화 → Parquet 변환하여 HDFS/로컬 적재.

입력 컬럼: listing_id, title, make, model, year, condition,
          price_usd, price_krw, currency, category(query),
          country_code, created_at, is_sold
출력 컬럼: mule_id, title, brand, category, price_krw,
          condition, country_code, reg_date, is_sold, crawled_at
"""

import os, sys, re, logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType, StringType

# Windows에서 Spark Python 워커가 같은 인터프리터를 사용하도록 고정
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

INPUT_PATH  = "data/raw/reverb_100MB.csv"
OUTPUT_PATH = "data/sample/mule_processed.parquet"

# ---------------------------------------------------------------------------
# 카테고리 매핑: Reverb 검색 쿼리 → 서비스 9개 슬러그
# ---------------------------------------------------------------------------
QUERY_TO_CATEGORY = {
    "electric guitar":  "electric-guitar",
    "bass guitar":      "bass-guitar",
    "effects pedal":    "effector",
    "distortion pedal": "effector",
    "reverb pedal":     "effector",
    "synthesizer":      "synthesizer",
    "amplifier":        "amp",
    "acoustic guitar":  "acoustic-guitar",
    "drum":             "drum",
    "keyboard":         "synthesizer",
    "mixer":            "mixer-interface",
    "audio interface":  "mixer-interface",
}

# ---------------------------------------------------------------------------
# 브랜드 정규화 패턴 (make 컬럼 → 표준 영문 브랜드)
# ---------------------------------------------------------------------------
_BRAND_PATTERNS = [
    (r"(?i)(펜더|미펜|일펜|fender|fnd)",             "Fender"),
    (r"(?i)(깁슨|gibson|gib)",                       "Gibson"),
    (r"(?i)(보스|boss)",                             "Boss"),
    (r"(?i)(아이바네즈|ibanez|ibz)",                 "Ibanez"),
    (r"(?i)(마샬|marshall)",                         "Marshall"),
    (r"(?i)(이벤타이드|eventide)",                   "Eventide"),
    (r"(?i)(스트라이몬|strymon)",                    "Strymon"),
    (r"(?i)\bmxr\b",                                 "MXR"),
    (r"(?i)(일렉트로하모닉스|ehx|electro.harmonix)", "Electro-Harmonix"),
    (r"(?i)(tc\s*electronic|tc일렉)",                "TC Electronic"),
    (r"(?i)(켐퍼|kemper)",                           "Kemper"),
    (r"(?i)(롤랜드|roland)",                         "Roland"),
    (r"(?i)(야마하|yamaha)",                         "Yamaha"),
]

def _normalize_brand(make):
    if not make: return "Unknown"
    for p, b in _BRAND_PATTERNS:
        if re.search(p, make): return b
    return make.strip().title() or "Unknown"   # 미매핑은 make 원본 유지

normalize_brand_udf = F.udf(_normalize_brand, StringType())


def _map_category(query):
    if not query: return "effector"
    return QUERY_TO_CATEGORY.get(query.lower().strip(), "effector")

map_category_udf = F.udf(_map_category, StringType())


# ---------------------------------------------------------------------------
# 판매 여부 정제
# ---------------------------------------------------------------------------
def _to_bool(val):
    if val is None: return False
    return str(val).lower() in ("true", "1", "yes", "판매완료", "완료", "sold")

to_bool_udf = F.udf(_to_bool, BooleanType())


# ---------------------------------------------------------------------------
# 파이프라인
# ---------------------------------------------------------------------------
def build_spark():
    return (SparkSession.builder
            .appName("MuleDataCleaner")
            .master("local[*]")
            .getOrCreate())


def load_raw(spark):
    logger.info("CSV 로드: %s", INPUT_PATH)
    df = (spark.read
          .option("header","true")
          .option("inferSchema","false")
          .option("encoding","utf-8")
          .option("quote", '"')
          .option("escape", '"')   # pandas 이스케이프 방식과 일치
          .option("multiLine","true")
          .csv(INPUT_PATH))
    logger.info("원본 행 수: %d  컬럼: %s", df.count(), df.columns)
    return df


def clean(df):
    # 1. 컬럼 이름 정리
    df = (df
          .withColumnRenamed("listing_id",   "mule_id")
          .withColumnRenamed("created_at",   "reg_date")
          .withColumnRenamed("country_code", "country_code"))

    # 2. 카테고리 정규화 (query 문자열 → 서비스 슬러그)
    df = df.withColumn("category", map_category_udf(F.col("category")))

    # 3. 브랜드 정규화 (make → brand)
    df = df.withColumn("brand", normalize_brand_udf(F.col("make"))).drop("make")

    # 4. price_krw 수치형 변환 + 이상치 제거 (비숫자 값은 NULL로 처리)
    df = df.withColumn("price_krw",
        F.regexp_replace(F.col("price_krw"), r"[^0-9]", "").cast(IntegerType()))
    before = df.count()
    df = df.filter(F.col("price_krw").isNotNull() & (F.col("price_krw") > 0))
    logger.info("가격 이상치 제거: %d → %d행", before, df.count())

    # 5. is_sold Boolean 변환
    df = df.withColumn("is_sold", to_bool_udf(F.col("is_sold")))

    # 6. crawled_at 추가
    df = df.withColumn("crawled_at", F.current_timestamp())

    # 7. 최종 컬럼 선택 및 중복 제거
    keep = ["mule_id","title","brand","category","price_krw",
            "condition","country_code","reg_date","is_sold","crawled_at"]
    exist = [c for c in keep if c in df.columns]
    df = df.select(exist).dropDuplicates(["mule_id"])

    return df


def save_parquet(df):
    logger.info("Parquet 저장: %s", OUTPUT_PATH)
    # toPandas() + pyarrow로 저장 — Windows winutils 의존성 우회
    pdf = df.toPandas()
    Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    pdf.to_parquet(OUTPUT_PATH, index=False, engine="pyarrow")
    logger.info("저장 완료 | %d행", len(pdf))


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = load_raw(spark)
    df = clean(df)

    logger.info("최종 스키마:")
    df.printSchema()

    save_parquet(df)
    spark.stop()


if __name__ == "__main__":
    main()
