"""
src/recommend/recommender_engine.py

하이브리드 콘텐츠 기반 추천 엔진
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
● 입력: Reverb 정제 Parquet + PostgreSQL 덤프 CSV 3종
● 실 데이터 우선 로드 — 파일 없을 때만 합성 Fallback(경고 로그)
● PySpark ML 파이프라인 피처 벡터화 (StringIndexer→OHE→MinMaxScaler)
● 유저 취향 벡터 = 행동 신호 가중평균 (좋아요 1.0 / 검색 0.7 / 판매 0.4/0.2)
● 카테고리 하드 필터 → 코사인 유사도 THRESHOLD=0.6 컷 → Top-5
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
실행: spark-submit --master local[*] src/recommend/recommender_engine.py

PostgreSQL 덤프 예시:
  \\copy (SELECT user_id, post_id FROM post_like) TO 'data/sample/likes.csv' CSV HEADER;
  \\copy (SELECT user_id, search_keyword FROM recent_search) TO 'data/sample/searches.csv' CSV HEADER;
  \\copy (SELECT user_id, post_id, sale_status FROM post) TO 'data/sample/sales.csv' CSV HEADER;
"""

import os
import sys
import math
import logging
import numpy as np
import pandas as pd
from pathlib import Path

os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    TimestampType, DoubleType, ArrayType,
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder,
    VectorAssembler, MinMaxScaler,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────
# 경로 (실 PostgreSQL 덤프 CSV)
# ────────────────────────────────────────────────────────────
PROCESSED_PATH = "data/sample/mule_processed.parquet"
LIKES_CSV      = "data/sample/likes.csv"       # post_like     → user_id, post_id
SEARCHES_CSV   = "data/sample/searches.csv"    # recent_search → user_id, search_keyword
SALES_CSV      = "data/sample/sales.csv"       # post          → user_id, post_id, sale_status
OUTPUT_PATH    = "data/sample/user_recommendations_output"

# ────────────────────────────────────────────────────────────
# 튜닝 파라미터 (보고서 실험용 상수로 모음)
# ────────────────────────────────────────────────────────────
THRESHOLD  = 0.6   # 코사인 유사도 최소 임계값 — 미달 시 추천 제외 (억지 5개 불채움)
TOP_N      = 5     # 유저당 최대 추천 수
TOP_CAT_N  = 2     # 카테고리 하드 필터: 유저별 상위 N개 카테고리만 후보 허용

W_LIKE     = 1.0   # 좋아요
W_SEARCH   = 0.7   # 검색어 브랜드 매핑
W_SOLD     = 0.4   # 판매 완료(sold_out)
W_ACTIVE   = 0.2   # 판매 중 / 예약(on_sale, reserved)

# ────────────────────────────────────────────────────────────
# 브랜드 매핑 사전 — spark_cleaner.py 와 동일 (검색어→브랜드)
# ────────────────────────────────────────────────────────────
BRAND_DICT: dict[str, str] = {
    "fender": "Fender", "펜더": "Fender", "미펜": "Fender", "일펜": "Fender", "fnd": "Fender",
    "gibson": "Gibson", "깁슨": "Gibson", "gib": "Gibson",
    "boss":   "Boss",   "보스": "Boss",
    "ibanez": "Ibanez", "아이바네즈": "Ibanez", "ibz": "Ibanez",
    "marshall": "Marshall", "마샬": "Marshall",
    "eventide": "Eventide", "이벤타이드": "Eventide",
    "strymon":  "Strymon",  "스트라이몬": "Strymon",
    "mxr":      "MXR",
    "ehx": "Electro-Harmonix", "electro-harmonix": "Electro-Harmonix",
    "tc electronic": "TC Electronic", "tc일렉": "TC Electronic",
    "kemper": "Kemper", "켐퍼": "Kemper",
    "roland": "Roland", "롤랜드": "Roland",
    "yamaha": "Yamaha", "야마하": "Yamaha",
}

def _keyword_to_brand(kw: str | None) -> str | None:
    """검색어에서 브랜드 토큰을 탐색. 매핑 안 되면 None(스킵)."""
    if not kw:
        return None
    kw_lower = kw.lower().strip()
    for token, brand in BRAND_DICT.items():
        if token in kw_lower:
            return brand
    return None  # 매핑 실패 → 해당 검색어 신호 무시

keyword_to_brand_udf = F.udf(_keyword_to_brand, StringType())

# ────────────────────────────────────────────────────────────
# 폴백 스키마 (파일 없을 때 합성 DataFrame 구조 유지용)
# ────────────────────────────────────────────────────────────
LIKES_SCHEMA = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("post_id", StringType(),  True),
])
SEARCHES_SCHEMA = StructType([
    StructField("user_id",        IntegerType(), True),
    StructField("search_keyword", StringType(),  True),
])
SALES_SCHEMA = StructType([
    StructField("user_id",     IntegerType(), True),
    StructField("post_id",     StringType(),  True),
    StructField("sale_status", StringType(),  True),
])


# ────────────────────────────────────────────────────────────
# 데이터 로드 — 실 CSV 우선, 없으면 합성 Fallback
# ────────────────────────────────────────────────────────────
def _load_csv(spark, path: str, schema: StructType, label: str):
    """
    실 PostgreSQL 덤프 CSV 로드.
    파일이 없으면 None 반환 → 호출부에서 합성 생성.
    """
    if not os.path.exists(path):
        logger.warning("⚠️  실 데이터 없음 — 합성 데이터로 대체 실행: %s", path)
        return None
    try:
        df = (spark.read
              .option("header", "true")
              .schema(schema)          # 명시적 스키마 적용 → 타입 오류 사전 차단
              .csv(path))
        logger.info("[%s] 실 데이터 로드: %d행 ← %s", label, df.count(), path)
        return df
    except Exception as exc:
        logger.warning("⚠️  [%s] 로드 실패 (%s) — 합성 데이터 대체", label, exc)
        return None


def _synthetic_likes(spark):
    rows = [(1,"101"),(1,"205"),(1,"88"),
            (2,"101"),(2,"303"),(2,"404"),
            (3,"205"),(3,"502"),(3,"88"),
            (4,"101"),(4,"505"),(4,"303"),
            (5,"303"),(5,"204"),(5,"101")]
    return spark.createDataFrame(rows, LIKES_SCHEMA)

def _synthetic_searches(spark):
    rows = [(1,"fender telecaster"),(1,"boss overdrive"),
            (2,"gibson les paul"),(2,"marshall amp"),
            (3,"strymon reverb"),(3,"tc electronic"),
            (4,"roland synth"),(4,"yamaha keyboard"),
            (5,"ibanez bass"),(5,"mxr phase")]
    return spark.createDataFrame(rows, SEARCHES_SCHEMA)

def _synthetic_sales(spark):
    rows = [(1,"601","on_sale"),(2,"602","sold_out"),
            (3,"603","reserved"),(4,"604","on_sale"),
            (5,"605","sold_out"),(1,"606","sold_out")]
    return spark.createDataFrame(rows, SALES_SCHEMA)


def load_logs(spark):
    """
    3종 행동 로그 로드.
    실 CSV가 있으면 무조건 실 데이터 사용.
    파일 없을 때만 경고 + 합성 Fallback.
    """
    likes_df   = _load_csv(spark, LIKES_CSV,   LIKES_SCHEMA,   "likes")   or _synthetic_likes(spark)
    searches_df= _load_csv(spark, SEARCHES_CSV, SEARCHES_SCHEMA, "searches") or _synthetic_searches(spark)
    sales_df   = _load_csv(spark, SALES_CSV,   SALES_SCHEMA,   "sales")   or _synthetic_sales(spark)
    return likes_df, searches_df, sales_df


# ────────────────────────────────────────────────────────────
# PySpark ML 피처 파이프라인
# ────────────────────────────────────────────────────────────
def build_feature_pipeline(items_df):
    """
    아이템 피처 벡터화: category(OHE) + brand(OHE) + price(MinMaxScaler) 결합.

    파이프라인 단계:
      category  → StringIndexer → OneHotEncoder (cat_vec)
      brand     → StringIndexer → OneHotEncoder (brand_vec)
      price_num → VectorAssembler → MinMaxScaler (price_scaled)
      [cat_vec, brand_vec, price_scaled] → VectorAssembler → features
    """
    # price 컬럼 통일 (price_krw 우선, 없으면 price)
    price_col = "price_krw" if "price_krw" in items_df.columns else "price"
    items_df  = (items_df
                 .withColumn("price_num",
                             F.col(price_col).cast(DoubleType()))
                 .fillna({"price_num": 0.0, "category": "effector", "brand": "Unknown"}))

    # category 원핫 인코딩
    cat_idx = StringIndexer(inputCol="category", outputCol="cat_idx",
                            handleInvalid="keep")
    cat_enc = OneHotEncoder(inputCol="cat_idx", outputCol="cat_vec",
                            dropLast=False, handleInvalid="keep")

    # brand 원핫 인코딩
    brand_idx = StringIndexer(inputCol="brand", outputCol="brand_idx",
                               handleInvalid="keep")
    brand_enc = OneHotEncoder(inputCol="brand_idx", outputCol="brand_vec",
                               dropLast=False, handleInvalid="keep")

    # price MinMaxScaler (VectorAssembler로 1-dim 벡터 래핑 필요)
    price_asm = VectorAssembler(inputCols=["price_num"], outputCol="price_raw",
                                handleInvalid="keep")
    price_scaler = MinMaxScaler(inputCol="price_raw", outputCol="price_scaled")

    # 전체 피처 결합
    final_asm = VectorAssembler(
        inputCols=["cat_vec", "brand_vec", "price_scaled"],
        outputCol="features",
        handleInvalid="keep",
    )

    pipeline = Pipeline(stages=[
        cat_idx, cat_enc,
        brand_idx, brand_enc,
        price_asm, price_scaler,
        final_asm,
    ])

    model = pipeline.fit(items_df)
    featured = model.transform(items_df)
    logger.info("ML 피처 파이프라인 완료: %d 아이템", featured.count())
    return featured


# ────────────────────────────────────────────────────────────
# 피처 벡터 연산 UDF
# ────────────────────────────────────────────────────────────
# DenseVector → Python list (groupBy agg 호환용)
to_array_udf = F.udf(lambda v: v.toArray().tolist() if v else None,
                     ArrayType(DoubleType()))

@F.udf(ArrayType(DoubleType()))
def weighted_avg_udf(arrays, weights):
    """
    배열 리스트의 가중평균 계산 UDF.
    유저 취향 벡터 = Σ(feature_arr_i × weight_i) / Σweight_i
    """
    if not arrays or not weights:
        return None
    n = len(arrays[0]) if arrays else 0
    if n == 0:
        return None
    result   = [0.0] * n
    total_w  = 0.0
    for arr, w in zip(arrays, weights):
        if arr and len(arr) == n and w:
            for i in range(n):
                result[i] += arr[i] * w
            total_w += w
    if total_w == 0:
        return None
    return [x / total_w for x in result]

@F.udf(DoubleType())
def cosine_sim_udf(a, b):
    """
    코사인 유사도 UDF.
    방식 선택 이유: 유저 수가 수십~수백 명 수준이므로
    cross join + UDF 방식이 broadcast join 대비 구현 복잡도가 낮고
    HDP Sandbox 규모에서 성능 차이가 미미하여 채택.
    """
    if not a or not b or len(a) != len(b):
        return 0.0
    dot  = sum(x * y for x, y in zip(a, b))
    na   = math.sqrt(sum(x * x for x in a))
    nb   = math.sqrt(sum(x * x for x in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


# ────────────────────────────────────────────────────────────
# 유저 행동 신호 수집
# ────────────────────────────────────────────────────────────
def build_user_signals(likes_df, searches_df, sales_df, items_df):
    """
    행동 로그 3종에서 (user_id, mule_id, weight) 형태의 신호 DataFrame 생성.

    좋아요    → item의 post_id = mule_id 매핑 (가중치 1.0)
    검색어    → BRAND_DICT 매핑 → 해당 브랜드 아이템  (가중치 0.7)
                 매핑 안 되는 키워드는 스킵
    판매 sold_out   → post_id = mule_id 매핑              (가중치 0.4)
    판매 on_sale/reserved                                   (가중치 0.2)
    """
    items_slim = items_df.select(
        F.col("mule_id").cast(StringType()).alias("mule_id"),
        "category", "brand",
    )

    # 좋아요
    like_sig = (
        likes_df
        .join(items_slim, F.col("post_id") == F.col("mule_id"), "inner")
        .select("user_id", "mule_id", F.lit(W_LIKE).cast(DoubleType()).alias("weight"))
    )

    # 검색어 → 브랜드 추출 → 해당 브랜드 아이템
    search_sig = (
        searches_df
        .withColumn("brand", keyword_to_brand_udf(F.col("search_keyword")))
        .filter(F.col("brand").isNotNull())
        .join(items_slim.select("mule_id", "brand"), "brand", "inner")
        .select("user_id", "mule_id", F.lit(W_SEARCH).cast(DoubleType()).alias("weight"))
    )

    # 판매 이력 sold_out
    sold_sig = (
        sales_df
        .filter(F.lower(F.col("sale_status")) == "sold_out")
        .join(items_slim, F.col("post_id") == F.col("mule_id"), "inner")
        .select("user_id", "mule_id", F.lit(W_SOLD).cast(DoubleType()).alias("weight"))
    )

    # 판매 이력 on_sale / reserved
    active_sig = (
        sales_df
        .filter(F.lower(F.col("sale_status")).isin("on_sale", "reserved"))
        .join(items_slim, F.col("post_id") == F.col("mule_id"), "inner")
        .select("user_id", "mule_id", F.lit(W_ACTIVE).cast(DoubleType()).alias("weight"))
    )

    all_signals = like_sig.union(search_sig).union(sold_sig).union(active_sig)
    logger.info("행동 신호 수집: %d (user, item) 쌍", all_signals.count())
    return all_signals


# ────────────────────────────────────────────────────────────
# 유저 취향 벡터 계산
# ────────────────────────────────────────────────────────────
def compute_user_preferences(user_signals, items_featured):
    """
    유저 취향 벡터 = 행동 신호로 상호작용한 아이템 피처 벡터의 가중평균.
    """
    # 피처 벡터를 Python list로 변환 (collect_list agg 호환)
    items_arr = items_featured.withColumn(
        "feat_arr", to_array_udf(F.col("features"))
    )

    # 신호 × 아이템 피처 join
    sig_feat = (
        user_signals
        .join(items_arr.select("mule_id", "feat_arr"), "mule_id", "inner")
        .select("user_id", "feat_arr", "weight")
    )

    # 유저별 가중평균
    pref_df = (
        sig_feat
        .groupBy("user_id")
        .agg(
            F.collect_list("feat_arr").alias("feat_arrays"),
            F.collect_list("weight").alias("weights"),
        )
        .withColumn("pref_arr", weighted_avg_udf(F.col("feat_arrays"), F.col("weights")))
        .filter(F.col("pref_arr").isNotNull())
        .select("user_id", "pref_arr")
    )
    logger.info("취향 벡터 보유 유저: %d명", pref_df.count())
    return pref_df


# ────────────────────────────────────────────────────────────
# 카테고리 하드 필터 — 유저별 상위 TOP_CAT_N 카테고리 추출
# ────────────────────────────────────────────────────────────
def get_user_top_categories(likes_df, searches_df, sales_df, items_slim):
    """
    유저의 모든 행동(좋아요 + 검색 브랜드 → 카테고리 + 판매)에서
    가장 빈도 높은 TOP_CAT_N개 카테고리를 추출.
    이 카테고리에 속하지 않는 후보 매물은 유사도 계산 전에 제거.
    """
    # 좋아요 → 카테고리
    like_cat = (
        likes_df
        .join(items_slim.select("mule_id","category"),
              F.col("post_id") == F.col("mule_id"), "inner")
        .select("user_id", "category")
    )
    # 검색어 → 브랜드 → 카테고리
    search_cat = (
        searches_df
        .withColumn("brand", keyword_to_brand_udf(F.col("search_keyword")))
        .filter(F.col("brand").isNotNull())
        .join(items_slim.select("brand","category").distinct(), "brand", "inner")
        .select("user_id", "category")
    )
    # 판매 → 카테고리
    sale_cat = (
        sales_df
        .join(items_slim.select("mule_id","category"),
              F.col("post_id") == F.col("mule_id"), "inner")
        .select("user_id", "category")
    )

    # 빈도 집계 → 유저별 rank → 상위 TOP_CAT_N
    w = Window.partitionBy("user_id").orderBy(F.desc("cnt"))
    top_cats = (
        like_cat.union(search_cat).union(sale_cat)
        .groupBy("user_id", "category")
        .agg(F.count("*").alias("cnt"))
        .withColumn("rank", F.rank().over(w))
        .filter(F.col("rank") <= TOP_CAT_N)
        .select("user_id", "category")
    )
    logger.info("카테고리 하드 필터 준비 완료 (유저당 최대 %d카테고리)", TOP_CAT_N)
    return top_cats


# ────────────────────────────────────────────────────────────
# SparkSession
# ────────────────────────────────────────────────────────────
def build_spark():
    return (
        SparkSession.builder
        .appName("MuleRecommender")
        .master("local[*]")
        # HDP 환경: spark-submit --master yarn 으로 오버라이드
        .getOrCreate()
    )


# ────────────────────────────────────────────────────────────
# 메인 추천 파이프라인
# ────────────────────────────────────────────────────────────
def run(spark):
    # ── 1. 아이템 메타데이터 로드 ──────────────────────────────────────────
    logger.info("=== Step 1: 아이템 메타데이터 로드 ===")
    _pdf = pd.read_parquet(PROCESSED_PATH).drop(columns=["crawled_at"], errors="ignore")
    items_df = spark.createDataFrame(_pdf)
    logger.info("아이템 수: %d", items_df.count())

    # ── 2. 행동 로그 로드 (실 CSV 우선) ───────────────────────────────────
    logger.info("=== Step 2: 행동 로그 로드 ===")
    likes_df, searches_df, sales_df = load_logs(spark)

    # ── 3. PySpark ML 피처 파이프라인 ─────────────────────────────────────
    logger.info("=== Step 3: PySpark ML 피처 벡터화 ===")
    items_featured = build_feature_pipeline(items_df)

    # ── 4. pandas 변환 (cross join UDF OOM 우회 → numpy 행렬 연산) ────────
    logger.info("=== Step 4~10: pandas/numpy 추천 계산 ===")

    items_pd = (items_featured
                .select("mule_id", "category", "brand", "is_sold", "features")
                .toPandas())
    items_pd["feat_arr"] = items_pd["features"].apply(lambda v: np.array(v.toArray()))
    items_pd["mule_id"]  = items_pd["mule_id"].astype(str)

    likes_pd    = likes_df.toPandas()
    searches_pd = searches_df.toPandas()
    sales_pd    = sales_df.toPandas()
    logger.info("행동 로그: likes=%d, searches=%d, sales=%d",
                len(likes_pd), len(searches_pd), len(sales_pd))

    # ── 5. 유저 행동 신호 수집 ────────────────────────────────────────────
    feat_map     = items_pd.set_index("mule_id")["feat_arr"].to_dict()
    cat_map      = items_pd.set_index("mule_id")["category"].to_dict()
    # 브랜드별 아이템 최대 200개 샘플 (검색 신호 과다 방지)
    brand_to_ids = (items_pd.groupby("brand")["mule_id"]
                    .apply(lambda x: x.sample(min(len(x), 200), random_state=42).tolist())
                    .to_dict())

    signals = []
    for _, r in likes_pd.iterrows():
        mid = str(r["post_id"])
        if mid in feat_map:
            signals.append((int(r["user_id"]), mid, W_LIKE))

    for _, r in searches_pd.iterrows():
        brand = _keyword_to_brand(r["search_keyword"])
        if brand and brand in brand_to_ids:
            for mid in brand_to_ids[brand]:
                signals.append((int(r["user_id"]), mid, W_SEARCH))

    for _, r in sales_pd.iterrows():
        mid    = str(r["post_id"])
        status = str(r.get("sale_status", "")).lower()
        w      = W_SOLD if status == "sold_out" else (W_ACTIVE if status in ("on_sale","reserved") else None)
        if w and mid in feat_map:
            signals.append((int(r["user_id"]), mid, w))

    if not signals:
        logger.warning("행동 신호 없음 → 추천 불가")
        return

    sig_df = pd.DataFrame(signals, columns=["user_id","mule_id","weight"])
    logger.info("행동 신호: %d 쌍", len(sig_df))

    # ── 6. 유저 취향 벡터 계산 ────────────────────────────────────────────
    user_prefs = {}
    for uid, grp in sig_df.groupby("user_id"):
        vecs, wts = [], []
        for _, r in grp.iterrows():
            feat = feat_map.get(r["mule_id"])
            if feat is not None:
                vecs.append(feat); wts.append(r["weight"])
        if vecs:
            wts_arr = np.array(wts)
            user_prefs[uid] = np.stack(vecs).T @ wts_arr / wts_arr.sum()
    logger.info("취향 벡터 보유 유저: %d명", len(user_prefs))

    # ── 7. 유저 상위 카테고리 추출 ────────────────────────────────────────
    user_top_cats = {}
    for uid, grp in sig_df.groupby("user_id"):
        cat_cnt = grp["mule_id"].map(cat_map).value_counts()
        user_top_cats[uid] = set(cat_cnt.head(TOP_CAT_N).index.tolist())

    # ── 8. 후보 매물 준비 (numpy 행렬) ───────────────────────────────────
    candidates  = items_pd[items_pd["is_sold"] != True].reset_index(drop=True)
    own_pairs   = set(zip(sales_pd["user_id"].astype(int),
                         sales_pd["post_id"].astype(str)))
    feat_matrix = np.stack(candidates["feat_arr"].values)
    norms       = np.linalg.norm(feat_matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1e-10
    feat_norm   = feat_matrix / norms
    logger.info("is_sold 필터 후 후보: %d개", len(candidates))

    # ── 9. 코사인 유사도 + 임계값 + Top-N ────────────────────────────────
    logger.info("=== Step 9: 코사인 유사도 계산 (numpy) ===")
    results = []
    for uid, pref_vec in user_prefs.items():
        top_cats = user_top_cats.get(uid, set())
        cat_mask = candidates["category"].isin(top_cats).values if top_cats \
                   else np.ones(len(candidates), dtype=bool)
        own_mask = ~candidates["mule_id"].apply(
            lambda mid: (uid, mid) in own_pairs).values
        mask = cat_mask & own_mask
        if not mask.any():
            continue

        sub_feat = feat_norm[mask]
        sub_ids  = candidates["mule_id"].values[mask]
        pref_n   = pref_vec / (np.linalg.norm(pref_vec) + 1e-10)
        scores   = sub_feat @ pref_n

        thr_mask = scores >= THRESHOLD
        if not thr_mask.any():
            continue

        top_idx = np.argsort(-scores[thr_mask])[:TOP_N]
        results.append({
            "user_id":              uid,
            "recommended_item_ids": ",".join(sub_ids[thr_mask][top_idx].tolist()),
            "updated_at":           pd.Timestamp.now(),
        })

    logger.info("THRESHOLD(%.1f) 통과: %d명에게 추천 생성", THRESHOLD, len(results))

    if not results:
        logger.warning("추천 결과 없음 (THRESHOLD %.1f 통과 아이템 부족)", THRESHOLD)
        return

    # ── 10. 결과 저장 ─────────────────────────────────────────────────────
    result_df = pd.DataFrame(results)
    logger.info("=== 추천 결과 ===\n%s", result_df.to_string())
    Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    result_df.to_csv(f"{OUTPUT_PATH}/part-0.csv", index=False)
    logger.info("저장 완료: %s | %d명", OUTPUT_PATH, len(result_df))


# ────────────────────────────────────────────────────────────
# 진입점
# ────────────────────────────────────────────────────────────
def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
