"""
src/db_sync/postgres_loader.py

Spark 추천 연산 결과를 프로덕션 PostgreSQL 에 UPSERT 적재합니다.

테이블 DDL (자동 생성):
  CREATE TABLE IF NOT EXISTS user_recommendations (
      user_id              INTEGER     PRIMARY KEY,
      recommended_item_ids TEXT        NOT NULL,
      updated_at           TIMESTAMP   NOT NULL
  );

환경변수 설정 (연결 정보):
  MULE_DB_HOST      기본값: localhost
  MULE_DB_PORT      기본값: 5432
  MULE_DB_NAME      기본값: mule_db
  MULE_DB_USER      기본값: postgres
  MULE_DB_PASSWORD  기본값: password

실행:
  python src/db_sync/postgres_loader.py
"""

import os
import glob
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 경로
# ---------------------------------------------------------------------------
RECO_CSV_DIR = "data/sample/user_recommendations_output"
TABLE_NAME   = "user_recommendations"

# ---------------------------------------------------------------------------
# DB 연결 설정 (환경변수 우선, 없으면 개발 기본값)
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host":     os.getenv("MULE_DB_HOST",     "localhost"),
    "port":     int(os.getenv("MULE_DB_PORT", "5432")),
    "dbname":   os.getenv("MULE_DB_NAME",     "mule_db"),
    "user":     os.getenv("MULE_DB_USER",     "postgres"),
    "password": os.getenv("MULE_DB_PASSWORD", "password"),
    "connect_timeout": 10,
}

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    user_id              INTEGER   NOT NULL,
    recommended_item_ids TEXT      NOT NULL,
    updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_user_recommendations PRIMARY KEY (user_id)
);
"""

UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (user_id, recommended_item_ids, updated_at)
VALUES %s
ON CONFLICT (user_id) DO UPDATE SET
    recommended_item_ids = EXCLUDED.recommended_item_ids,
    updated_at           = EXCLUDED.updated_at;
"""


# ---------------------------------------------------------------------------
# 추천 결과 CSV 로드 (Spark part-*.csv 디렉터리 구조 대응)
# ---------------------------------------------------------------------------
def load_recommendations(csv_dir: str) -> pd.DataFrame:
    """
    Spark CSV 출력 디렉터리에서 part-*.csv 파일을 모두 읽어 합칩니다.
    파일 없으면 빈 DataFrame 반환.
    """
    part_files = glob.glob(f"{csv_dir}/part-*.csv")
    if not part_files:
        # 단일 CSV 파일로 저장된 경우도 처리
        single = glob.glob(f"{csv_dir}/*.csv")
        part_files = [f for f in single if "_SUCCESS" not in f]

    if not part_files:
        logger.error("추천 결과 CSV 없음: %s", csv_dir)
        return pd.DataFrame(columns=["user_id","recommended_item_ids","updated_at"])

    dfs = []
    for f in part_files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception as exc:
            logger.warning("파일 읽기 실패 (%s): %s", f, exc)

    if not dfs:
        return pd.DataFrame(columns=["user_id","recommended_item_ids","updated_at"])

    df = pd.concat(dfs, ignore_index=True).dropna(subset=["user_id","recommended_item_ids"])
    df["user_id"] = df["user_id"].astype(int)
    logger.info("추천 결과 로드: %d행", len(df))
    return df


# ---------------------------------------------------------------------------
# PostgreSQL 연결 및 테이블 초기화
# ---------------------------------------------------------------------------
def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("DB 연결 성공: %s:%s/%s",
                    DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
        return conn
    except psycopg2.OperationalError as exc:
        logger.error("DB 연결 실패: %s", exc)
        logger.error("환경변수를 확인하세요: MULE_DB_HOST / MULE_DB_USER / MULE_DB_PASSWORD")
        raise


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    logger.info("테이블 준비 완료: %s", TABLE_NAME)


# ---------------------------------------------------------------------------
# UPSERT 적재
# ---------------------------------------------------------------------------
def upsert_recommendations(conn, df: pd.DataFrame, batch_size: int = 1_000) -> int:
    """
    user_recommendations 테이블에 단일 트랜잭션 UPSERT.

    - 모든 배치가 성공한 뒤 마지막에 commit 1회
    - 어느 배치라도 에러 발생 시 호출부(main)에서 rollback → 전체 무효화
    - ON CONFLICT (user_id) → recommended_item_ids, updated_at 갱신

    반환: 처리된 총 행 수
    """
    if df.empty:
        logger.warning("적재할 데이터 없음")
        return 0

    if "updated_at" not in df.columns:
        df["updated_at"] = pd.Timestamp.now()
    df["updated_at"] = (pd.to_datetime(df["updated_at"], errors="coerce")
                        .fillna(pd.Timestamp.now()))

    records  = list(df[["user_id","recommended_item_ids","updated_at"]]
                    .itertuples(index=False, name=None))
    total    = len(records)
    executed = 0

    with conn.cursor() as cur:
        for start in range(0, total, batch_size):
            batch = records[start:start + batch_size]
            execute_values(cur, UPSERT_SQL, batch)   # 아직 commit 안 함
            executed += len(batch)
            logger.info("  배치 실행(미commit): %d / %d 행", executed, total)

    # 모든 배치 성공 → 단 1회 commit (전체 트랜잭션 완료)
    conn.commit()
    logger.info("UPSERT commit 완료: %d행 → %s", executed, TABLE_NAME)
    return executed


# ---------------------------------------------------------------------------
# 적재 결과 검증
# ---------------------------------------------------------------------------
def verify(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*), MAX(updated_at) FROM {TABLE_NAME};")
        cnt, latest = cur.fetchone()
    logger.info("─" * 50)
    logger.info("검증 결과")
    logger.info("  테이블:      %s", TABLE_NAME)
    logger.info("  총 유저 수:  %s", cnt)
    logger.info("  최신 갱신:   %s", latest)
    logger.info("─" * 50)

    # 샘플 조회
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY updated_at DESC LIMIT 5;")
        rows = cur.fetchall()
    logger.info("최근 5행 샘플:")
    for r in rows:
        logger.info("  user_id=%-4s  items=%-40s  at=%s", r[0], r[1][:40], r[2])


# ---------------------------------------------------------------------------
# 진입점
# ---------------------------------------------------------------------------
def main() -> None:
    # 1. 추천 결과 로드
    reco_df = load_recommendations(RECO_CSV_DIR)
    if reco_df.empty:
        logger.error("적재할 추천 데이터가 없습니다. recommender_engine.py를 먼저 실행하세요.")
        return

    # 2. DB 연결 + 테이블 초기화
    conn = get_connection()
    try:
        ensure_table(conn)

        # 3. UPSERT 적재 (단일 트랜잭션)
        upsert_recommendations(conn, reco_df)

        # 4. 검증
        verify(conn)

    except Exception as exc:
        # 어느 단계에서든 에러 → 전체 Rollback
        conn.rollback()
        logger.error("오류 발생 → 전체 Rollback 수행: %s", exc)
        raise

    finally:
        conn.close()
        logger.info("DB 연결 종료")


if __name__ == "__main__":
    main()
