#!/bin/bash
# =============================================================================
# src/ingest/sqoop_import.sh
#
# PostgreSQL(운영 DB) → HDFS 증분 import
#
# 기술 선택 이유:
#   Sqoop은 RDBMS ↔ HDFS 간 대용량 데이터 이동의 표준 도구.
#   운영 DB(PostgreSQL)에 직접 Spark 쿼리를 날리면 프로덕션 부하가 발생하므로,
#   Sqoop으로 분석용 데이터를 HDFS에 복제한 뒤 Spark/Hive가 읽는 구조를 채택.
#
# 실행 (HDP Sandbox):
#   chmod +x src/ingest/sqoop_import.sh
#   ./src/ingest/sqoop_import.sh
# =============================================================================

set -e

# 환경변수 로드 (.env 파일 사용 시)
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

DB_HOST="${MULE_DB_HOST:-localhost}"
DB_PORT="${MULE_DB_PORT:-5432}"
DB_NAME="${MULE_DB_NAME:-mule_db}"
DB_USER="${MULE_DB_USER:-postgres}"
DB_PASS="${MULE_DB_PASSWORD:-password}"

JDBC_URL="jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}"
HDFS_BASE="/user/hive/warehouse/mule_reco"

echo "[$(date)] Sqoop import 시작: ${DB_HOST}/${DB_NAME} → HDFS ${HDFS_BASE}"

# ------------------------------------------------------------------
# 1. 매물(Post) 테이블 — 현재 판매 중인 매물만
# ------------------------------------------------------------------
echo "[$(date)] post 테이블 import..."
sqoop import \
    --connect "${JDBC_URL}" \
    --username "${DB_USER}" \
    --password "${DB_PASS}" \
    --query "SELECT id, brand, category, price, is_sold, created_at
             FROM post
             WHERE deleted_at IS NULL AND \$CONDITIONS" \
    --target-dir "${HDFS_BASE}/posts" \
    --delete-target-dir \
    --fields-terminated-by ',' \
    --m 1 \
    --as-textfile

# ------------------------------------------------------------------
# 2. 좋아요(PostLike) 테이블 — 증분 (어제 이후)
# ------------------------------------------------------------------
echo "[$(date)] post_like 테이블 증분 import..."
sqoop import \
    --connect "${JDBC_URL}" \
    --username "${DB_USER}" \
    --password "${DB_PASS}" \
    --query "SELECT user_id, post_id, created_at
             FROM post_like
             WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
             AND \$CONDITIONS" \
    --target-dir "${HDFS_BASE}/likes_delta" \
    --delete-target-dir \
    --fields-terminated-by ',' \
    --m 1

# ------------------------------------------------------------------
# 3. 검색어(RecentSearch) 테이블 — 증분
# ------------------------------------------------------------------
echo "[$(date)] recent_search 테이블 증분 import..."
sqoop import \
    --connect "${JDBC_URL}" \
    --username "${DB_USER}" \
    --password "${DB_PASS}" \
    --query "SELECT user_id, search_keyword, created_at
             FROM recent_search
             WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
             AND \$CONDITIONS" \
    --target-dir "${HDFS_BASE}/searches_delta" \
    --delete-target-dir \
    --fields-terminated-by ',' \
    --m 1

# ------------------------------------------------------------------
# 4. 채팅(Chat) 테이블 — 증분
# ------------------------------------------------------------------
echo "[$(date)] chat 테이블 증분 import..."
sqoop import \
    --connect "${JDBC_URL}" \
    --username "${DB_USER}" \
    --password "${DB_PASS}" \
    --query "SELECT sender_id AS user_id, post_id, created_at
             FROM chat_message
             WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
             AND \$CONDITIONS" \
    --target-dir "${HDFS_BASE}/chats_delta" \
    --delete-target-dir \
    --fields-terminated-by ',' \
    --m 1

echo "[$(date)] Sqoop import 완료 → HDFS ${HDFS_BASE}"
