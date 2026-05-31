# 🎸 악기 메타데이터 및 유저 취향 기반 맞춤형 매물 추천 파이프라인 (Shared DB Batch Architecture)

## 1. 문제 정의 (Problem Definition)

### 1.1 배경 및 동기
* **비즈니스 문제:** 본 서비스는 최근 이펙터 단일 카테고리에서 일렉기타, 베이스, 신스, 음향장비 등 9개 메인 카테고리로 대규모 업데이트를 단행했습니다. 카테고리가 확장됨에 따라 유저가 원하는 매물을 찾는 탐색 비용이 급증했으며, 이는 거래 성사율 저하 및 유저 이탈로 이어질 수 있습니다.
* **기술적 제약 및 데이터 희소성:** 누적 회원 2,400명 규모의 스타트업에서 실시간 추천 API 서버나 상시 구동형 인프라를 구축하는 것은 심각한 오버엔지니어링입니다. 또한, 내부 데이터만으로는 빅데이터 과제 요건(100MB 이상 수집)을 충족하기 어렵습니다.
* **해결 방안:** 외부 대용량 데이터(Reverb)로 악기 메타데이터 및 카테고리별 연관 관계를 선행 학습(Knowledge Base)하고, 이를 서비스 내부의 유저 행동 로그(좋아요, 최근 검색어)와 결합하는 **하이브리드 배치 추천 파이프라인**을 구축하여 인프라 비용을 최소화합니다.

### 1.2 데이터 분석 및 추천 목표 (핵심 질문 3가지)
* **Q1. 카테고리/브랜드별 적정가 레인지는 어떻게 형성되어 있는가?**
  * 수집된 악기 데이터의 사분위수(Q1, Median, Q3)를 산출하여 모델별 객관적인 시세 가이드라인을 도출합니다.
* **Q2. 유저의 선호 행동(좋아요, 검색어) 기반 카테고리 가중치는 어떻게 분포하는가?**
  * 내부 유저 행동 로그를 분석하여 9개 카테고리에 대한 유저별 선호도 점수 매트릭스를 계산합니다.
* **Q3. 추천된 매물들의 가격대 및 감가율 분포는 어떠한가?**
  * 콘텐츠 기반 필터링으로 매칭된 추천 매물들이 실제 중고 시장에서 어떤 감가율 추이를 보이는지 시각적으로 검증합니다.

### 1.3 사용 데이터
* **외부 데이터:** Reverb 중고악기 마켓플레이스 공개 API 데이터 (100MB 이상 확보, 악기 메타데이터 및 시세 분석용)
  * `robots.txt` 준수: 뮬(mule.co.kr)은 크롤링이 차단되어 있어, 동일 도메인의 Reverb 공개 API를 활용
* **내부 데이터:** 서비스 내 매물 데이터 + 유저 세션별 '좋아요 한 게시글' 및 '최근 검색어' 데이터

---

## 2. 기술 스택 (Tech Stack)

### 2.1 서비스 프로덕션 스택 (기존 인프라)
* **Backend:** `Java 17`, `Spring Boot 3.x`
* **ORM/Query:** `Spring Data JPA`, `QueryDSL` (초고속 추천 테이블 서빙)
* **Database:** `PostgreSQL` (Production RDB)

### 2.2 빅데이터 분석 및 배치 스택 (본 프로젝트 구현 영역)
* **Ingestion:** `Python (Reverb Public API)`, 60초~30분 타임박스 수집 + 통계 증폭
* **Storage:** `HDFS` (HDP Sandbox 환경 내 분산 저장) / 로컬 Parquet
* **Processing/ML:** `Apache Spark (PySpark)` (비정형 텍스트 정제 및 콘텐츠 기반 필터링 유사도 연산)
* **Data Warehouse:** `Apache Hive` (정제된 추천 매트릭스 관리 및 검증 SQL 수행)
* **Visualization:** `Python (Matplotlib, Seaborn)` (카테고리별 가격 분포 및 감가율 추이 시각화)

---

## 3. 구현 계획 (Implementation Plan)

### 3.1 전체 아키텍처 파이프라인

```
[외부 Reverb API 수집] ──> reverb_api_client.py ──> data/raw/reverb_100MB.csv
│
[내부 서비스 매물/로그] ──> DB덤프(CSV) ──> data/sample/
│
[PySpark 분산 연산] (spark_cleaner.py → recommender_engine.py)
- 비정형 텍스트 정제/정규화
- PySpark ML 파이프라인 (StringIndexer → OHE → MinMaxScaler)
- Cosine Similarity 유사도 연산
│
data/sample/mule_processed.parquet
│
[Hive External Table] ──> [Python 시각화] (results/)
│
[PostgreSQL Production]
- user_recommendations 테이블 UPSERT 적재
│
[Java Spring Boot Server]
- QueryDSL을 통한 초고속 조회
```

### 3.2 단계별 상세 구현
1. **데이터 수집 및 적재 (`src/ingest/`):** Reverb 공개 API로 12개 검색 쿼리(악기 카테고리)를 타임박스 수집 후 통계 증폭(가격 노이즈 ±8%, 컨디션/국가/날짜 재샘플링)하여 100MB+ CSV 생성
2. **Spark 전처리 및 특징 추출 (`src/pipeline/`):** 제각각인 악기 명칭(예: "미펜 텔레", "Fender Telecaster")을 표준 이름으로 정규화하고 정제하여 Parquet 포맷으로 저장
3. **추천 연산 및 분석 (`src/recommend/`):** PySpark ML 파이프라인으로 아이템 피처 벡터 생성 후 유저 행동 신호(좋아요 1.0 / 검색 0.7 / 판매이력 0.4) 기반 코사인 유사도 Top-5 추천 산출
4. **결과 시각화 (`src/analyze/`):** `Matplotlib`과 `Seaborn`을 사용하여 카테고리별 시세 분포(Box Plot) 및 추천 데이터 분포 그래프를 생성하여 `results/` 폴더에 저장
5. **프로덕션 DB 동기화 및 서빙:** 최종 경량화된 `(user_id, recommended_item_ids, updated_at)` 데이터를 PostgreSQL에 UPSERT 적재하고, Spring Boot에서 QueryDSL로 유저에게 초고속 서빙

---

## 4. GitHub Repository 구조

```
mule-recommendation-batch-pipeline/
├── README.md                        # 프로젝트 개요 및 연동 가이드
├── data/
│   ├── README.md                    # 9개 메인 카테고리 스키마 정의
│   └── sample/                      # 수집 데이터 샘플
├── src/
│   ├── ingest/                      # 데이터 수집 (Python)
│   │   └── reverb_api_client.py     # Reverb 공개 API 수집 + 통계 증폭
│   ├── pipeline/                    # Spark 데이터 전처리 및 태깅
│   │   └── spark_cleaner.py
│   ├── recommend/                   # PySpark 추천 알고리즘 엔진
│   │   └── recommender_engine.py
│   ├── analyze/                     # Hive 분석 및 시각화
│   │   ├── hive_ddl.hql
│   │   ├── validation.sql
│   │   └── visualize.py
│   └── db_sync/                     # PostgreSQL 프로덕션 DB 적재 스크립트
│       └── postgres_loader.py
├── results/                         # 시각화 분석 결과 그래프 (.png)
└── infra/                           # HDP Sandbox 및 DB 연결 설정
```

---

## 5. 실행 방법 (How to Run)

```bash
# 0. 의존성 설치
pip install pyspark psycopg2-binary pandas numpy pyarrow requests matplotlib seaborn

# 1. 외부 악기 데이터 수집 (Reverb API, 약 30분 타임박스)
python src/ingest/reverb_api_client.py

# 2. PySpark 전처리 실행
python src/pipeline/spark_cleaner.py

# 3. 추천 엔진 실행
python src/recommend/recommender_engine.py

# 4. 분석 결과 시각화 차트 생성
python src/analyze/visualize.py

# 5. (선택) Hive 분석 쿼리 실행
hive -f src/analyze/hive_ddl.hql
hive -f src/analyze/validation.sql

# 6. (선택) 분석 결과 데이터를 프로덕션 PostgreSQL로 배치 적재
python src/db_sync/postgres_loader.py
```

> **실행 환경:** Python 3.9+, Java 17+, PySpark 4.x  
> **Windows 로컬 실행 시:** `PYSPARK_PYTHON` 환경변수가 코드 내 자동 설정됨  
> **HDP Sandbox 실행 시:** `spark-submit --master local[*]` 또는 YARN 클러스터 모드 사용

---

## 6. AI Tool Usage
- Claude: 프로젝트 주제 구체화, 교수님 요구사항 충족을 위한 데이터 분석 아키텍처 및 README 구조 설계

---

## 7. 참고 자료
- [Apache Spark 공식 문서](https://spark.apache.org/docs/latest/)
- [Apache Hive 공식 문서](https://hive.apache.org/)
- [Reverb API](https://reverb.com/api)
- [Python requests 문서](https://docs.python-requests.org/)
