
# 🎸 악기 메타데이터 및 유저 취향 기반 맞춤형 매물 추천 파이프라인 (Shared DB Batch Architecture)

## 1. 문제 정의 (Problem Definition)

### 1.1 배경 및 동기
* **비즈니스 문제:** 본 서비스는 최근 이펙터 단일 카테고리에서 일렉기타, 베이스, 신스, 음향장비 등 9개 메인 카테고리로 대규모 업데이트를 단행했습니다. 카테고리가 확장됨에 따라 유저가 원하는 매물을 찾는 탐색 비용이 급증했으며, 이는 거래 성사율 저하 및 유저 이탈로 이어질 수 있습니다.
* **기술적 제약 (Lean Startup):** 누적 회원 2,400명 규모의 스타트업에서 실시간 추천 API 서버(Python 계열)나 상시 구동형 인프라(Elasticsearch 등)를 구축하는 것은 심각한 오버엔지니어링이며 고정 비용 부담을 초래합니다. 또한, 현재 내부 DB 데이터만으로는 빅데이터 과제 요건(100MB 이상 수집)을 충족하기 어렵습니다.
* **해결 방안:** 서비스 초기 단계의 데이터 희소성(Data Sparsity) 및 Cold Start 문제를 해결하기 위해, 외부 대용량 데이터(Mule)로 콘텐츠 기반 유사도 매트릭스를 선행 학습(Knowledge Base)합니다. 이를 서비스 내부의 유저 행동 로그(좋아요 한 게시글, 최근 검색어)와 결합하는 **하이브리드 배치 추천 파이프라인**을 구축합니다.

### 1.2 분석 및 추천 목표 (핵심 질문 3가지)
* **Q1. 카테고리/태그 기반 유사 매물 매칭:** 유저가 특정 악기(예: 101 오버드라이브)를 볼 때, 메타데이터 유사도(브랜드, 기능, 가격대)가 높은 다른 연관 매물을 정확히 추천할 수 있는가?
* **Q2. 유저 취향 선호도 매트릭스 산출:** 외부 정제 데이터와 내부 프로덕션 DB의 유저 행동 로그(`user_id`별 좋아요, 최근 검색어)를 결합하여 유저별 맞춤형 추천 스코어를 배치로 계산할 수 있는가?
* **Q3. 프로덕션 시스템과의 저비용 연동:** 파이썬(Python) 기반 빅데이터 연산 결과물과 기존 자바(Java) 기반 프로덕션 서버를 추가 인프라 비용 없이 안정적으로 결합할 수 있는가?

### 1.3 사용 데이터
* **외부 데이터:** 뮬(Mule) 장터 최근 1년치 게시글 데이터 (100MB 이상, 악기 메타데이터 추출용)
* **내부 데이터:** 서비스 내 매물 데이터 + 유저 세션별 '좋아요 한 게시글' 및 '최근 검색어' 데이터

---

## 2. 기술 스택 (Tech Stack)

### 2.1 서비스 프로덕션 스택 (기존 인프라)
* **Backend:** `Java 17`, `Spring Boot 3.x`
* **ORM/Query:** `Spring Data JPA`, `QueryDSL` (초고속 추천 테이블 서빙)
* **Database:** `PostgreSQL` (Production RDB)

### 2.2 빅데이터 분석 및 배치 스택 (본 프로젝트 구현 영역)
* **Ingestion:** `Python (BeautifulSoup)`, `Crontab`
* **Storage:** `HDFS` (HDP Sandbox 환경 내 분산 저장)
* **Processing/ML:** `Apache Spark (PySpark)` (비정형 텍스트 정제 및 콘텐츠 기반 필터링 유사도 연산)
* **Data Warehouse:** `Apache Hive` (정제된 추천 매트릭스 관리 및 검증 SQL 수행)

> **핵심 아키텍처 포인트 (Shared DB Batch):** 24시간 구동되는 파이썬 API 서버 대신, Spark가 새벽 시간에 연산을 완료한 뒤 결과 테이블(`user_recommendation`)을 프로덕션 `PostgreSQL`에 직접 `INSERT/UPDATE`만 하고 종료되는 구조를 채택하여 인프라 비용을 **0원**으로 통제합니다.

---

## 3. 구현 계획 (Implementation Plan)

### 3.1 전체 아키텍처 파이프라인

[외부 뮬 데이터 수집] ──> Python Crawler ──> HDFS (/raw)
│
[내부 서비스 매물/로그] ──> DB덤프(CSV)  ──> HDFS (/app_data)
│
[PySpark 분산 연산]
- 비정형 텍스트 정제/정규화
- Cosine Similarity 유사도 연산
│
HDFS (/processed)
│
[Hive External Table]
│
[PostgreSQL Production]
- user_recommendation 테이블 적재
│
[Java Spring Boot Server]
- QueryDSL을 통한 초고속 조회


### 3.2 단계별 상세 구현
1. **데이터 수집 및 적재 (`src/ingest/`):** Python을 통해 뮬의 9개 카테고리 매물 명세와 텍스트를 크롤링하여 일별 CSV로 HDFS에 적재합니다. (`time.sleep()`을 적용하여 윤리적 수집 준수)
2. **Spark 기반 텍스트 정제 및 특징 추출 (`src/pipeline/`):** 제각각인 악기 명칭(예: "미펜 텔레", "Fender Telecaster")을 표준 이름으로 정규화하고, 정규표현식을 통해 픽업/기능별 태그 벡터를 생성합니다.
3. **추천 매트릭스 연산 (`src/recommend/`):** PySpark를 통해 유저-아이템 간 콘텐츠 기반 필터링 알고리즘을 수행하고, 내부 PostgreSQL에서 가져온 유저별 '좋아요' 및 '최근 검색어' 가중치를 결합하여 최종 추천 리스트를 도출합니다.
4. **프로덕션 DB 동기화 (`src/db_sync/`):** 연산 완료 후 오직 `(user_id, recommended_item_ids, updated_at)` 형태의 정제된 경량 결과물만 추출하여 프로덕션 PostgreSQL DB로 직접 적재합니다.
5. **Spring Boot 서빙:** 스프링 백엔드에서는 복잡한 Python 연산 없이, 이미 계산되어 저장된 PostgreSQL 테이블을 QueryDSL로 단순 select 하여 유저 홈 화면에 고성능으로 뿌려줍니다.

---

## 4. GitHub Repository 구조

mule-recommendation-batch-pipeline/
├── README.md                    # 프로젝트 개요 및 연동 가이드
├── data/
│   ├── README.md                # 9개 메인 카테고리 스키마 정의
│   └── sample/                  # 수집 데이터 샘플
├── src/
│   ├── ingest/                  # 데이터 수집 (Python)
│   │   └── crawler.py
│   ├── pipeline/                # Spark 데이터 전처리 및 태깅
│   │   └── spark_cleaner.py
│   ├── recommend/               # PySpark 추천 알고리즘 엔진
│   │   └── recommender_engine.py
│   ├── analyze/                 # Hive 기반 분석 및 데이터 검증
│   │   ├── hive_ddl.hql
│   │   └── validation.sql
│   └── db_sync/                 # PostgreSQL 프로덕션 DB 적재 스크립트
│       └── postgres_loader.py
└── infra/                       # HDP Sandbox 및 DB 연결 설정


---


## 5. 실행 방법 (HDP Sandbox & Production Link)

```bash
# 1. 외부 악기 데이터 및 매물 데이터 수집
python src/ingest/crawler.py

# 2. PySpark 추천 연산 실행 (오프라인 배치 가동)
spark-submit --master local[*] src/recommend/recommender_engine.py

# 3. Hive를 통한 추천 정합성 및 분포 검증
hive -f src/analyze/validation.sql

# 4. 분석 결과 데이터를 프로덕션 PostgreSQL로 배치 적재
python src/db_sync/postgres_loader.py



## 6. AI Tool Usage
Claude: 프로젝트 주제 구체화 및 README 구조 설계 도움, 기술 스택 선정 시 비용 구조 조사

7. 참고 자료
Apache Spark 공식 문서

Apache Hive 공식 문서

뮬(Mule) 중고악기 장터

Python BeautifulSoup 문서
