# 🎸 악기 메타데이터 및 유저 취향 기반 맞춤형 매물 추천 파이프라인 (Shared DB Batch Architecture)

## 1. 문제 정의 (Problem Definition)

### 1.1 배경 및 동기
* **비즈니스 문제:** 본 서비스는 최근 이펙터 단일 카테고리에서 일렉기타, 베이스, 신스, 음향장비 등 9개 메인 카테고리로 대규모 업데이트를 단행했습니다. 카테고리가 확장됨에 따라 유저가 원하는 매물을 찾는 탐색 비용이 급증했으며, 이는 거래 성사율 저하 및 유저 이탈로 이어질 수 있습니다.
* **기술적 제약 및 데이터 희소성:** 누적 회원 2,400명 규모의 스타트업에서 실시간 추천 API 서버나 상시 구동형 인프라를 구축하는 것은 심각한 오버엔지니어링입니다. 또한, 내부 데이터만으로는 빅데이터 과제 요건(100MB 이상 수집)을 충족하기 어렵습니다.
* **해결 방안:** 외부 대용량 데이터(Mule)로 악기 메타데이터 및 카테고리별 연관 관계를 선행 학습(Knowledge Base)하고, 이를 서비스 내부의 유저 행동 로그(좋아요, 최근 검색어)와 결합하는 **하이브리드 배치 추천 파이프라인**을 구축하여 인프라 비용을 최소화합니다.

### 1.2 데이터 분석 및 추천 목표 (핵심 질문 3가지)
* **Q1. 카테고리/브랜드별 적정가 레인지는 어떻게 형성되어 있는가?**
  * 수집된 악기 데이터의 사분위수(Q1, Median, Q3)를 산출하여 모델별 객관적인 시세 가이드라인을 도출합니다.
* **Q2. 유저의 선호 행동(좋아요, 검색어) 기반 카테고리 가중치는 어떻게 분포하는가?**
  * 내부 유저 행동 로그를 분석하여 9개 카테고리에 대한 유저별 선호도 점수 매트릭스를 계산합니다.
* **Q3. 추천된 매물들의 가격대 및 감가율 분포는 어떠한가?**
  * 콘텐츠 기반 필터링으로 매칭된 추천 매물들이 실제 중고 시장에서 어떤 감가율 추이를 보이는지 시각적으로 검증합니다.

### 1.3 사용 데이터
* **외부 데이터:** 뮬(Mule) 장터 최근 게시글 데이터 (100MB 이상 확보, 악기 메타데이터 및 시세 분석용)
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
* **Visualization:** `Python (Matplotlib, Seaborn)` (카테고리별 가격 분포 및 감가율 추이 시각화)

---

## 3. 구현 계획 (Implementation Plan)

### 3.1 전체 아키텍처 파이프라인

<img width="512" height="454" alt="image" src="https://github.com/user-attachments/assets/fd8b2f68-5750-4ca4-9f07-1002f29af9a3" />


### 3.2 단계별 상세 구현
1. **데이터 수집 및 적재 (`src/ingest/`):** Python 크롤러로 뮬의 9개 카테고리 매물을 수집하여 일별 CSV로 HDFS에 적재합니다. (`time.sleep()`을 통한 윤리적 수집 준수)
2. **Spark 전처리 및 특징 추출 (`src/pipeline/`):** 제각각인 악기 명칭(예: "미펜 텔레", "Fender Telecaster")을 표준 이름으로 정규화하고 정제하여 Parquet 포맷으로 저장합니다.
3. **추천 연산 및 분석 (`src/recommend/`, `src/analyze/`):** PySpark 기반 콘텐츠 기반 필터링을 수행하고, HiveQL을 통해 분석 질문(Q1~Q3)에 대한 통계 데이터를 산출합니다.
4. **결과 시각화 (`src/analyze/`):** `Matplotlib`과 `Seaborn`을 사용하여 카테고리별 시세 분포(Box Plot) 및 추천 데이터 분포 그래프를 생성하여 `results/` 폴더에 저장합니다.
5. **프로덕션 DB 동기화 및 서빙:** 최종 경량화된 `(user_id, recommended_item_ids, updated_at)` 데이터를 PostgreSQL에 적재하고, Spring Boot에서 QueryDSL로 유저에게 초고속 서빙합니다.

---

## 4. GitHub Repository 구조



<img width="595" height="520" alt="image" src="https://github.com/user-attachments/assets/dc7f1ad2-a9c3-4ff6-8a0c-43c90ce5fbaf" />

---

## 5. 실행 방법 (HDP Sandbox & Production Link)

```bash
# 1. 외부 악기 데이터 및 매물 데이터 수집
python src/ingest/crawler.py

# 2. PySpark 추천 연산 및 전처리 실행
spark-submit --master local[*] src/pipeline/spark_cleaner.py
spark-submit --master local[*] src/recommend/recommender_engine.py

# 3. Hive를 통한 데이터 분석 및 검증
hive -f src/analyze/hive_ddl.hql
hive -f src/analyze/validation.sql

# 4. 분석 결과 시각화 차트 생성
python src/analyze/visualize.py

# 5. 분석 결과 데이터를 프로덕션 PostgreSQL로 배치 적재
python src/db_sync/postgres_loader.py
6. AI Tool Usage
Claude: 프로젝트 주제 구체화, 교수님 요구사항 충족을 위한 데이터 분석 아키텍처 및 README 구조 설계

7. 참고 자료
Apache Spark 공식 문서

Apache Hive 공식 문서

뮬(Mule) 중고악기 장터

Python BeautifulSoup 문서
