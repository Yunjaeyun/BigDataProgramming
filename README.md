# BigDataProgramming
# 🎸 중고 악기 시세 분석 파이프라인: 적정가 산출 시스템

## 1. 문제 정의 (Problem Definition)

### 1.1 배경 및 동기

중고 악기 시장은 표준화된 가격 지표가 존재하지 않는다. 동일한 모델이라도 상태, 연식, 판매자의 주관에 따라 가격 편차가 크며, 이로 인해 다음과 같은 문제가 발생한다.

- **구매자**: "이 가격이 적정한지" 판단할 객관적 근거가 없어 구매를 망설임
- **판매자**: 시세를 모르고 지나치게 높게 올려 매물이 장기간 미판매 상태로 방치됨
- **결과**: 거래 성사율 하락, 플랫폼 내 유저 이탈 증가

본 프로젝트는 국내 최대 악기 커뮤니티 **뮬(Mule)**의 중고 장터 데이터를 대규모로 수집·분석하여, 브랜드/모델별 **적정가 레인지**와 **꿀매물 판별 기준**을 도출하는 빅데이터 파이프라인을 구축한다.

### 1.2 분석 목표 (핵심 질문 3가지)

1. **브랜드/모델별 적정가 레인지는?**
   - 사분위(Q1~Q3) 기반 가격 분포를 산출하여 "Boss DD-7 적정가: 9~12만원" 형태의 시세 정보 제공
2. **판매 완료 매물 vs 미판매 매물의 가격 차이는?**
   - 실제 거래가 성사된 매물의 가격대를 분석하여 "적정가 이하 매물이 평균 몇 일 내 판매 완료되는가" 검증
3. **카테고리별 시세 변동 추이와 감가율은?**
   - 이펙터, 일렉기타, 앰프 등 카테고리별 월간 시세 추이를 분석하고, 특정 모델의 시간 경과에 따른 감가율 산출

### 1.3 사용 데이터

| 항목 | 내용 |
|------|------|
| **데이터 출처** | 뮬(mule.co.kr) 중고 장터 웹 크롤링 |
| **수집 대상** | 게시글 제목, 판매 가격, 브랜드, 모델명, 악기 카테고리, 상태(A/B/C급), 게시 일시, 판매 완료 여부, 거래 지역 |
| **수집 범위** | 최근 6개월 ~ 1년치 장터 게시글 (전 카테고리: 이펙터, 일렉기타, 통기타, 베이스, 앰프, 드럼, 건반, 음향장비 등) |
| **목표 용량** | 누적 100MB 이상 (일별 크롤링 스크립트를 통해 분할 수집) |
| **데이터 포맷** | CSV (일별 파일 분할 저장) |

---

## 2. 기술 스택 (Tech Stack)

| 단계 | 기술 | 선정 이유 |
|------|------|-----------|
| **데이터 수집** | Python (BeautifulSoup, Requests), Crontab | 뮬 장터 HTML 파싱, 일별 자동 수집 스케줄링 |
| **데이터 적재** | HDFS | Raw 데이터 보존 및 분산 저장소 활용 |
| **데이터 전처리** | Apache Spark (PySpark) | 비정형 텍스트(브랜드명 정규화: "펜더"/"Fender"/"미펜" → "Fender") 처리, 가격 파싱, 결측치 제거 등 대량 데이터 정제에 적합 |
| **데이터 분석** | Hive + Spark SQL | 시계열 시세 쿼리, 사분위 가격 산출, 판매 완료/미완료 비교 분석 |
| **시각화** | Python (Matplotlib, Seaborn) | 브랜드별 시세 분포 차트, 카테고리별 추이 그래프, 감가율 시각화 |

### 핵심 기술 스택 조합: **Spark + Hive** (가산점 요건 충족)

- **Spark**: 비정형 텍스트 정제 및 Feature 추출 (제목에서 브랜드/모델/상태 파싱)
- **Hive**: 정제된 데이터를 파티셔닝된 테이블로 관리하고, SQL 기반 집계 분석 수행

---

## 3. 구현 계획 (Implementation Plan)

### 3.1 전체 파이프라인

```
[뮬 장터] → [Python Crawler] → [CSV (일별)] → [HDFS /raw]
                                                    ↓
                                          [Spark 전처리/정규화]
                                                    ↓
                                          [HDFS /processed (Parquet)]
                                                    ↓
                                          [Hive External Table]
                                                    ↓
                                    [Spark SQL / HiveQL 분석]
                                                    ↓
                                    [시각화 (Matplotlib/Seaborn)]
                                                    ↓
                                    [적정가 레인지 결과 테이블]
```

### 3.2 단계별 상세

**① 데이터 수집 (src/ingest/)**
- Python 스크립트로 뮬 장터 페이지를 카테고리별로 크롤링
- 일별 CSV 파일로 저장 (예: `mule_2025-05-14_effects.csv`)
- `time.sleep()`을 적용하여 서버 부하 최소화 (윤리적 수집)
- Crontab 또는 Shell Script로 일 1회 자동 실행
- 재실행 가능하도록 스크립트화

**② 데이터 적재 (HDFS)**
- 수집된 CSV 파일을 `hdfs dfs -put`으로 HDFS `/user/data/mule/raw/` 경로에 적재
- 일자별 디렉토리 분할 저장 (예: `/user/data/mule/raw/2025-05-14/`)

**③ 데이터 전처리 (src/pipeline/)**
- Spark DataFrame을 활용한 데이터 정제:
  - 제목에서 브랜드/모델명 키워드 추출 (정규표현식 + 브랜드 사전 매핑)
  - 브랜드명 정규화 ("펜더", "fender", "미펜", "Fender" → "Fender")
  - 가격 문자열 파싱 ("12만원", "120,000원", "12만" → 120000)
  - 판매 완료 여부 판별 ("판매완료", "sold" 등 키워드 탐지)
  - 결측치 및 이상치(가격 0원, 비정상 매물) 제거
- 정제된 데이터를 Parquet 포맷으로 HDFS `/user/data/mule/processed/`에 저장

**④ 데이터 분석 (src/analyze/)**
- Hive External Table 생성 (Parquet 기반, 카테고리별 파티셔닝)
- 분석 쿼리 수행:
  - Q1: `GROUP BY brand, model` → 사분위(Q1, 중앙값, Q3) 가격 산출 → 적정가 레인지
  - Q2: `판매완료 = true vs false` → 가격대별 판매율 비교, 평균 게시 기간 분석
  - Q3: `GROUP BY category, month` → 월별 평균 시세 추이, 감가율 계산

**⑤ 시각화 및 결과 정리**
- 브랜드별 가격 분포 (Box Plot)
- 카테고리별 월간 시세 추이 (Line Chart)
- 판매 완료 vs 미판매 가격 분포 비교 (Histogram)
- 감가율 상위/하위 모델 Top 10

### 3.3 서비스 연계 (확장 방향)

본 프로젝트의 분석 결과는 현재 운영 중인 중고 악기 거래 앱 서비스에 다음과 같이 활용할 수 있다:

- 매물 등록 시: "비슷한 매물 평균가: 9~12만원 (현재 가격은 상위 20%)" 안내
- 매물 조회 시: "이 매물은 평균 시세 대비 20% 저렴합니다" 꿀매물 라벨 표시
- 분석 결과 테이블(CSV/SQL)만 서비스 DB에 주기적으로 반영하는 배치 방식으로, 추가 인프라 비용 없이 적용 가능

---

## 4. GitHub Repository 구조

```
mule-instrument-price-pipeline/
├── README.md                    # 프로젝트 개요, 실행 방법, 결과 요약
├── data/
│   ├── README.md                # 데이터 출처, 스키마 설명
│   └── sample/                  # 샘플 데이터 (100~1000줄)
├── src/
│   ├── ingest/                  # 데이터 수집 스크립트
│   │   ├── crawler.py           # 뮬 장터 크롤러
│   │   ├── run_crawl.sh         # 크롤링 자동화 쉘 스크립트
│   │   └── brand_dict.json      # 브랜드명 매핑 사전
│   ├── pipeline/                # 전처리 코드
│   │   ├── preprocess.py        # Spark 전처리 (정규화, 파싱)
│   │   └── load_to_hdfs.sh      # HDFS 적재 스크립트
│   └── analyze/                 # 분석 코드
│       ├── hive_ddl.hql         # Hive 테이블 생성 DDL
│       ├── analysis_queries.hql # 핵심 분석 쿼리 (Q1~Q3)
│       └── visualize.py         # 시각화 스크립트
├── results/                     # 분석 결과 그래프 및 요약
└── .gitignore                   # raw 데이터 제외
```

---

## 5. 실행 방법 (HDP Sandbox 기준)

```bash
# 1. 데이터 수집
cd src/ingest/
python crawler.py --category all --days 180
bash run_crawl.sh

# 2. HDFS 적재
bash src/pipeline/load_to_hdfs.sh

# 3. Spark 전처리
spark-submit src/pipeline/preprocess.py

# 4. Hive 테이블 생성 및 분석
hive -f src/analyze/hive_ddl.hql
hive -f src/analyze/analysis_queries.hql

# 5. 시각화
python src/analyze/visualize.py
```

상세 실행 가이드는 프로젝트 진행 중 업데이트 예정.

---

## 6. AI Tool Usage

- **Claude**: 프로젝트 주제 구체화 및 README 구조 설계 도움, 기술 스택 선정 시 비용 구조 조사

---

## 7. 참고 자료

- [Apache Spark 공식 문서](https://spark.apache.org/docs/latest/)
- [Apache Hive 공식 문서](https://hive.apache.org/)
- [뮬(Mule) 중고악기 장터](https://mule.co.kr/)
- [Python BeautifulSoup 문서](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
