"""
src/ingest/reverb_api_client.py

Phase 1 — 60초 타임박싱 시드 수집
    Reverb 공개 API (robots.txt 허용) 를 소수점 Jitter 간격으로 호출.
    API 키 미발급 시 Mock 데이터 Fallback 자동 전환.

Phase 2 — 40만행 통계적 증폭 + USD→KRW 환산
    수집된 시드를 Pandas 변형 기법으로 data/raw/reverb_100MB.csv 저장.
"""

import time, random, logging, requests
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------
TIMEBOX_SECONDS = 1800          # Phase 1 타임박스 (초) — 30분
USD_TO_KRW      = 1_400         # 환율 고정값 (프로젝트 기준일)
PER_PAGE        = 50
TARGET_ROWS     = 600_000
OUTPUT_PATH     = Path("data/raw/reverb_100MB.csv")

JITTER_MIN  = 0.53
JITTER_MAX  = 1.41

REVERB_HEADERS = {
    "Accept":         "application/hal+json",
    "Accept-Version": "3.0",
    "Accept-Language":"en-US,en;q=0.9",
    "User-Agent":     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # "Authorization": "Bearer <YOUR_TOKEN>",  # 토큰 있으면 주석 해제
}

SEED_QUERIES = [
    "electric guitar", "bass guitar", "effects pedal", "synthesizer",
    "amplifier", "acoustic guitar", "drum", "keyboard",
    "mixer", "audio interface", "distortion pedal", "reverb pedal",
]

CONDITIONS      = ["mint", "excellent", "very-good", "good", "fair"]
COND_WEIGHTS    = [0.08,   0.28,        0.34,        0.22,   0.08 ]
COUNTRIES       = ["US",  "KR",  "JP",  "DE",  "GB",  "AU",  "CA",  "FR"]
COUNTRY_WEIGHTS = [0.45,  0.20,  0.12,  0.07,  0.06,  0.04,  0.04,  0.02]

# 카테고리별 KRW 시세 (증폭 다양성용)
CATEGORY_KRW_RANGE = {
    "electric guitar":  (200_000, 3_000_000),
    "bass guitar":      (200_000, 2_500_000),
    "effects pedal":    (50_000,  500_000),
    "distortion pedal": (40_000,  400_000),
    "reverb pedal":     (80_000,  600_000),
    "synthesizer":      (300_000, 2_000_000),
    "amplifier":        (200_000, 3_000_000),
    "acoustic guitar":  (150_000, 2_000_000),
    "drum":             (500_000, 5_000_000),
    "keyboard":         (200_000, 1_500_000),
    "mixer":            (100_000, 1_000_000),
    "audio interface":  (80_000,  800_000),
}

MAKES = ["Fender","Gibson","Boss","Ibanez","Marshall","Eventide","Strymon",
         "MXR","Electro-Harmonix","TC Electronic","Kemper","Roland","Yamaha"]


# ---------------------------------------------------------------------------
# Phase 1 — 실 API 수집
# ---------------------------------------------------------------------------
def _jitter() -> None:
    time.sleep(round(random.uniform(JITTER_MIN, JITTER_MAX), 4))


def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(REVERB_HEADERS)
    return s


def _normalize(item: dict, query: str) -> dict:
    price_info = item.get("price") or {}
    condition  = item.get("condition") or {}
    origin     = ((item.get("shipping") or {}).get("origin") or {})
    price_usd  = float(price_info.get("amount") or 0)
    return {
        "listing_id":   str(item.get("id", "")),
        "title":        item.get("title", ""),
        "make":         item.get("make", "Unknown"),
        "model":        item.get("model", ""),
        "year":         str(item.get("year", "")),
        "condition":    condition.get("slug", ""),
        "price_usd":    round(price_usd, 2),
        "price_krw":    int(price_usd * USD_TO_KRW),
        "currency":     price_info.get("currency", "USD"),
        "category":     query,
        "country_code": origin.get("country_code", "US"),
        "created_at":   item.get("created_at", ""),
        "is_sold":      str(item.get("state", "") == "sold"),
    }


def _fetch_page(session: requests.Session, query: str, page: int) -> list[dict]:
    params = {"query": query, "per_page": PER_PAGE, "page": page, "currency": "USD"}
    for attempt in range(1, 4):
        try:
            resp = session.get("https://api.reverb.com/api/listings",
                               params=params, timeout=10)
            if resp.status_code == 404:
                return []
            if resp.status_code == 429:
                wait = round(random.uniform(2.31, 5.17), 4)
                logger.warning("429 Rate-limit → %.4f초 대기", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return [_normalize(i, query) for i in resp.json().get("listings", [])]
        except requests.RequestException as exc:
            wait = round(random.uniform(1.83, 3.61), 4)
            logger.warning("요청 실패 (시도 %d) %.4f초 후 재시도: %s", attempt, wait, exc)
            time.sleep(wait)
    return []


def collect_seed(timebox_sec: int = TIMEBOX_SECONDS) -> pd.DataFrame:
    """60초 타임박스 내 Reverb API 시드 수집. 실패 시 Mock 자동 전환."""
    session  = _build_session()
    records:  list[dict] = []
    deadline = time.monotonic() + timebox_sec
    q_idx, page = 0, 1

    logger.info("─" * 55)
    logger.info("Phase 1 | 타임박스 %d초 | Reverb API 시드 수집 시작", timebox_sec)

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            logger.info("⏱  %d초 만료 → 루프 탈출", timebox_sec)
            break

        query   = SEED_QUERIES[q_idx % len(SEED_QUERIES)]
        elapsed = timebox_sec - remaining
        logger.info("[경과 %5.1fs | 잔여 %5.1fs]  %-20s p.%d  (누계 %d건)",
                    elapsed, remaining, query, page, len(records))

        batch = _fetch_page(session, query, page)
        if not batch:
            q_idx += 1; page = 1
        else:
            records.extend(batch)
            logger.info("  └─ %d건 수신", len(batch))
            page += 1

        if page > 3:
            q_idx += 1; page = 1

        if deadline - time.monotonic() < JITTER_MIN:
            break
        _jitter()

    logger.info("Phase 1 완료 | 시드 %d건 확보", len(records))

    if not records:
        logger.warning("API 무응답 → Mock 데이터 Fallback 전환")
        return _make_mock_data(200)

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Mock Fallback (API 키 미발급 대비)
# ---------------------------------------------------------------------------
def _make_mock_data(n: int = 200) -> pd.DataFrame:
    """
    API 키 발급 지연 또는 네트워크 불가 시 사용하는 Mock 시드 데이터.
    실제 Reverb 응답과 동일한 컬럼 구조를 갖습니다.
    """
    np.random.seed(0)
    rows = []
    for i in range(n):
        query     = random.choice(SEED_QUERIES)
        lo, hi    = CATEGORY_KRW_RANGE.get(query, (100_000, 1_000_000))
        price_krw = int(np.random.triangular(lo, (lo + hi) / 2, hi))
        price_usd = round(price_krw / USD_TO_KRW, 2)
        rows.append({
            "listing_id":   f"mock_{i}",
            "title":        f"{random.choice(MAKES)} {query.title()} #{i}",
            "make":         random.choice(MAKES),
            "model":        f"Model-{i % 30}",
            "year":         str(random.randint(2010, 2023)),
            "condition":    np.random.choice(CONDITIONS, p=COND_WEIGHTS),
            "price_usd":    price_usd,
            "price_krw":    price_krw,
            "currency":     "USD",
            "category":     query,
            "country_code": np.random.choice(COUNTRIES, p=COUNTRY_WEIGHTS),
            "created_at":   "",
            "is_sold":      str(random.random() < 0.12),
        })
    logger.info("Mock 데이터 %d건 생성 완료", n)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Phase 2 — 40만행 통계적 증폭
# ---------------------------------------------------------------------------
def augment(seed_df: pd.DataFrame, target: int = TARGET_ROWS) -> pd.DataFrame:
    """
    5가지 통계적 변형으로 시드를 target 건수까지 증폭.
      1. 가격 노이즈    정규분포 ±12% (KRW/USD 연동 유지)
      2. 컨디션         실제 중고 시장 분포 재샘플링
      3. 등록일         최근 365일 무작위 분산
      4. 국가           글로벌 거래 분포 재샘플링
      5. is_sold        판매 완료율 12% 재샘플링
    """
    np.random.seed(42)
    seed_n = len(seed_df)
    logger.info("─" * 55)
    logger.info("Phase 2 | 시드 %d건 → 목표 %d건 증폭 시작", seed_n, target)

    df = seed_df.sample(n=target, replace=True, random_state=42).reset_index(drop=True)
    df["price_krw"] = pd.to_numeric(df["price_krw"], errors="coerce").fillna(100_000)
    df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce").fillna(71.0)

    # 1. 가격 노이즈
    noise = np.clip(np.random.normal(1.0, 0.08, target), 0.72, 1.30)
    df["price_krw"] = (df["price_krw"] * noise).round(-2).clip(lower=10_000).astype(int)
    df["price_usd"] = (df["price_krw"] / USD_TO_KRW).round(2)

    # 2. 컨디션
    df["condition"] = np.random.choice(CONDITIONS, size=target, p=COND_WEIGHTS)

    # 3. 등록일
    base = datetime(2023, 1, 1)
    df["created_at"] = [
        (base + timedelta(days=int(d))).strftime("%Y-%m-%dT%H:%M:%SZ")
        for d in np.random.randint(0, 365, size=target)
    ]

    # 4. 국가
    df["country_code"] = np.random.choice(COUNTRIES, size=target, p=COUNTRY_WEIGHTS)

    # 5. is_sold
    df["is_sold"] = np.random.choice(["True","False"], size=target, p=[0.12, 0.88])

    # listing_id 고유화
    df["listing_id"] = df["listing_id"].astype(str) + "_aug_" + df.index.astype(str)

    logger.info("Phase 2 완료 | %d건 증폭", len(df))
    return df


# ---------------------------------------------------------------------------
# 저장 및 검증
# ---------------------------------------------------------------------------
def save_and_verify(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    size_mb = path.stat().st_size / (1024 ** 2)
    logger.info("─" * 55)
    logger.info("저장 완료 | %s | %.1f MB | %d행", path, size_mb, len(df))
    logger.info("컬럼: %s", list(df.columns))
    logger.info("샘플:\n%s", df.head(3).to_string())


def main() -> None:
    seed_df = collect_seed(TIMEBOX_SECONDS)
    full_df = augment(seed_df, TARGET_ROWS)
    save_and_verify(full_df, OUTPUT_PATH)


if __name__ == "__main__":
    main()
