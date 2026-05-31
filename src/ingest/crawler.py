import time
import random
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_URL    = "https://www.mule.co.kr"
OUTPUT_PATH = Path("data/sample/mule_raw_sample.csv")
MAX_PAGES   = 3   # 테스트용: 카테고리당 상위 3페이지만 수집

# ---------------------------------------------------------------------------
# 무작위 지연 설정 (고정 패턴 방지 → IP 밴 및 DDoS 오인 차단)
# ---------------------------------------------------------------------------
JITTER_PAGE_MIN     = 1.0   # 페이지 요청 간 최소 대기 (초)
JITTER_PAGE_MAX     = 2.5   # 페이지 요청 간 최대 대기 (초)
JITTER_CATEGORY_MIN = 2.5   # 카테고리 전환 시 최소 대기 (초)
JITTER_CATEGORY_MAX = 5.0   # 카테고리 전환 시 최대 대기 (초)

# ---------------------------------------------------------------------------
# 재시도 설정
# ---------------------------------------------------------------------------
MAX_RETRIES         = 3
BACKOFF_BASE_SEC    = 2.0   # 지수 백오프 기준 (초), 시도별 × 2^n

# ---------------------------------------------------------------------------
# User-Agent 풀 (최신 실제 브라우저 UA 5종 순환)
# WAF/봇 차단 솔루션은 단일 UA 고정 패턴을 1순위로 탐지함
# ---------------------------------------------------------------------------
UA_POOL = [
    # Chrome 124 — Windows 11
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    # Chrome 123 — Windows 10
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    # Chrome 124 — macOS Sonoma
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    # Firefox 125 — Windows 10
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
    # Edge 124 — Windows 11
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
    ),
]

# 뮬 9개 메인 카테고리 slug 목록
CATEGORIES = {
    "이펙터":      "/goods/list?category=effector",
    "일렉기타":    "/goods/list?category=electric-guitar",
    "베이스기타":  "/goods/list?category=bass-guitar",
    "어쿠스틱기타":"/goods/list?category=acoustic-guitar",
    "신스/키보드": "/goods/list?category=synthesizer",
    "앰프":        "/goods/list?category=amp",
    "음향장비":    "/goods/list?category=audio-equipment",
    "스튜디오장비":"/goods/list?category=studio",
    "기타악세서리":"/goods/list?category=accessory",
}


# ---------------------------------------------------------------------------
# 세션 빌더 — 실제 크롬 브라우저와 동일한 HTTP 헤더 세트 구성
# ---------------------------------------------------------------------------
def _build_session() -> requests.Session:
    """
    requests.Session 을 생성하고 실제 브라우저가 전송하는 전체 헤더를 설정.
    - Accept / Accept-Encoding: 브라우저가 처리 가능한 포맷 명시
    - Sec-Fetch-*: Fetch Metadata 헤더 (주요 WAF 검증 대상)
    - DNT: Do-Not-Track (오히려 브라우저 요청처럼 보이게 함)
    User-Agent는 fetch_page() 호출마다 UA_POOL에서 순환 교체.
    """
    session = requests.Session()
    session.headers.update({
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.7"
        ),
        "Accept-Language":          "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding":          "gzip, deflate, br",
        "Connection":               "keep-alive",
        "Upgrade-Insecure-Requests":"1",
        "Sec-Fetch-Dest":           "document",
        "Sec-Fetch-Mode":           "navigate",
        "Sec-Fetch-Site":           "none",
        "Sec-Fetch-User":           "?1",
        "Cache-Control":            "max-age=0",
        "DNT":                      "1",
    })
    return session


# ---------------------------------------------------------------------------
# 지연 헬퍼
# ---------------------------------------------------------------------------
def _jitter(min_sec: float, max_sec: float) -> None:
    """무작위 구간 대기 — 고정 패턴 봇 탐지 우회."""
    delay = random.uniform(min_sec, max_sec)
    logger.debug("대기 %.2f초", delay)
    time.sleep(delay)


# ---------------------------------------------------------------------------
# 페이지 요청 (재시도 + 지수 백오프 + UA 순환)
# ---------------------------------------------------------------------------
def fetch_page(url: str, session: requests.Session) -> BeautifulSoup | None:
    """
    단일 URL을 가져와 BeautifulSoup 객체를 반환.
    실패 시 지수 백오프(2^n 초) 적용 후 MAX_RETRIES 회 재시도.
    429 / 503 응답은 더 긴 대기 후 재시도, 403 / 404는 즉시 스킵.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        # 시도마다 UA 순환 (봇 특징인 단일 UA 고정 패턴 방지)
        session.headers["User-Agent"] = random.choice(UA_POOL)

        try:
            resp = session.get(url, timeout=12)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")

        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0

            if status in (429, 503):
                # 서버가 과부하를 신호 → 더 긴 대기
                wait = BACKOFF_BASE_SEC ** attempt + random.uniform(2.0, 5.0)
                logger.warning(
                    "HTTP %d (과부하/레이트리밋) — %.1f초 후 재시도 (%d/%d): %s",
                    status, wait, attempt, MAX_RETRIES, url,
                )
                time.sleep(wait)

            elif status in (403, 404):
                logger.warning("HTTP %d → 스킵: %s", status, url)
                return None

            else:
                wait = BACKOFF_BASE_SEC ** attempt
                logger.warning(
                    "HTTP %d — %.1f초 후 재시도 (%d/%d): %s",
                    status, wait, attempt, MAX_RETRIES, url,
                )
                time.sleep(wait)

        except requests.RequestException as exc:
            wait = BACKOFF_BASE_SEC ** attempt
            logger.warning(
                "요청 실패 — %.1f초 후 재시도 (%d/%d): %s | %s",
                wait, attempt, MAX_RETRIES, url, exc,
            )
            time.sleep(wait)

    logger.error("최대 재시도(%d회) 초과 → 스킵: %s", MAX_RETRIES, url)
    return None


# ---------------------------------------------------------------------------
# 가격 파싱
# ---------------------------------------------------------------------------
def parse_price(raw: str) -> int:
    """가격 문자열을 정수로 변환. 파싱 불가 시 0 반환."""
    try:
        digits = "".join(c for c in raw if c.isdigit())
        return int(digits) if digits else 0
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# 매물 파싱
# ---------------------------------------------------------------------------
def parse_items(soup: BeautifulSoup, category_name: str) -> list[dict]:
    """
    파싱된 HTML에서 매물 리스트를 추출하여 반환.
    NOTE: 뮬 실제 HTML 구조에 맞게 CSS 셀렉터 조정 필요 (F12 확인).
    """
    items = []
    cards = soup.select("ul.goods-list li.goods-item")

    if not cards:
        logger.debug("[%s] 매물 카드를 찾지 못했습니다.", category_name)
        return items

    for card in cards:
        try:
            link_tag = card.select_one("a[href]")
            mule_id  = ""
            if link_tag:
                href    = link_tag.get("href", "")
                mule_id = href.split("/")[-1].split("?")[0]

            title_tag = card.select_one(".goods-title, .title, h3")
            title     = title_tag.get_text(strip=True) if title_tag else ""

            price_tag = card.select_one(".goods-price, .price")
            price     = parse_price(price_tag.get_text(strip=True) if price_tag else "")

            date_tag  = card.select_one(".reg-date, .date, time")
            reg_date  = date_tag.get_text(strip=True) if date_tag else ""

            sold_tag  = card.select_one(".sold-out, .is-sold, .badge-sold")
            is_sold   = sold_tag is not None

            items.append({
                "mule_id":    mule_id,
                "title":      title,
                "price":      price,
                "category":   category_name,
                "reg_date":   reg_date,
                "is_sold":    is_sold,
                "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })

        except Exception as exc:
            logger.warning("[%s] 매물 파싱 오류: %s", category_name, exc)
            continue

    return items


# ---------------------------------------------------------------------------
# 카테고리 크롤링 (페이지 단위 지터 적용)
# ---------------------------------------------------------------------------
def crawl_category(
    category_name: str,
    path: str,
    session: requests.Session,
) -> list[dict]:
    """단일 카테고리를 MAX_PAGES 페이지까지 수집. 페이지마다 무작위 대기."""
    all_items: list[dict] = []

    for page in range(1, MAX_PAGES + 1):
        url = f"{BASE_URL}{path}&page={page}"
        logger.info("[%s] p.%d 수집 중... %s", category_name, page, url)

        soup = fetch_page(url, session)
        if soup is None:
            logger.warning("[%s] p.%d 스킵", category_name, page)
            # 실패해도 지터 대기 유지 (재연결 패턴 감추기)
            _jitter(JITTER_PAGE_MIN, JITTER_PAGE_MAX)
            continue

        items = parse_items(soup, category_name)
        logger.info("[%s] p.%d: %d건 수집", category_name, page, len(items))
        all_items.extend(items)

        # 마지막 페이지 이후 불필요한 대기 방지
        if page < MAX_PAGES:
            _jitter(JITTER_PAGE_MIN, JITTER_PAGE_MAX)

    return all_items


# ---------------------------------------------------------------------------
# 진입점
# ---------------------------------------------------------------------------
def main() -> None:
    session  = _build_session()
    all_data: list[dict] = []

    categories = list(CATEGORIES.items())
    for idx, (name, path) in enumerate(categories):
        logger.info("===== 카테고리 시작 (%d/%d): %s =====",
                    idx + 1, len(categories), name)

        items = crawl_category(name, path, session)
        all_data.extend(items)

        # 카테고리 전환 시 더 긴 무작위 대기 (사람이 탐색하는 패턴 모사)
        if idx < len(categories) - 1:
            pause = random.uniform(JITTER_CATEGORY_MIN, JITTER_CATEGORY_MAX)
            logger.info("다음 카테고리까지 %.1f초 대기...", pause)
            time.sleep(pause)

    if not all_data:
        logger.error("수집된 데이터가 없습니다. HTML 셀렉터를 확인하세요.")
        return

    df = pd.DataFrame(all_data)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    logger.info("저장 완료: %s (%d건)", OUTPUT_PATH, len(df))
    logger.info("\n%s", df.head())


if __name__ == "__main__":
    main()
