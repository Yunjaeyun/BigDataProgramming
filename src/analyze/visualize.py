"""
src/analyze/visualize.py

Q1: 카테고리별 중고악기 매물 가격 분포 (KRW 기준 Boxplot)
Q2: 행동 신호 3종 기여 점수 비율 (Stacked Bar + Pie)
Q3: 하이브리드 추천 스코어 분포 (Histogram + KDE)
"""

import re, glob, logging, platform, warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.font_manager as fm
import seaborn as sns

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 경로
# ---------------------------------------------------------------------------
PARQUET_PATH    = "data/sample/mule_processed_parquet"
LIKES_PATH      = "data/sample/post_likes.csv"
SEARCHES_PATH   = "data/sample/recent_searches.csv"
USER_POSTS_PATH = "data/sample/user_posts.csv"
RESULTS_DIR     = Path("results")
DPI = 150

# ---------------------------------------------------------------------------
# 행동 가중치 (recommender_engine.py 동기화)
# ---------------------------------------------------------------------------
WEIGHT_LIKE   = 1.0
WEIGHT_SEARCH = 0.7
WEIGHT_POST   = 0.4
CO_OCCUR_FACTOR = 0.35

SIGNAL_COLORS = {"like":"#4c8ef7", "search":"#f9a825", "post":"#66bb6a"}
SIGNAL_LABELS = {
    "like":   f"좋아요  (×{WEIGHT_LIKE})",
    "search": f"검색어  (×{WEIGHT_SEARCH})",
    "post":   f"판매이력 (×{WEIGHT_POST})",
}

# ---------------------------------------------------------------------------
# 도메인 상수
# ---------------------------------------------------------------------------
CATEGORY_KO = {
    "effector":"이펙터", "bass-guitar":"베이스기타",
    "electric-guitar":"일렉기타", "acoustic-guitar":"어쿠스틱기타",
    "synthesizer":"신스/키보드", "drum":"드럼", "amp":"앰프",
    "audio-speaker":"음향장비", "mixer-interface":"믹서/인터페이스",
}
PRICE_RANGES_KRW = {
    "effector":        (50_000,    500_000),
    "bass-guitar":     (200_000, 2_500_000),
    "electric-guitar": (200_000, 3_000_000),
    "acoustic-guitar": (150_000, 2_000_000),
    "synthesizer":     (300_000, 2_000_000),
    "drum":            (500_000, 5_000_000),
    "amp":             (200_000, 3_000_000),
    "audio-speaker":   (100_000, 1_500_000),
    "mixer-interface": (100_000, 1_000_000),
}
BRANDS = ["Fender","Gibson","Boss","Ibanez","Marshall","Eventide",
          "Strymon","MXR","Electro-Harmonix","TC Electronic",
          "Kemper","Roland","Yamaha","Unknown"]
SEED_QUERIES = ["electric guitar","bass guitar","effects pedal","synthesizer",
                "amplifier","acoustic guitar","drum","keyboard","mixer","audio interface",
                "distortion pedal","reverb pedal"]

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
def _extract_brand(kw):
    if not isinstance(kw, str): return "Unknown"
    for p, b in _BRAND_PATTERNS:
        if re.search(p, kw): return b
    return "Unknown"


# ---------------------------------------------------------------------------
# 한글 폰트 설정
# ---------------------------------------------------------------------------
def setup_font():
    sys = platform.system()
    if sys == "Windows":   font = "Malgun Gothic"
    elif sys == "Darwin":  font = "AppleGothic"
    else:
        cands = [f.name for f in fm.fontManager.ttflist
                 if any(k in f.name for k in ("Nanum","Gothic","Gulim","Dotum"))]
        font = cands[0] if cands else "DejaVu Sans"
        if not cands: logger.warning("한글 폰트 없음 — apt install fonts-nanum 권장")
    sns.set_theme(style="whitegrid", palette="Set2")   # 먼저 테마 적용
    plt.rcParams["font.family"] = font                 # 그 다음 폰트 덮어쓰기
    plt.rcParams["font.sans-serif"] = [font, "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    logger.info("폰트: %s", font)


# ---------------------------------------------------------------------------
# 데이터 로드
# ---------------------------------------------------------------------------
def load_parquet_safe(path):
    parts = glob.glob(f"{path}/*.parquet")
    if parts:
        try:
            df = pd.read_parquet(path)
            logger.info("Parquet %d행 로드", len(df))
            return df
        except Exception as e:
            logger.warning("Parquet 실패 → 합성 데이터: %s", e)
    return _make_items()


def _load_csv_safe(path, label):
    try:
        df = pd.read_csv(path)
        logger.info("[%s] %d행 로드", label, len(df))
        return df
    except Exception:
        return None


def load_action_logs():
    likes   = _load_csv_safe(LIKES_PATH,      "post_likes")      or _make_likes()
    searches= _load_csv_safe(SEARCHES_PATH,   "recent_searches") or _make_searches()
    posts   = _load_csv_safe(USER_POSTS_PATH, "user_posts")      or _make_posts()
    return likes, searches, posts


# ---------------------------------------------------------------------------
# 합성 폴백 데이터
# ---------------------------------------------------------------------------
def _make_items(n=500):
    np.random.seed(42)
    cats = list(PRICE_RANGES_KRW.keys())
    rows = []
    for i, cat in enumerate(np.random.choice(cats, n,
            p=[0.20,0.10,0.15,0.08,0.10,0.05,0.10,0.10,0.12])):
        lo, hi = PRICE_RANGES_KRW[cat]
        rows.append({"mule_id":str(i+1),"category":cat,
                     "brand":np.random.choice(BRANDS),
                     "price_krw":int(np.random.triangular(lo,(lo+hi)/2,hi))})
    return pd.DataFrame(rows)


def _make_likes():
    np.random.seed(42)
    cats = list(PRICE_RANGES_KRW.keys())
    rows = [{"user_id": uid, "post_id": str(np.random.randint(1000,9999)),
             "category": np.random.choice(cats),
             "brand": np.random.choice(BRANDS[:10]), "liked_at": "2024-01-15"}
            for uid in range(1,9) for _ in range(np.random.randint(2,6))]
    return pd.DataFrame(rows)


def _make_searches():
    pool = ["보스 오버드라이브","fender telecaster","Gibson Les Paul",
            "스트라이몬 블루스카이","Roland synth","이바네즈 베이스",
            "마샬 앰프","boss ds-1","Eventide H9","MXR Phase 90",
            "야마하 키보드","켐퍼 앰프","TC Electronic delay","ehx big muff"]
    np.random.seed(42)
    rows = [{"user_id":uid,"search_keyword":kw,"searched_at":"2024-01-15"}
            for uid in range(1,9)
            for kw in np.random.choice(pool, np.random.randint(2,5), replace=False)]
    return pd.DataFrame(rows)


def _make_posts():
    np.random.seed(42)
    cats = list(PRICE_RANGES_KRW.keys())
    rows = [{"user_id":uid,"post_id":str(np.random.randint(100,999)),
             "category":np.random.choice(cats),
             "brand":np.random.choice(BRANDS[:10]),"created_at":"2024-01-10"}
            for uid in range(1,9) for _ in range(np.random.randint(1,3))]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Q2 연산: 신호 기여 점수
# ---------------------------------------------------------------------------
def compute_signal_contributions(likes, searches, posts):
    like_sig = pd.concat([
        likes[["user_id","category"]].dropna().drop_duplicates().rename(columns={"category":"s"}),
        likes[likes["brand"].notna() & (likes["brand"]!="Unknown")][["user_id","brand"]].drop_duplicates().rename(columns={"brand":"s"}),
    ]).drop_duplicates()
    like_score = like_sig.groupby("user_id").size().mul(WEIGHT_LIKE).rename("like_score")

    s2 = searches.copy()
    s2["brand"] = s2["search_keyword"].apply(_extract_brand)
    search_sig = s2[s2["brand"]!="Unknown"][["user_id","brand"]].drop_duplicates()
    search_score = search_sig.groupby("user_id").size().mul(WEIGHT_SEARCH).rename("search_score")

    post_sig = pd.concat([
        posts[["user_id","category"]].dropna().drop_duplicates().rename(columns={"category":"s"}),
        posts[posts["brand"].notna() & (posts["brand"]!="Unknown")][["user_id","brand"]].drop_duplicates().rename(columns={"brand":"s"}),
    ]).drop_duplicates()
    post_score = post_sig.groupby("user_id").size().mul(WEIGHT_POST).rename("post_score")

    users = sorted(set(likes["user_id"].dropna()) | set(searches["user_id"].dropna()) | set(posts["user_id"].dropna()))
    df = (pd.DataFrame({"user_id":users})
          .merge(like_score.reset_index(),   on="user_id", how="left")
          .merge(search_score.reset_index(), on="user_id", how="left")
          .merge(post_score.reset_index(),   on="user_id", how="left")
          .fillna(0))
    return df


# ---------------------------------------------------------------------------
# Q1 — 카테고리별 KRW 가격 분포 (Boxplot)
# ---------------------------------------------------------------------------
def plot_q1(items_df):
    logger.info("Q1 차트 생성")
    df = items_df.copy()

    # price_krw 또는 price 컬럼 자동 감지
    price_col = "price_krw" if "price_krw" in df.columns else "price"
    df = df.rename(columns={price_col: "price_krw"})
    df["price_krw"] = pd.to_numeric(df["price_krw"], errors="coerce")
    df = df.dropna(subset=["price_krw"])
    df["cat_ko"] = df["category"].map(CATEGORY_KO).fillna(df["category"])

    q1v, q3v = df["price_krw"].quantile(0.25), df["price_krw"].quantile(0.75)
    df = df[df["price_krw"].between(q1v - 3*(q3v-q1v), q3v + 3*(q3v-q1v))]
    cat_order = df.groupby("cat_ko")["price_krw"].median().sort_values(ascending=False).index.tolist()

    fig, ax = plt.subplots(figsize=(13,6))
    sns.boxplot(data=df, x="cat_ko", y="price_krw", order=cat_order,
                palette="Set2",
                flierprops=dict(marker="o", markersize=3, alpha=0.4, color="gray"), ax=ax)
    sns.stripplot(data=df.sample(min(200,len(df)),random_state=42),
                  x="cat_ko", y="price_krw", order=cat_order,
                  color="gray", size=2.5, alpha=0.22, jitter=True, ax=ax)

    ax.set_title("카테고리별 중고악기 매물 가격 분포 — KRW 기준  (Q1)",
                 fontsize=15, fontweight="bold", pad=14)
    ax.set_xlabel("카테고리", fontsize=12)
    ax.set_ylabel("가격 (원화, KRW)", fontsize=12)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v,_: f"{int(v):,}"))
    ax.tick_params(axis="x", rotation=30)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    _save(fig, "q1_price_distribution.png")


# ---------------------------------------------------------------------------
# Q2 — 3가지 행동 신호 기여 점수 (Stacked Bar + Pie)
# ---------------------------------------------------------------------------
def plot_q2(likes, searches, posts):
    logger.info("Q2 차트 생성")
    contrib = compute_signal_contributions(likes, searches, posts)
    contrib["user_label"] = contrib["user_id"].apply(lambda x: f"User {int(x)}")
    contrib = contrib.set_index("user_label")
    sig_cols  = ["like_score","search_score","post_score"]
    sig_names = [SIGNAL_LABELS["like"], SIGNAL_LABELS["search"], SIGNAL_LABELS["post"]]
    colors    = [SIGNAL_COLORS["like"],  SIGNAL_COLORS["search"], SIGNAL_COLORS["post"]]

    fig, axes = plt.subplots(1, 2, figsize=(15,6), gridspec_kw={"width_ratios":[3,2]})

    # ── 왼쪽: 유저별 누적 Stacked Bar ────────────────────────────────────────
    ax_bar = axes[0]
    bottoms = np.zeros(len(contrib))
    for col, lbl, clr in zip(sig_cols, sig_names, colors):
        vals = contrib[col].values
        bars = ax_bar.bar(contrib.index, vals, bottom=bottoms,
                          label=lbl, color=clr, edgecolor="white", linewidth=0.8, width=0.55)
        for bar, bot, val in zip(bars, bottoms, vals):
            if val > 0.25:
                ax_bar.text(bar.get_x()+bar.get_width()/2, bot+val/2, f"{val:.1f}",
                            ha="center",va="center",fontsize=8.5,fontweight="bold",color="white")
        bottoms += vals
    for i, tot in enumerate(bottoms):
        ax_bar.text(i, tot+0.04, f"{tot:.1f}", ha="center", va="bottom", fontsize=9, color="#333")

    ax_bar.set_title("유저별 행동 신호 기여 점수  (Q2)", fontsize=13, fontweight="bold")
    ax_bar.set_xlabel("유저", fontsize=11)
    ax_bar.set_ylabel("누적 기여 점수 (가중치 합산)", fontsize=11)
    ax_bar.tick_params(axis="x", rotation=25)
    ax_bar.legend(loc="upper right", fontsize=9)
    ax_bar.grid(axis="y", linestyle="--", alpha=0.45)
    ax_bar.set_ylim(0, bottoms.max() * 1.15)

    # ── 오른쪽: 전체 기여 비율 Pie ──────────────────────────────────────────
    ax_pie = axes[1]
    totals = [contrib[c].sum() for c in sig_cols]
    nz = [(t,n,c) for t,n,c in zip(totals,sig_names,colors) if t > 0]
    if nz:
        pv, pl, pc = zip(*nz)
        _, _, autotexts = ax_pie.pie(pv, labels=pl, colors=pc, autopct="%1.1f%%",
                                     startangle=130, pctdistance=0.78,
                                     wedgeprops=dict(edgecolor="white",linewidth=2))
        for at in autotexts: at.set_fontsize(10); at.set_fontweight("bold")
    ax_pie.set_title("전체 기여 점수 비율 (신호 타입별)", fontsize=13, fontweight="bold")

    patches = [mpatches.Patch(color=c, label=f"{n}  합계: {contrib[col].sum():.1f}")
               for col,n,c in zip(sig_cols,sig_names,colors)]
    fig.legend(handles=patches, loc="lower center", ncol=3, fontsize=9,
               frameon=True, bbox_to_anchor=(0.5,-0.02))
    fig.tight_layout(rect=[0,0.06,1,1])
    _save(fig, "q2_top_brands.png")


# ---------------------------------------------------------------------------
# Q3 — 하이브리드 추천 스코어 분포 (Histogram + KDE)
# ---------------------------------------------------------------------------
def plot_q3():
    logger.info("Q3 차트 생성")
    scores = _make_hybrid_scores()
    df = pd.DataFrame({"score": scores})
    mean_s, med_s = scores.mean(), float(np.median(scores))

    fig, ax = plt.subplots(figsize=(11,5))
    sns.histplot(data=df, x="score", bins=42, kde=True,
                 color=sns.color_palette("Set2")[0],
                 edgecolor="white", linewidth=0.5, ax=ax)
    ax.axvline(mean_s, color="#e74c3c", linestyle="--", linewidth=1.8, label=f"평균  {mean_s:.2f}")
    ax.axvline(med_s,  color="#2980b9", linestyle="-.", linewidth=1.8, label=f"중앙값 {med_s:.2f}")
    ax.axvspan(1.0, scores.max()+0.1, alpha=0.07, color="#27ae60", label="추천 임계값 (≥1.0)")

    annots = [
        (WEIGHT_POST,              "판매이력\n(0.4)",      "#66bb6a"),
        (WEIGHT_SEARCH,            "검색어\n(0.7)",        "#f9a825"),
        (WEIGHT_LIKE,              "좋아요\n(1.0)",        "#4c8ef7"),
        (WEIGHT_LIKE+WEIGHT_POST,  "좋아요\n+판매 (1.4)", "#e91e63"),
        (WEIGHT_LIKE+WEIGHT_SEARCH,"좋아요\n+검색 (1.7)", "#9c27b0"),
        (WEIGHT_LIKE+WEIGHT_SEARCH+CO_OCCUR_FACTOR,
                                   "Co-occur\n보너스",    "#ff6f00"),
    ]
    y_top = ax.get_ylim()[1]
    for xpos, lbl, clr in annots:
        ax.axvline(xpos, color=clr, linestyle=":", linewidth=1.0, alpha=0.55)
        ax.text(xpos, y_top*0.72, lbl, ha="center", fontsize=7.5,
                color=clr, fontstyle="italic")

    ax.set_title("하이브리드 추천 스코어 분포 (행동신호 + Co-occurrence 보너스)  (Q3)",
                 fontsize=13, fontweight="bold", pad=14)
    ax.set_xlabel("추천 스코어 (좋아요×1.0 + 검색×0.7 + 판매×0.4 + Co-occur×0.35)",
                  fontsize=10)
    ax.set_ylabel("빈도", fontsize=12)
    ax.set_xlim(0, scores.max()+0.2)
    ax.legend(fontsize=9, loc="upper right")
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    _save(fig, "q3_recommend_score_dist.png")


def _make_hybrid_scores(n=1400):
    """행동신호 + Co-occurrence 보너스를 반영한 합성 스코어 분포."""
    np.random.seed(42)
    post_only   = np.random.normal(WEIGHT_POST,                          0.04, int(n*0.14))
    search_only = np.random.normal(WEIGHT_SEARCH,                        0.04, int(n*0.13))
    like_only   = np.random.normal(WEIGHT_LIKE,                          0.05, int(n*0.20))
    like_post   = np.random.normal(WEIGHT_LIKE+WEIGHT_POST,              0.06, int(n*0.12))
    like_search = np.random.normal(WEIGHT_LIKE+WEIGHT_SEARCH,            0.07, int(n*0.16))
    all_sig     = np.random.normal(WEIGHT_LIKE+WEIGHT_SEARCH+WEIGHT_POST,0.08, int(n*0.10))
    # Co-occurrence 보너스 포함 구간
    cooccur     = np.random.normal(WEIGHT_LIKE+WEIGHT_SEARCH+CO_OCCUR_FACTOR, 0.09, int(n*0.15))
    scores = np.clip(np.concatenate([
        post_only,search_only,like_only,like_post,like_search,all_sig,cooccur
    ]), 0.01, None)
    np.random.shuffle(scores)
    return scores


# ---------------------------------------------------------------------------
# 저장 헬퍼
# ---------------------------------------------------------------------------
def _save(fig, filename):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / filename
    fig.savefig(out, dpi=DPI, bbox_inches="tight")
    logger.info("저장: %s", out)
    plt.show()
    plt.close(fig)


# ---------------------------------------------------------------------------
# 진입점
# ---------------------------------------------------------------------------
def main():
    setup_font()
    items_df              = load_parquet_safe(PARQUET_PATH)
    likes, searches, posts = load_action_logs()

    plot_q1(items_df)
    plot_q2(likes, searches, posts)
    plot_q3()
    logger.info("모든 차트 생성 완료 → results/ 확인")


if __name__ == "__main__":
    main()
