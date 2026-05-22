"""
台股智能分析平台 - 後端 API v2
資料來源：
  - 三竹股市 API (Mistock) — 盤中即時行情，免費無需 key
  - TWSE 官方 API            — 收盤完整資料、月線歷史
  - 各媒體 RSS               — 財經新聞
部署：Render.com 免費方案
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx
import feedparser
import asyncio
from datetime import datetime, date
import time

app = FastAPI(title="台股智能分析 API v2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# 快取設定
# ─────────────────────────────────────────────
_cache = {}
CACHE_TTL_REALTIME = 30   # 即時行情：30 秒更新
CACHE_TTL_HISTORY  = 300  # 月線歷史：5 分鐘
CACHE_TTL_NEWS     = 120  # 新聞：2 分鐘

def cache_get(key):
    item = _cache.get(key)
    if item and time.time() - item["ts"] < item["ttl"]:
        return item["data"]
    return None

def cache_set(key, data, ttl=60):
    _cache[key] = {"data": data, "ts": time.time(), "ttl": ttl}

# ─────────────────────────────────────────────
# API 來源設定
# ─────────────────────────────────────────────

# 三竹股市 API（盤中即時，免費）
# 文件參考：http://www.mstock.com.tw/
MISTOCK_BASE = "https://mis.twse.com.tw/stock/api"

# TWSE 官方（收盤完整資料）
TWSE_BASE = "https://www.twse.com.tw/rwd/zh"

# 關鍵字
KEYWORDS = ["執行長", "董事長", "總經理", "副總經理", "財務長", "發言人", "政府", "創天價"]

# RSS 新聞來源
NEWS_SOURCES = {
    "時報新聞": "https://www.chinatimes.com/rss/finance.xml",
    "自由時報": "https://ec.ltn.com.tw/rss/news.xml",
    "經濟日報": "https://money.udn.com/rssfeed/news/1001/5591?ch=money",
    "工商時報": "https://ctee.com.tw/feed",
    "非凡新聞": "https://news.ustv.com.tw/rss.xml",
}

# 共用 headers（三竹需要 Referer）
MISTOCK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
    "Referer": "https://mis.twse.com.tw/",
    "Accept": "application/json",
}

# ─────────────────────────────────────────────
# 工具函數
# ─────────────────────────────────────────────
def safe_float(val, default=0.0):
    try:
        return float(str(val).replace(",", "").replace("+", "").strip())
    except:
        return default

def safe_int(val, default=0):
    try:
        return int(str(val).replace(",", "").strip())
    except:
        return default

def check_keywords(text):
    return [kw for kw in KEYWORDS if kw in text]

def pct_change(current, prev):
    if prev and prev != 0:
        return round((current - prev) / prev * 100, 2)
    return 0.0

# ─────────────────────────────────────────────
# 1. 三竹 API — 盤中即時個股行情
#    GET /getStockInfo.do?json=1&delay=0&ex_ch=tse_2330.tw|tse_2317.tw
# ─────────────────────────────────────────────
@app.get("/api/realtime/{stock_codes}")
async def get_realtime(stock_codes: str):
    """
    即時查詢個股報價（三竹 API）
    stock_codes: 逗號分隔，例如 2330,2317,2454
    """
    cached = cache_get(f"rt_{stock_codes}")
    if cached:
        return cached

    # 組成三竹格式：tse_2330.tw|tse_2317.tw
    codes = stock_codes.split(",")
    ex_ch = "|".join(f"tse_{c.strip()}.tw" for c in codes)

    url = f"{MISTOCK_BASE}/getStockInfo.do?json=1&delay=0&ex_ch={ex_ch}"

    async with httpx.AsyncClient(timeout=10) as client:
        try:
            r = await client.get(url, headers=MISTOCK_HEADERS)
            raw = r.json()
            msgArray = raw.get("msgArray", [])

            stocks = []
            for item in msgArray:
                z  = safe_float(item.get("z",  item.get("y", 0)))   # 成交價（z=即時，y=昨收）
                y  = safe_float(item.get("y", 0))                    # 昨收
                v  = safe_float(item.get("v", 0))                    # 成交量（張）
                a  = safe_float(item.get("a", 0))                    # 最佳賣價
                b  = safe_float(item.get("b", 0))                    # 最佳買價
                tv = safe_float(item.get("tv", 0))                   # 上成值（元）

                change = round(z - y, 2) if z and y else 0
                change_pct = pct_change(z, y)
                turnover_value = round(tv / 1e8, 2) if tv else round(z * v * 1000 / 1e8, 2)

                stocks.append({
                    "code":           item.get("c", ""),
                    "name":           item.get("n", ""),
                    "price":          z,
                    "open":           safe_float(item.get("o", 0)),
                    "high":           safe_float(item.get("h", 0)),
                    "low":            safe_float(item.get("l", 0)),
                    "prev_close":     y,
                    "change":         change,
                    "change_pct":     change_pct,
                    "volume":         int(v),           # 成交張數
                    "turnover_value": turnover_value,   # 上成值（億）
                    "is_limit_up":    change_pct >= 9.5,
                    "is_limit_dn":    change_pct <= -9.5,
                    "best_ask":       a,
                    "best_bid":       b,
                    "time":           item.get("t", ""),
                    "status":         item.get("s", ""),
                })

            result = {"count": len(stocks), "stocks": stocks, "updated_at": datetime.now().isoformat()}
        except Exception as e:
            result = {"error": str(e), "stocks": []}

    cache_set(f"rt_{stock_codes}", result, CACHE_TTL_REALTIME)
    return result


# ─────────────────────────────────────────────
# 2. 三竹 API — 大盤即時指數
#    GET /getStockInfo.do?json=1&delay=0&ex_ch=tse_t00.tw
# ─────────────────────────────────────────────
@app.get("/api/index")
async def get_index():
    """大盤加權指數即時行情（三竹）"""
    cached = cache_get("index")
    if cached:
        return cached

    url = f"{MISTOCK_BASE}/getStockInfo.do?json=1&delay=0&ex_ch=tse_t00.tw"
    async with httpx.AsyncClient(timeout=8) as client:
        try:
            r = await client.get(url, headers=MISTOCK_HEADERS)
            raw = r.json()
            item = raw.get("msgArray", [{}])[0]

            z = safe_float(item.get("z", 0))
            y = safe_float(item.get("y", 0))
            change = round(z - y, 2)
            change_pct = pct_change(z, y)

            result = {
                "index":      z,
                "prev_close": y,
                "change":     change,
                "change_pct": change_pct,
                "time":       item.get("t", ""),
                "updated_at": datetime.now().isoformat(),
            }
        except Exception as e:
            result = {"error": str(e), "index": 0}

    cache_set("index", result, CACHE_TTL_REALTIME)
    return result


# ─────────────────────────────────────────────
# 3. TWSE — 全市場收盤行情 + 上成值計算
#    盤中補充：對上成值前 N 名再用三竹抓即時價
# ─────────────────────────────────────────────
@app.get("/api/stocks/daily")
async def get_daily_stocks():
    """
    全市場收盤行情（TWSE）+ 上成值排序
    上成值 = 成交金額（億元）
    """
    cached = cache_get("daily_stocks")
    if cached:
        return cached

    url = f"{TWSE_BASE}/exchangeReport/STOCK_DAY_ALL?response=json"
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.get(url)
            raw = r.json()
            data = raw.get("data", [])

            stocks = []
            for row in data:
                if len(row) < 11:
                    continue
                code        = row[0].strip()
                name        = row[1].strip()
                close       = safe_float(row[8])
                change_str  = row[9].strip()
                change_pct  = safe_float(row[10])
                tv_yuan     = safe_float(row[4])          # 成交金額（元）
                turnover_val = round(tv_yuan / 1e8, 2)    # 轉億元

                stocks.append({
                    "code":           code,
                    "name":           name,
                    "close":          close,
                    "change":         change_str,
                    "change_pct":     round(change_pct, 2),
                    "volume_shares":  safe_int(row[2]),
                    "turnover_value": turnover_val,
                    "is_limit_up":    change_pct >= 9.5,
                    "is_limit_dn":    change_pct <= -9.5,
                })

            stocks.sort(key=lambda x: x["turnover_value"], reverse=True)

            result = {
                "total":           len(stocks),
                "limit_up_count":  sum(1 for s in stocks if s["is_limit_up"]),
                "stocks":          stocks,
                "updated_at":      datetime.now().isoformat(),
            }
        except Exception as e:
            result = {"error": str(e), "stocks": []}

    cache_set("daily_stocks", result, CACHE_TTL_REALTIME)
    return result


# ─────────────────────────────────────────────
# 4. 三竹 API — 盤中漲停板即時掃描
#    策略：先用 TWSE 取股票清單，再用三竹批次查詢
#    三竹每次最多建議查 50 檔，分批處理
# ─────────────────────────────────────────────
@app.get("/api/stocks/limit-up")
async def get_limit_up(top: int = 30):
    """
    漲停板股票列表，依上成值排序（三竹即時資料）
    """
    cached = cache_get("limit_up")
    if cached:
        return cached

    # Step 1：先從 TWSE 取昨日資料，找出可能漲停的候選股（昨日漲幅 > 5% 或知名大型股）
    daily = await get_daily_stocks()
    if "error" in daily:
        return daily

    # 取上成值前 200 名候選（盤中可能漲停的通常是量大的）
    candidates = daily["stocks"][:200]
    codes = [s["code"] for s in candidates]

    # Step 2：用三竹批次查即時行情，每批 50 檔
    realtime_stocks = []
    batch_size = 50
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        batch_str = ",".join(batch)
        rt = await get_realtime(batch_str)
        realtime_stocks.extend(rt.get("stocks", []))
        await asyncio.sleep(0.3)  # 禮貌性延遲，避免被封鎖

    # Step 3：篩選漲停 + 依上成值排序
    limit_stocks = [s for s in realtime_stocks if s["is_limit_up"]]
    limit_stocks.sort(key=lambda x: x["turnover_value"], reverse=True)

    result = {
        "count":      len(limit_stocks),
        "stocks":     limit_stocks[:top],
        "updated_at": datetime.now().isoformat(),
        "source":     "三竹即時 API",
    }
    cache_set("limit_up", result, CACHE_TTL_REALTIME)
    return result


# ─────────────────────────────────────────────
# 5. 月線創新高篩選
#    用 TWSE 月K歷史資料判斷
# ─────────────────────────────────────────────
@app.get("/api/stocks/monthly-high/{stock_code}")
async def get_monthly_high(stock_code: str):
    """個股月線創新高判斷（TWSE 月K）"""
    cached = cache_get(f"mh_{stock_code}")
    if cached:
        return cached

    today = date.today().strftime("%Y%m%d")
    url = f"{TWSE_BASE}/exchangeReport/STOCK_DAY?response=json&stockNo={stock_code}&date={today}"

    async with httpx.AsyncClient(timeout=10) as client:
        try:
            r = await client.get(url)
            raw = r.json()
            data = raw.get("data", [])
            if not data:
                return {"stock_code": stock_code, "is_monthly_high": False}

            closes = [safe_float(row[6]) for row in data if len(row) > 6 and safe_float(row[6]) > 0]
            if len(closes) < 5:
                return {"stock_code": stock_code, "is_monthly_high": False}

            current = closes[-1]
            prev_max = max(closes[:-1])
            is_high = current >= prev_max * 0.995

            result = {
                "stock_code":    stock_code,
                "current_close": current,
                "monthly_high":  prev_max,
                "is_monthly_high": is_high,
                "breakout_pct":  round((current / prev_max - 1) * 100, 2),
            }
        except Exception as e:
            result = {"stock_code": stock_code, "is_monthly_high": False, "error": str(e)}

    cache_set(f"mh_{stock_code}", result, CACHE_TTL_HISTORY)
    return result


@app.get("/api/stocks/monthly-high-list")
async def get_monthly_high_list(top: int = 50):
    """上成值前 top 名中，篩選月線創新高的股票"""
    cached = cache_get(f"mhl_{top}")
    if cached:
        return cached

    daily = await get_daily_stocks()
    if "error" in daily:
        return daily

    top_stocks = daily["stocks"][:top]

    async def check_one(stock):
        mh = await get_monthly_high(stock["code"])
        return {**stock, **mh}

    results = await asyncio.gather(*[check_one(s) for s in top_stocks], return_exceptions=True)
    monthly_highs = [r for r in results if isinstance(r, dict) and r.get("is_monthly_high")]
    monthly_highs.sort(key=lambda x: x["turnover_value"], reverse=True)

    output = {
        "count":      len(monthly_highs),
        "stocks":     monthly_highs,
        "updated_at": datetime.now().isoformat(),
    }
    cache_set(f"mhl_{top}", output, CACHE_TTL_HISTORY)
    return output


# ─────────────────────────────────────────────
# 6. 財經新聞 RSS + 關鍵字篩選
# ─────────────────────────────────────────────
@app.get("/api/news")
async def get_news(keyword: str = "", source: str = ""):
    cached = cache_get(f"news_{keyword}_{source}")
    if cached:
        return cached

    async def fetch_rss(source_name, rss_url):
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                r = await client.get(rss_url, headers={"User-Agent": "Mozilla/5.0"})
                feed = feedparser.parse(r.text)
                items = []
                for entry in feed.entries[:25]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", "")
                    full    = title + " " + summary
                    kws     = check_keywords(full)
                    try:
                        t = datetime(*entry.published_parsed[:6])
                        pub = t.strftime("%H:%M")
                    except:
                        pub = ""
                    items.append({
                        "source":         source_name,
                        "title":          title,
                        "url":            entry.get("link", ""),
                        "time":           pub,
                        "keywords_found": kws,
                        "has_keyword":    len(kws) > 0,
                    })
                return items
        except:
            return []

    sources_to_fetch = {k: v for k, v in NEWS_SOURCES.items() if not source or source in k}
    results = await asyncio.gather(*[fetch_rss(n, u) for n, u in sources_to_fetch.items()])

    all_news = [item for sublist in results for item in sublist]
    if keyword:
        all_news = [n for n in all_news if keyword in n["keywords_found"]]

    all_news.sort(key=lambda x: (not x["has_keyword"], x.get("time", "")))

    output = {
        "total":           len(all_news),
        "keyword_matched": sum(1 for n in all_news if n["has_keyword"]),
        "news":            all_news,
        "updated_at":      datetime.now().isoformat(),
    }
    cache_set(f"news_{keyword}_{source}", output, CACHE_TTL_NEWS)
    return output


# ─────────────────────────────────────────────
# 7. 健康檢查 & API 總覽
# ─────────────────────────────────────────────
@app.get("/")
async def health():
    return {
        "status":    "ok",
        "version":   "v2 — 三竹即時 + TWSE + RSS",
        "endpoints": {
            "即時大盤":     "/api/index",
            "即時個股":     "/api/realtime/2330,2317",
            "全市場行情":   "/api/stocks/daily",
            "漲停上成值榜": "/api/stocks/limit-up",
            "月線新高":     "/api/stocks/monthly-high-list",
            "財經新聞":     "/api/news",
        },
        "time": datetime.now().isoformat(),
    }
