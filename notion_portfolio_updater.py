#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Notion 주식 포트폴리오 자동 업데이트 스크립트 (Python)

- Notion Database의 각 종목(Ticker) 현재가/평가금액/수익률을 주기적으로 갱신합니다.
- 가격 소스: yfinance (기본). 필요 시 다른 API로 교체 가능합니다.

요구사항:
  pip install notion-client yfinance python-dotenv pandas

환경변수(.env 권장):
  NOTION_TOKEN=secret_xxx
  NOTION_DATABASE_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  BASE_CURRENCY=USD           # 표시용(계산은 숫자)
  FX_RATE=1.0                 # (선택) 원화환산 등 필요 시 사용
  UPDATE_ONLY_MISSING=false   # true면 비어있는 가격만 채움

Notion DB 속성(권장):
  - Name (title): 종목명
  - Ticker (rich_text): 예) AAPL, 005930.KS, SPY
  - Shares (number): 수량
  - Avg Cost (number): 매수단가 (기준 통화)
  - Price (number): 현재가 (자동)
  - Value (number): 평가금액 (자동)
  - P/L % (number): 수익률% (자동)
  - Last Updated (date): 업데이트 시각 (자동)
  - Notes (rich_text): 메모(선택)

실행:
  python notion_portfolio_updater.py
"""
from __future__ import annotations

import os
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import yfinance as yf
from notion_client import Client
from dotenv import load_dotenv

load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
BASE_CURRENCY = os.getenv("BASE_CURRENCY", "USD")
FX_RATE = float(os.getenv("FX_RATE", "1.0"))  # 예: USD->KRW 환산에 활용 가능(선택)
UPDATE_ONLY_MISSING = os.getenv("UPDATE_ONLY_MISSING", "false").lower() == "true"

# Notion 속성명(필요 시 여기만 수정)
PROP_TITLE = "Name"
PROP_TICKER = "Ticker"
PROP_SHARES = "Shares"
PROP_AVG_COST = "Avg Cost"
PROP_PRICE = "Price"
PROP_VALUE = "Value"
PROP_PL_PCT = "P/L %"
PROP_LAST_UPDATED = "Last Updated"

def require_env() -> None:
    missing = []
    if not NOTION_TOKEN:
        missing.append("NOTION_TOKEN")
    if not DATABASE_ID:
        missing.append("NOTION_DATABASE_ID")
    if missing:
        raise SystemExit(f"[ENV ERROR] 환경변수 누락: {', '.join(missing)}\n"
                         f"  - .env 파일을 만들고 값을 넣거나, 셸 환경변수로 설정하세요.")

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_num(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            if math.isnan(float(x)):
                return None
            return float(x)
        return float(x)
    except Exception:
        return None

def get_prop_number(page: Dict[str, Any], prop: str) -> Optional[float]:
    p = page["properties"].get(prop)
    if not p or p["type"] != "number":
        return None
    return safe_num(p.get("number"))

def get_prop_rich_text(page: Dict[str, Any], prop: str) -> str:
    p = page["properties"].get(prop)
    if not p:
        return ""
    if p["type"] == "rich_text":
        rt = p.get("rich_text", [])
        return "".join([t.get("plain_text", "") for t in rt]).strip()
    if p["type"] == "title":
        tt = p.get("title", [])
        return "".join([t.get("plain_text", "") for t in tt]).strip()
    return ""

def notion_number(value: Optional[float]) -> Dict[str, Any]:
    return {"number": value if value is not None else None}

def notion_date(iso: str) -> Dict[str, Any]:
    return {"date": {"start": iso}}

def fetch_price(ticker: str) -> Optional[float]:
    """
    yfinance로 현재가 추출.
    - 미국: "AAPL"
    - 한국(KRX): "005930.KS" (KOSPI), "035420.KQ" (KOSDAQ)
    """
    t = ticker.strip()
    if not t:
        return None

    try:
        tk = yf.Ticker(t)
        info = tk.fast_info if hasattr(tk, "fast_info") else None
        # 우선 fast_info 사용 (상대적으로 빠름)
        if info and "last_price" in info and info["last_price"] is not None:
            return float(info["last_price"])
        # fallback
        hist = tk.history(period="1d", interval="1m")
        if hist is not None and len(hist) > 0:
            return float(hist["Close"].iloc[-1])
    except Exception as e:
        print(f"[PRICE ERROR] {ticker}: {e}")
    return None

def compute_metrics(shares: Optional[float], avg_cost: Optional[float], price: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if shares is None or price is None:
        return None, None
    value = shares * price
    pl_pct = None
    if avg_cost is not None and avg_cost != 0:
        pl_pct = (price / avg_cost - 1.0) * 100.0
    return value, pl_pct

def query_all_pages(client: Client, database_id: str) -> List[Dict[str, Any]]:
    pages: List[Dict[str, Any]] = []
    cursor = None
    while True:
        resp = client.databases.query(database_id=database_id, start_cursor=cursor) if cursor else client.databases.query(database_id=database_id)
        pages.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return pages

def update_page(client: Client, page_id: str, price: Optional[float], value: Optional[float], pl_pct: Optional[float]) -> None:
    props: Dict[str, Any] = {
        PROP_PRICE: notion_number(price),
        PROP_VALUE: notion_number(value),
        PROP_PL_PCT: notion_number(pl_pct),
        PROP_LAST_UPDATED: notion_date(now_iso()),
    }
    client.pages.update(page_id=page_id, properties=props)

def main() -> None:
    require_env()
    client = Client(auth=NOTION_TOKEN)

    pages = query_all_pages(client, DATABASE_ID)
    print(f"[INFO] 페이지 수: {len(pages)}")

    updated = 0
    skipped = 0
    failed = 0

    for page in pages:
        page_id = page["id"]

        ticker = get_prop_rich_text(page, PROP_TICKER)
        if not ticker:
            skipped += 1
            continue

        shares = get_prop_number(page, PROP_SHARES)
        avg_cost = get_prop_number(page, PROP_AVG_COST)
        existing_price = get_prop_number(page, PROP_PRICE)

        if UPDATE_ONLY_MISSING and existing_price is not None:
            skipped += 1
            continue

        price = fetch_price(ticker)
        if price is None:
            failed += 1
            print(f"[WARN] 가격 조회 실패: {ticker}")
            continue

        # 필요 시 환산(예: 원화로 보여주고 싶다면 FX_RATE를 이용)
        # price_display = price * FX_RATE
        price_display = price

        value, pl_pct = compute_metrics(shares, avg_cost, price_display)
        try:
            update_page(client, page_id, price_display, value, pl_pct)
            updated += 1
            title = get_prop_rich_text(page, PROP_TITLE) or ticker
            print(f"[OK] {title} ({ticker}) price={price_display:.4f} value={value if value is not None else None} pl%={pl_pct if pl_pct is not None else None}")
        except Exception as e:
            failed += 1
            print(f"[NOTION ERROR] 업데이트 실패 {ticker}: {e}")

    print(f"[DONE] updated={updated} skipped={skipped} failed={failed} currency={BASE_CURRENCY} fx={FX_RATE}")

if __name__ == "__main__":
    main()
