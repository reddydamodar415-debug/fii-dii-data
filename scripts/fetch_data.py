"""
fetch_data.py - NSE FII/DII + Bulk Deals + Block Deals + F&O COT Auto-Fetch
Run by GitHub Actions daily at 6 PM IST (Mon-Fri).
Writes:
  data/latest.json      - today FII/DII session data
    data/history.json     - rolling 60-day FII/DII archive
      data/bulk_deals.json  - today NSE bulk deals
        data/block_deals.json - today NSE block deals
          data/fno_cot.json     - F&O participant OI (FII fresh positions)
          """
import requests, json, os, sys, time
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))
NSE_BASE  = "https://www.nseindia.com"
NSE_API   = NSE_BASE + "/api/fiidiiTradeReact"
COT_API   = NSE_BASE + "/api/fii-statistics"

HEADERS = {
      "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
      ),
      "Accept":          "application/json, text/plain, */*",
      "Accept-Language": "en-US,en;q=0.9",
      "Referer":         "https://www.nseindia.com/",
      "Connection":      "keep-alive",
}

def make_session():
      session = requests.Session()
      session.headers.update(HEADERS)
      try:
                session.get(NSE_BASE, timeout=15)
                time.sleep(2)
except Exception:
        pass
    return session

def fetch_json(session, url, label=""):
      try:
                resp = session.get(url, timeout=25)
                resp.raise_for_status()
                return resp.json()
except Exception as e:
        print(f"  WARNING: Could not fetch {label}: {e}", file=sys.stderr)
        return None

def _to_float(val):
      try:
                return float(val or 0)
except (ValueError, TypeError):
        return 0.0

def transform_fiidii(raw):
      out = {
                "date": "",
                "fii_buy": 0, "fii_sell": 0, "fii_net": 0,
                "dii_buy": 0, "dii_sell": 0, "dii_net": 0,
      }
      for row in raw:
                cat = (row.get("category") or "").upper()
                if "FII" in cat or "FPI" in cat:
                              out["fii_buy"]  = _to_float(row.get("buyValue",  0))
                              out["fii_sell"] = _to_float(row.get("sellValue", 0))
                              out["fii_net"]  = _to_float(row.get("netValue",  0))
                              out["date"]     = row.get("date", "")
elif "DII" in cat:
            out["dii_buy"]  = _to_float(row.get("buyValue",  0))
            out["dii_sell"] = _to_float(row.get("sellValue", 0))
            out["dii_net"]  = _to_float(row.get("netValue",  0))
    out["_updated_at"] = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")
    out["_source"]     = "github-actions"
    return out

def update_history(latest):
      path = "data/history.json"
      try:
                with open(path) as f:
                              history = json.load(f)
      except (FileNotFoundError, json.JSONDecodeError):
                history = []
            history = [r for r in history if r.get("date") != latest["date"]]
    history.insert(0, latest)
    history = history[:60]
    with open(path, "w") as f:
              json.dump(history, f)

def parse_deals(raw):
      if not raw:
                return []
            records = raw.get("data", raw) if isinstance(raw, dict) else raw
    if not isinstance(records, list):
              return []
          deals = []
    for r in records[:100]:
              deal = {
                            "symbol":    str(r.get("symbol", r.get("SYMBOL", ""))),
                            "client":    str(r.get("clientName", r.get("client", r.get("CLIENT NAME", "")))),
                            "deal_type": str(r.get("buySell", r.get("BUY/SELL", r.get("dealType", "")))),
                            "qty":       _to_float(r.get("quantityTraded", r.get("qty", 0))),
                            "price":     _to_float(r.get("tradePrice", r.get("price", 0))),
                            "date":      str(r.get("date", r.get("DATE", ""))),
              }
              if deal["symbol"]:
                            deals.append(deal)
                    return deals

def transform_fno_cot(raw):
      if not raw:
                return {}
            records = raw.get("data", [raw]) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
    result = {
              "_updated_at": datetime.now(IST).strftime("%d-%b-%Y %H:%M IST"),
              "fii_index_futures_long":  0,
              "fii_index_futures_short": 0,
              "fii_index_futures_net":   0,
              "fii_stock_futures_long":  0,
              "fii_stock_futures_short": 0,
              "fii_stock_futures_net":   0,
              "fii_index_call_long":     0,
              "fii_index_put_long":      0,
              "fii_index_call_short":    0,
              "fii_index_put_short":     0,
              "raw_records": []
    }
    for r in records[:15]:
              cat = str(r.get("category", r.get("client_type", r.get("participant", "")))).upper()
        rec = {
                      "category":          cat,
                      "fut_index_long":    _to_float(r.get("futureIndexLong",  r.get("fut_idx_long",  0))),
                      "fut_index_short":   _to_float(r.get("futureIndexShort", r.get("fut_idx_short", 0))),
                      "fut_stock_long":    _to_float(r.get("futureStockLong",  r.get("fut_stk_long",  0))),
                      "fut_stock_short":   _to_float(r.get("futureStockShort", r.get("fut_stk_short", 0))),
                      "opt_call_long":     _to_float(r.get("optionIndexCallLong",  0)),
                      "opt_put_long":      _to_float(r.get("optionIndexPutLong",   0)),
                      "opt_call_short":    _to_float(r.get("optionIndexCallShort", 0)),
                      "opt_put_short":     _to_float(r.get("optionIndexPutShort",  0)),
                      "date":              str(r.get("date", r.get("tradeDate", ""))),
        }
        if "FII" in cat or "FPI" in cat:
                      result["fii_index_futures_long"]  = rec["fut_index_long"]
                      result["fii_index_futures_short"] = rec["fut_index_short"]
                      result["fii_index_futures_net"]   = rec["fut_index_long"] - rec["fut_index_short"]
                      result["fii_stock_futures_long"]  = rec["fut_stock_long"]
                      result["fii_stock_futures_short"] = rec["fut_stock_short"]
                      result["fii_stock_futures_net"]   = rec["fut_stock_long"] - rec["fut_stock_short"]
                      result["fii_index_call_long"]     = rec["opt_call_long"]
                      result["fii_index_put_long"]      = rec["opt_put_long"]
                      result["fii_index_call_short"]    = rec["opt_call_short"]
                      result["fii_index_put_short"]     = rec["opt_put_short"]
                  result["raw_records"].append(rec)
    return result

if __name__ == "__main__":
      now_str = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")
    print(f"[{now_str}] Starting NSE data fetch...")
    session = make_session()
    os.makedirs("data", exist_ok=True)
    today_fmt = datetime.now(IST).strftime("%d-%m-%Y")

    # 1. FII/DII aggregate
    print("  [1/4] Fetching FII/DII aggregate...")
    raw = fetch_json(session, NSE_API, "FII/DII")
    if not raw:
              print("FATAL: FII/DII fetch failed.", file=sys.stderr)
              sys.exit(1)
          data = transform_fiidii(raw)
    if not data["date"]:
              print("No data today (market closed).", file=sys.stderr)
              sys.exit(0)
          print(f"  FII Net: {data['fii_net']} | DII Net: {data['dii_net']}")
    with open("data/latest.json", "w") as f:
              json.dump(data, f, indent=2)
          update_history(data)
    print("  Written: data/latest.json + data/history.json")
    time.sleep(2)

    # 2. Bulk deals
    print("  [2/4] Fetching Bulk Deals...")
    bulk_url = f"{NSE_BASE}/api/historical/bulk-deals?from={today_fmt}&to={today_fmt}"
    raw_bulk = fetch_json(session, bulk_url, "Bulk Deals")
    bulk_data = {
              "_updated_at": datetime.now(IST).strftime("%d-%b-%Y %H:%M IST"),
              "_date": data["date"],
              "deals": parse_deals(raw_bulk)
    }
    with open("data/bulk_deals.json", "w") as f:
              json.dump(bulk_data, f, indent=2)
          print(f"  Written: data/bulk_deals.json ({len(bulk_data['deals'])} deals)")
    time.sleep(2)

    # 3. Block deals
    print("  [3/4] Fetching Block Deals...")
    block_url = f"{NSE_BASE}/api/historical/block-deals?from={today_fmt}&to={today_fmt}"
    raw_block = fetch_json(session, block_url, "Block Deals")
    block_data = {
              "_updated_at": datetime.now(IST).strftime("%d-%b-%Y %H:%M IST"),
              "_date": data["date"],
              "deals": parse_deals(raw_block)
    }
    with open("data/block_deals.json", "w") as f:
              json.dump(block_data, f, indent=2)
          print(f"  Written: data/block_deals.json ({len(block_data['deals'])} deals)")
    time.sleep(2)

    # 4. F&O COT
    print("  [4/4] Fetching F&O COT participant data...")
    raw_cot = fetch_json(session, COT_API, "F&O COT")
    cot_data = transform_fno_cot(raw_cot)
    cot_data["_date"] = data["date"]
    with open("data/fno_cot.json", "w") as f:
              json.dump(cot_data, f, indent=2)
          print(f"  Written: data/fno_cot.json | FII Index Fut Net: {cot_data.get('fii_index_futures_net', 'N/A')}")

    print("\nAll done!")
