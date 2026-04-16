"""
Multi-Market Stock Dashboard Scanner
Fetches top stocks from NASDAQ, BSE (India), and UAE (ADX/DFM)
Scores them using fundamental metrics and generates buy/sell signals.
Uses Twelve Data API (free tier: 800 calls/day, 8/min).
"""

import os
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

API_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
BASE_URL = "https://api.twelvedata.com"

# Rate limiting: 8 requests/min on free tier
REQUEST_DELAY = 8.0  # seconds between requests (safe for 8/min)
requests_made = 0

# ---------------------------------------------------------------------------
# STOCK UNIVERSES - Top 100 by market cap for each market
# We maintain curated lists because Twelve Data free tier has no screener.
# ---------------------------------------------------------------------------

NASDAQ_TOP_100 = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "COST", "NFLX",
    "AMD", "PEP", "ADBE", "CSCO", "TMUS", "INTC", "INTU", "CMCSA", "TXN", "QCOM",
    "AMGN", "ISRG", "HON", "AMAT", "BKNG", "SBUX", "VRTX", "LRCX", "ADI", "GILD",
    "MDLZ", "REGN", "MU", "PANW", "KLAC", "SNPS", "CDNS", "MELI", "PYPL", "CTAS",
    "MAR", "CRWD", "ORLY", "ABNB", "MNST", "CSX", "MRVL", "FTNT", "WDAY", "NXPI",
    "DASH", "PCAR", "DXCM", "ROST", "AEP", "MCHP", "CPRT", "ODFL", "KDP", "PAYX",
    "IDXX", "TTD", "KHC", "CHTR", "FAST", "VRSK", "CTSH", "EXC", "CSGP", "GEHC",
    "FANG", "EA", "BKR", "DDOG", "XEL", "CCEP", "TEAM", "ANSS", "ON", "CDW",
    "ZS", "ILMN", "BIIB", "WBD", "DLTR", "MRNA", "CEG", "GFS", "ARM", "SMCI",
    "COIN", "MSTR", "PLTR", "APP", "HOOD", "ROKU", "LULU", "AZN", "ASML", "PDD"
]

BSE_TOP_100 = [
    "RELIANCE.BSE", "TCS.BSE", "HDFCBANK.BSE", "INFY.BSE", "ICICIBANK.BSE",
    "HINDUNILVR.BSE", "BHARTIARTL.BSE", "SBIN.BSE", "ITC.BSE", "BAJFINANCE.BSE",
    "LICI.BSE", "LT.BSE", "HCLTECH.BSE", "KOTAKBANK.BSE", "AXISBANK.BSE",
    "MARUTI.BSE", "SUNPHARMA.BSE", "TITAN.BSE", "ONGC.BSE", "NTPC.BSE",
    "ADANIENT.BSE", "ADANIPORTS.BSE", "ULTRACEMCO.BSE", "ASIANPAINT.BSE", "BAJAJFINSV.BSE",
    "WIPRO.BSE", "COALINDIA.BSE", "POWERGRID.BSE", "TATAMOTORS.BSE", "JSWSTEEL.BSE",
    "M&M.BSE", "NESTLEIND.BSE", "TATASTEEL.BSE", "TECHM.BSE", "HDFCLIFE.BSE",
    "BAJAJ-AUTO.BSE", "SBILIFE.BSE", "INDUSINDBK.BSE", "GRASIM.BSE", "DIVISLAB.BSE",
    "DRREDDY.BSE", "CIPLA.BSE", "BPCL.BSE", "BRITANNIA.BSE", "APOLLOHOSP.BSE",
    "HINDALCO.BSE", "EICHERMOT.BSE", "HEROMOTOCO.BSE", "TATACONSUM.BSE", "VEDL.BSE",
    "ADANIGREEN.BSE", "DABUR.BSE", "GODREJCP.BSE", "HAVELLS.BSE", "PIDILITIND.BSE",
    "SIEMENS.BSE", "DLF.BSE", "AMBUJACEM.BSE", "ABB.BSE", "BANKBARODA.BSE",
    "ICICIPRULI.BSE", "INDIGO.BSE", "TORNTPHARM.BSE", "SRF.BSE", "SBICARD.BSE",
    "CHOLAFIN.BSE", "IOC.BSE", "TATAPOWER.BSE", "PNB.BSE", "MARICO.BSE",
    "BERGEPAINT.BSE", "MUTHOOTFIN.BSE", "CANBK.BSE", "PIIND.BSE", "PERSISTENT.BSE",
    "LTIM.BSE", "MAXHEALTH.BSE", "HAL.BSE", "BHEL.BSE", "IRCTC.BSE",
    "ZOMATO.BSE", "PAYTM.BSE", "NYKAA.BSE", "POLICYBZR.BSE", "DMART.BSE",
    "TRENT.BSE", "JSWENERGY.BSE", "RECLTD.BSE", "PFC.BSE", "NHPC.BSE",
    "SAIL.BSE", "GAIL.BSE", "PETRONET.BSE", "CONCOR.BSE", "MOTHERSON.BSE",
    "VOLTAS.BSE", "AUROPHARMA.BSE", "LUPIN.BSE", "PAGEIND.BSE", "MFSL.BSE"
]

UAE_TOP_STOCKS = [
    "FAB.XADS", "ETISALAT.XADS", "ADNOCDIST.XADS", "ALDAR.XADS", "IHC.XADS",
    "ADCB.XADS", "TAQA.XADS", "DANA.XADS", "ADIB.XADS", "MULTIPLY.XADS",
    "ADNOCDRILL.XADS", "FERTIGLB.XADS", "PRESIGHT.XADS", "ALPHADHABI.XADS", "ADNOCLOG.XADS",
    "BUROOJ.XADS", "ESG.XADS", "QABC.XADS", "JULPHAR.XADS", "RAKPROP.XADS",
    "ADAVIATION.XADS", "NMDC.XADS", "AGTHIA.XADS", "PALMS.XADS", "RAK.XADS",
    "EMAAR.XDFM", "DIB.XDFM", "DFM.XDFM", "DEWA.XDFM", "EMIRATESNBD.XDFM",
    "SALIK.XDFM", "DAMAC.XDFM", "DUBAIISLAMIC.XDFM", "MASHR.XDFM", "DEYAAR.XDFM",
    "EMAARDEV.XDFM", "TECOM.XDFM", "SHUAA.XDFM", "DUBAIINVEST.XDFM", "GFH.XDFM",
    "AMLAK.XDFM", "TABREED.XDFM", "ARAMEX.XDFM", "EIBANK.XDFM", "ENBD.XDFM",
    "DNIR.XDFM", "PARKIN.XDFM", "TALABAT.XDFM", "SPINNEYS.XDFM", "LULU.XDFM"
]


def api_call(endpoint, params):
    """Make a rate-limited API call to Twelve Data."""
    global requests_made

    params["apikey"] = API_KEY
    query = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{BASE_URL}/{endpoint}?{query}"

    # Rate limiting
    if requests_made > 0:
        time.sleep(REQUEST_DELAY)
    requests_made += 1

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MarketDashboard/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        return data
    except (urllib.error.URLError, json.JSONDecodeError) as e:
        print(f"  API error for {endpoint} ({params.get('symbol','')}): {e}")
        return None


def safe_float(val, default=None):
    """Safely convert to float, handling None, NaN, 'N/A', etc."""
    if val is None:
        return default
    try:
        f = float(val)
        if f != f or abs(f) == float('inf'):  # NaN or Inf
            return default
        return f
    except (ValueError, TypeError):
        return default


def fetch_batch_quotes(symbols):
    """Fetch price quotes for a batch of symbols."""
    results = {}
    total = len(symbols)

    for i, symbol in enumerate(symbols):
        print(f"  Quote {i+1}/{total}: {symbol}")
        data = api_call("quote", {"symbol": symbol})
        if data and "code" not in data:
            results[symbol] = data
        elif data and data.get("code") == 429:
            print("  Rate limited! Waiting 60s...")
            time.sleep(60)
            data = api_call("quote", {"symbol": symbol})
            if data and "code" not in data:
                results[symbol] = data

    return results


def fetch_statistics(symbol):
    """Fetch fundamental statistics for a single symbol."""
    data = api_call("statistics", {"symbol": symbol})
    if data and "code" not in data:
        return data
    return None


def fetch_fundamentals(symbols):
    """Fetch statistics/fundamentals for all symbols."""
    results = {}
    total = len(symbols)

    for i, symbol in enumerate(symbols):
        print(f"  Fundamentals {i+1}/{total}: {symbol}")
        data = fetch_statistics(symbol)
        if data:
            results[symbol] = data
        elif data is None:
            pass

    return results


def compute_signal(stock):
    """
    Compute buy/sell signal based on fundamental metrics.
    Scoring (0-100): P/E(25) + EPS Growth(25) + Div Yield(15) + Rev Growth(15) + Margin(10) + D/E(10)
    """
    score = 0
    details = {}

    pe = stock.get("pe_ratio")
    eps_growth = stock.get("eps_growth")
    div_yield = stock.get("dividend_yield")
    rev_growth = stock.get("revenue_growth")
    profit_margin = stock.get("profit_margin")
    debt_equity = stock.get("debt_equity")

    if pe is not None:
        if pe < 0: pe_score = 0
        elif pe < 10: pe_score = 25
        elif pe < 15: pe_score = 22
        elif pe < 20: pe_score = 18
        elif pe < 25: pe_score = 14
        elif pe < 35: pe_score = 8
        elif pe < 50: pe_score = 4
        else: pe_score = 0
        score += pe_score
        details["pe_score"] = pe_score

    if eps_growth is not None:
        if eps_growth > 30: eg_score = 25
        elif eps_growth > 20: eg_score = 22
        elif eps_growth > 15: eg_score = 18
        elif eps_growth > 10: eg_score = 14
        elif eps_growth > 5: eg_score = 10
        elif eps_growth > 0: eg_score = 5
        else: eg_score = 0
        score += eg_score
        details["eps_growth_score"] = eg_score

    if div_yield is not None:
        if div_yield > 6: dy_score = 8
        elif div_yield > 4: dy_score = 15
        elif div_yield > 3: dy_score = 13
        elif div_yield > 2: dy_score = 10
        elif div_yield > 1: dy_score = 7
        elif div_yield > 0: dy_score = 3
        else: dy_score = 0
        score += dy_score
        details["dividend_score"] = dy_score

    if rev_growth is not None:
        if rev_growth > 25: rg_score = 15
        elif rev_growth > 15: rg_score = 12
        elif rev_growth > 10: rg_score = 10
        elif rev_growth > 5: rg_score = 7
        elif rev_growth > 0: rg_score = 3
        else: rg_score = 0
        score += rg_score
        details["revenue_growth_score"] = rg_score

    if profit_margin is not None:
        if profit_margin > 25: pm_score = 10
        elif profit_margin > 15: pm_score = 8
        elif profit_margin > 10: pm_score = 6
        elif profit_margin > 5: pm_score = 4
        elif profit_margin > 0: pm_score = 2
        else: pm_score = 0
        score += pm_score
        details["profit_margin_score"] = pm_score

    if debt_equity is not None:
        if debt_equity < 0.3: de_score = 10
        elif debt_equity < 0.5: de_score = 8
        elif debt_equity < 1.0: de_score = 6
        elif debt_equity < 1.5: de_score = 4
        elif debt_equity < 2.0: de_score = 2
        else: de_score = 0
        score += de_score
        details["debt_equity_score"] = de_score

    if score >= 70: signal = "STRONG BUY"
    elif score >= 55: signal = "BUY"
    elif score >= 40: signal = "HOLD"
    elif score >= 25: signal = "SELL"
    else: signal = "STRONG SELL"

    return score, signal, details


def extract_fundamentals(stats_data, quote_data):
    """Extract key fundamental metrics from Twelve Data API responses."""
    result = {
        "pe_ratio": None, "eps_growth": None, "dividend_yield": None,
        "revenue_growth": None, "profit_margin": None, "debt_equity": None,
        "market_cap": None, "52w_high": None, "52w_low": None, "beta": None,
    }

    if quote_data:
        result["price"] = safe_float(quote_data.get("close"))
        result["change"] = safe_float(quote_data.get("change"))
        result["percent_change"] = safe_float(quote_data.get("percent_change"))
        result["volume"] = safe_float(quote_data.get("volume"))
        result["name"] = quote_data.get("name", "")
        result["exchange"] = quote_data.get("exchange", "")
        result["currency"] = quote_data.get("currency", "")
        result["52w_high"] = safe_float(quote_data.get("fifty_two_week", {}).get("high"))
        result["52w_low"] = safe_float(quote_data.get("fifty_two_week", {}).get("low"))

    if stats_data:
        # Twelve Data nests everything under "statistics" key
        stats = stats_data.get("statistics", stats_data)

        val = stats.get("valuations_metrics", {})
        if not val:
            val = stats.get("valuation_metrics", {})
        result["pe_ratio"] = safe_float(val.get("trailing_pe"))
        result["market_cap"] = safe_float(val.get("market_capitalization"))

        fin = stats.get("financials", {})
        if fin:
            bal = fin.get("balance_sheet", {})
            inc = fin.get("income_statement", {})

            # Profit margin - directly available on financials level
            pm = safe_float(fin.get("profit_margin"))
            if pm is not None:
                result["profit_margin"] = round(pm * 100, 2)
            elif inc:
                rev = safe_float(inc.get("revenue_ttm"))
                net_inc = safe_float(inc.get("net_income_to_common_ttm"))
                if rev and net_inc and rev > 0:
                    result["profit_margin"] = round((net_inc / rev) * 100, 2)

            # Debt to equity - use pre-computed ratio if available
            if bal:
                de_pct = safe_float(bal.get("total_debt_to_equity_mrq"))
                if de_pct is not None:
                    result["debt_equity"] = round(de_pct / 100, 2)
                else:
                    total_debt = safe_float(bal.get("total_debt_mrq") or bal.get("total_debt"))
                    equity = safe_float(bal.get("shareholders_equity"))
                    if total_debt is not None and equity and equity > 0:
                        result["debt_equity"] = round(total_debt / equity, 2)

            # EPS growth from quarterly YoY earnings growth
            if inc:
                eps_g = safe_float(inc.get("quarterly_earnings_growth_yoy"))
                if eps_g is not None:
                    result["eps_growth"] = round(eps_g * 100, 2)

            # Revenue growth from quarterly revenue growth
            if inc:
                rev_g = safe_float(inc.get("quarterly_revenue_growth"))
                if rev_g is not None:
                    result["revenue_growth"] = round(rev_g * 100, 2)

        divs = stats.get("dividends_and_splits", {})
        if divs:
            result["dividend_yield"] = safe_float(divs.get("forward_annual_dividend_yield"))
            if result["dividend_yield"]:
                result["dividend_yield"] = round(result["dividend_yield"] * 100, 2)

        sp = stats.get("stock_price_summary", {})
        if sp:
            result["beta"] = safe_float(sp.get("beta"))
            if not result["52w_high"]:
                result["52w_high"] = safe_float(sp.get("fifty_two_week_high"))
            if not result["52w_low"]:
                result["52w_low"] = safe_float(sp.get("fifty_two_week_low"))

        # EPS growth fallback
        if result["eps_growth"] is None:
            earn = stats.get("earnings", {})
            if earn:
                eps_curr = safe_float(earn.get("earnings_per_share"))
                eps_prev = safe_float(earn.get("earnings_per_share_previous"))
                if eps_curr is not None and eps_prev is not None and eps_prev != 0:
                    result["eps_growth"] = round(((eps_curr - eps_prev) / abs(eps_prev)) * 100, 2)

        # Revenue growth fallback
        if result["revenue_growth"] is None:
            rev_data = stats.get("revenue", {})
            if rev_data:
                rev_curr = safe_float(rev_data.get("revenue_ttm"))
                rev_prev = safe_float(rev_data.get("revenue_previous_year"))
                if rev_curr is not None and rev_prev is not None and rev_prev > 0:
                    result["revenue_growth"] = round(((rev_curr - rev_prev) / rev_prev) * 100, 2)

    return result


def process_market(market_name, symbols):
    """Process all stocks in a market."""
    print(f"\n{'='*60}")
    print(f"Processing {market_name} ({len(symbols)} stocks)")
    print(f"{'='*60}")

    stocks = []

    print(f"\nFetching quotes for {market_name}...")
    quotes = fetch_batch_quotes(symbols)
    print(f"  Got {len(quotes)} quotes")

    print(f"\nFetching fundamentals for {market_name}...")
    fundamentals = fetch_fundamentals(symbols)
    print(f"  Got {len(fundamentals)} fundamental records")

    for symbol in symbols:
        quote = quotes.get(symbol)
        stats = fundamentals.get(symbol)
        if not quote:
            continue
        metrics = extract_fundamentals(stats, quote)
        score, signal, score_details = compute_signal(metrics)
        stock_data = {
            "symbol": symbol.replace(".BSE", "").replace(".XADS", "").replace(".XDFM", ""),
            "raw_symbol": symbol,
            "name": metrics.get("name", symbol),
            "market": market_name,
            "exchange": metrics.get("exchange", ""),
            "currency": metrics.get("currency", "USD"),
            "price": metrics.get("price"),
            "change": metrics.get("change"),
            "percent_change": metrics.get("percent_change"),
            "volume": metrics.get("volume"),
            "market_cap": metrics.get("market_cap"),
            "pe_ratio": metrics.get("pe_ratio"),
            "eps_growth": metrics.get("eps_growth"),
            "dividend_yield": metrics.get("dividend_yield"),
            "revenue_growth": metrics.get("revenue_growth"),
            "profit_margin": metrics.get("profit_margin"),
            "debt_equity": metrics.get("debt_equity"),
            "52w_high": metrics.get("52w_high"),
            "52w_low": metrics.get("52w_low"),
            "beta": metrics.get("beta"),
            "score": score,
            "signal": signal,
            "score_details": score_details,
        }
        stocks.append(stock_data)

    stocks.sort(key=lambda x: x.get("score", 0), reverse=True)
    print(f"\n  Processed {len(stocks)} stocks for {market_name}")
    signal_counts = {}
    for s in stocks:
        sig = s["signal"]
        signal_counts[sig] = signal_counts.get(sig, 0) + 1
    for sig, count in sorted(signal_counts.items()):
        print(f"    {sig}: {count}")
    return stocks


def send_email_alert(all_stocks):
    """Send email with top buy/sell signals."""
    email_user = os.environ.get("EMAIL_USER", "")
    email_pass = os.environ.get("EMAIL_PASS", "")
    email_to = os.environ.get("EMAIL_TO", "")

    if not all([email_user, email_pass, email_to]):
        print("\nEmail credentials not set, skipping alert.")
        return

    strong_buys = [s for s in all_stocks if s["signal"] == "STRONG BUY"]
    buys = [s for s in all_stocks if s["signal"] == "BUY"]
    strong_sells = [s for s in all_stocks if s["signal"] == "STRONG SELL"]
    sells = [s for s in all_stocks if s["signal"] == "SELL"]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    body = f"Market Dashboard Scan - {now}\n"
    body += "=" * 50 + "\n\n"

    if strong_buys:
        body += f"STRONG BUY ({len(strong_buys)} stocks):\n"
        body += "-" * 40 + "\n"
        for s in strong_buys[:10]:
            body += f"  {s['symbol']:12s} | {s['market']:8s} | Score: {s['score']:3d} | Price: {s.get('currency','')}{s.get('price','N/A')}\n"
            body += f"               P/E: {s.get('pe_ratio','N/A')} | EPS Growth: {s.get('eps_growth','N/A')}% | Div: {s.get('dividend_yield','N/A')}%\n"
        body += "\n"

    if buys:
        body += f"BUY ({len(buys)} stocks):\n"
        body += "-" * 40 + "\n"
        for s in buys[:10]:
            body += f"  {s['symbol']:12s} | {s['market']:8s} | Score: {s['score']:3d} | Price: {s.get('currency','')}{s.get('price','N/A')}\n"
        body += "\n"

    if strong_sells:
        body += f"STRONG SELL ({len(strong_sells)} stocks):\n"
        body += "-" * 40 + "\n"
        for s in strong_sells[:10]:
            body += f"  {s['symbol']:12s} | {s['market']:8s} | Score: {s['score']:3d} | Price: {s.get('currency','')}{s.get('price','N/A')}\n"
        body += "\n"

    if sells:
        body += f"SELL ({len(sells)} stocks):\n"
        body += "-" * 40 + "\n"
        for s in sells[:10]:
            body += f"  {s['symbol']:12s} | {s['market']:8s} | Score: {s['score']:3d} | Price: {s.get('currency','')}{s.get('price','N/A')}\n"
        body += "\n"

    body += f"\nTotal stocks scanned: {len(all_stocks)}\n"
    body += "View full dashboard: https://vishwasndn.github.io/market-dashboard/\n"

    import smtplib
    from email.mime.text import MIMEText

    msg = MIMEText(body)
    msg["Subject"] = f"Market Dashboard Alert - {len(strong_buys)} Strong Buys, {len(strong_sells)} Strong Sells"
    msg["From"] = email_user
    msg["To"] = email_to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(email_user, email_pass)
            server.sendmail(email_user, email_to, msg.as_string())
        print(f"\nEmail alert sent to {email_to}")
    except Exception as e:
        print(f"\nEmail error: {e}")


def main():
    if not API_KEY:
        print("ERROR: TWELVEDATA_API_KEY environment variable not set!")
        print("Get your free key at https://twelvedata.com/pricing")
        return

    print(f"Market Dashboard Scanner - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"API Key: ...{API_KEY[-4:]}")

    all_stocks = []

    # Free tier: Top 50 per market = ~300 calls = ~38 min
    nasdaq_stocks = process_market("NASDAQ", NASDAQ_TOP_100[:50])
    all_stocks.extend(nasdaq_stocks)

    bse_stocks = process_market("BSE", BSE_TOP_100[:50])
    all_stocks.extend(bse_stocks)

    uae_stocks = process_market("UAE", UAE_TOP_STOCKS)
    all_stocks.extend(uae_stocks)

    scan_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    output = {
        "scan_time": scan_time,
        "total_stocks": len(all_stocks),
        "markets": {
            "NASDAQ": [s for s in all_stocks if s["market"] == "NASDAQ"],
            "BSE": [s for s in all_stocks if s["market"] == "BSE"],
            "UAE": [s for s in all_stocks if s["market"] == "UAE"],
        },
        "summary": {
            "strong_buy": len([s for s in all_stocks if s["signal"] == "STRONG BUY"]),
            "buy": len([s for s in all_stocks if s["signal"] == "BUY"]),
            "hold": len([s for s in all_stocks if s["signal"] == "HOLD"]),
            "sell": len([s for s in all_stocks if s["signal"] == "SELL"]),
            "strong_sell": len([s for s in all_stocks if s["signal"] == "STRONG SELL"]),
        }
    }

    os.makedirs("data", exist_ok=True)
    with open("data/dashboard.json", "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nSaved data/dashboard.json ({len(all_stocks)} stocks)")

    history_file = "data/history.json"
    history = []
    if os.path.exists(history_file):
        try:
            with open(history_file) as f:
                history = json.load(f)
        except:
            history = []

    history.append({
        "date": scan_time,
        "total": len(all_stocks),
        "strong_buy": output["summary"]["strong_buy"],
        "buy": output["summary"]["buy"],
        "hold": output["summary"]["hold"],
        "sell": output["summary"]["sell"],
        "strong_sell": output["summary"]["strong_sell"],
        "top_buys": [{"symbol": s["symbol"], "market": s["market"], "score": s["score"]}
                     for s in sorted(all_stocks, key=lambda x: x["score"], reverse=True)[:5]],
    })
    history = history[-90:]

    with open(history_file, "w") as f:
        json.dump(history, f, indent=2, default=str)
    print(f"Updated history ({len(history)} entries)")

    send_email_alert(all_stocks)
    print(f"\nDone! Total API calls: {requests_made}")


if __name__ == "__main__":
    main()
