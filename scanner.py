"""
Multi-Market Stock Dashboard Scanner
Fetches top stocks from NASDAQ, BSE (India), and UAE (ADX/DFM)
Scores them using fundamental metrics and generates buy/sell signals.

Primary data source: yfinance (no API key needed, no rate limits)
Fallback for quotes: Twelve Data API (free tier: 800 calls/day, 8/min)
"""

import os
import json
import time
import sys
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# STOCK UNIVERSES — Top 100 by market cap for each market
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
    # ADX (Abu Dhabi) - exchange code XADS
    "FAB.XADS", "ETISALAT.XADS", "ADNOCDIST.XADS", "ALDAR.XADS", "IHC.XADS",
    "ADCB.XADS", "TAQA.XADS", "DANA.XADS", "ADIB.XADS", "MULTIPLY.XADS",
    "ADNOCDRILL.XADS", "FERTIGLB.XADS", "PRESIGHT.XADS", "ALPHADHABI.XADS", "ADNOCLOG.XADS",
    "BUROOJ.XADS", "ESG.XADS", "QABC.XADS", "JULPHAR.XADS", "RAKPROP.XADS",
    "ADAVIATION.XADS", "NMDC.XADS", "AGTHIA.XADS", "PALMS.XADS", "RAK.XADS",
    # DFM (Dubai) - exchange code XDFM
    "EMAAR.XDFM", "DIB.XDFM", "DFM.XDFM", "DEWA.XDFM", "EMIRATESNBD.XDFM",
    "SALIK.XDFM", "DAMAC.XDFM", "DUBAIISLAMIC.XDFM", "MASHR.XDFM", "DEYAAR.XDFM",
    "EMAARDEV.XDFM", "TECOM.XDFM", "SHUAA.XDFM", "DUBAIINVEST.XDFM", "GFH.XDFM",
    "AMLAK.XDFM", "TABREED.XDFM", "ARAMEX.XDFM", "EIBANK.XDFM", "ENBD.XDFM",
    "DNIR.XDFM", "PARKIN.XDFM", "TALABAT.XDFM", "SPINNEYS.XDFM", "LULU.XDFM"
]

# ---------------------------------------------------------------------------
# Yahoo Finance ticker mapping
# yfinance uses different suffixes than our internal format:
#   BSE: .BO (Bombay)    UAE ADX: .AD    UAE DFM: .DU
# ---------------------------------------------------------------------------

def to_yfinance_symbol(symbol):
    """Convert our internal symbol format to yfinance format."""
    if symbol.endswith(".BSE"):
        return symbol.replace(".BSE", ".BO")
    elif symbol.endswith(".XADS"):
        return symbol.replace(".XADS", ".AD")
    elif symbol.endswith(".XDFM"):
        return symbol.replace(".XDFM", ".DU")
    return symbol  # NASDAQ tickers stay as-is


def to_display_symbol(symbol):
    """Strip exchange suffixes for display."""
    for suffix in [".BSE", ".XADS", ".XDFM"]:
        symbol = symbol.replace(suffix, "")
    return symbol


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


def fetch_yfinance_data(symbols):
    """
    Fetch quote + fundamental data for a list of symbols using yfinance.
    Returns dict: {original_symbol: {metrics...}}
    """
    import yfinance as yf

    results = {}
    total = len(symbols)

    # Process in small batches to handle errors gracefully
    batch_size = 10
    for batch_start in range(0, total, batch_size):
        batch = symbols[batch_start:batch_start + batch_size]
        yf_symbols = [to_yfinance_symbol(s) for s in batch]

        print(f"  Fetching batch {batch_start // batch_size + 1}/{(total + batch_size - 1) // batch_size}: {len(batch)} stocks...")

        for i, (orig_sym, yf_sym) in enumerate(zip(batch, yf_symbols)):
            try:
                ticker = yf.Ticker(yf_sym)
                info = ticker.info

                if not info or info.get("trailingPegRatio") is None and info.get("currentPrice") is None and info.get("regularMarketPrice") is None:
                    # Check if we got any useful data at all
                    price = info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose")
                    if not price:
                        print(f"    {yf_sym}: No data available")
                        continue

                results[orig_sym] = extract_from_yfinance(info)
                name = info.get("shortName", info.get("longName", yf_sym))
                print(f"    {yf_sym}: OK ({name})")

            except Exception as e:
                print(f"    {yf_sym}: Error - {e}")
                continue

        # Small delay between batches to be respectful
        if batch_start + batch_size < total:
            time.sleep(1)

    return results


def extract_from_yfinance(info):
    """Extract our standard metrics from yfinance ticker.info dict."""
    result = {
        "pe_ratio": None,
        "eps_growth": None,
        "dividend_yield": None,
        "revenue_growth": None,
        "profit_margin": None,
        "debt_equity": None,
        "market_cap": None,
        "52w_high": None,
        "52w_low": None,
        "beta": None,
        "price": None,
        "change": None,
        "percent_change": None,
        "volume": None,
        "name": "",
        "exchange": "",
        "currency": "USD",
    }

    # Basic quote data
    result["price"] = safe_float(info.get("currentPrice") or info.get("regularMarketPrice"))
    prev_close = safe_float(info.get("previousClose") or info.get("regularMarketPreviousClose"))
    if result["price"] and prev_close:
        result["change"] = round(result["price"] - prev_close, 4)
        if prev_close > 0:
            result["percent_change"] = round((result["change"] / prev_close) * 100, 4)

    result["volume"] = safe_float(info.get("volume") or info.get("regularMarketVolume"))
    result["name"] = info.get("shortName") or info.get("longName") or ""
    result["exchange"] = info.get("exchange", "")
    result["currency"] = info.get("currency", "USD")

    # Fundamentals
    result["pe_ratio"] = safe_float(info.get("trailingPE"))
    result["market_cap"] = safe_float(info.get("marketCap"))
    result["52w_high"] = safe_float(info.get("fiftyTwoWeekHigh"))
    result["52w_low"] = safe_float(info.get("fiftyTwoWeekLow"))
    result["beta"] = safe_float(info.get("beta"))

    # Dividend yield (yfinance returns as decimal, e.g., 0.005 = 0.5%)
    div_yield = safe_float(info.get("dividendYield"))
    if div_yield is not None:
        result["dividend_yield"] = round(div_yield * 100, 2)

    # Profit margin (yfinance returns as decimal, e.g., 0.27 = 27%)
    pm = safe_float(info.get("profitMargins"))
    if pm is not None:
        result["profit_margin"] = round(pm * 100, 2)

    # Debt to equity (yfinance returns as percentage, e.g., 150 = 1.5 ratio)
    de = safe_float(info.get("debtToEquity"))
    if de is not None:
        result["debt_equity"] = round(de / 100, 2)

    # EPS growth - use earningsGrowth (quarterly YoY) or earningsQuarterlyGrowth
    eps_g = safe_float(info.get("earningsGrowth"))
    if eps_g is not None:
        result["eps_growth"] = round(eps_g * 100, 2)
    else:
        eps_g = safe_float(info.get("earningsQuarterlyGrowth"))
        if eps_g is not None:
            result["eps_growth"] = round(eps_g * 100, 2)

    # Revenue growth (yfinance returns as decimal)
    rev_g = safe_float(info.get("revenueGrowth"))
    if rev_g is not None:
        result["revenue_growth"] = round(rev_g * 100, 2)

    return result


def compute_signal(stock):
    """
    Compute buy/sell signal based on fundamental metrics.

    Scoring (0-100 scale):
    - P/E Ratio:       0-25 pts (lower is better, relative to sector)
    - EPS Growth:       0-25 pts (higher is better)
    - Dividend Yield:   0-15 pts (higher is better, up to a point)
    - Revenue Growth:   0-15 pts (higher is better)
    - Profit Margin:    0-10 pts (higher is better)
    - Debt/Equity:      0-10 pts (lower is better)

    Signal thresholds:
    - 70+: STRONG BUY
    - 55-69: BUY
    - 40-54: HOLD
    - 25-39: SELL
    - 0-24: STRONG SELL
    """
    score = 0
    details = {}
    has_any_data = False

    pe = stock.get("pe_ratio")
    eps_growth = stock.get("eps_growth")
    div_yield = stock.get("dividend_yield")
    rev_growth = stock.get("revenue_growth")
    profit_margin = stock.get("profit_margin")
    debt_equity = stock.get("debt_equity")

    # P/E Ratio scoring (0-25)
    if pe is not None:
        has_any_data = True
        if pe < 0:
            pe_score = 0
        elif pe < 10:
            pe_score = 25
        elif pe < 15:
            pe_score = 22
        elif pe < 20:
            pe_score = 18
        elif pe < 25:
            pe_score = 14
        elif pe < 35:
            pe_score = 8
        elif pe < 50:
            pe_score = 4
        else:
            pe_score = 0
        score += pe_score
        details["pe_score"] = pe_score

    # EPS Growth scoring (0-25)
    if eps_growth is not None:
        has_any_data = True
        if eps_growth > 30:
            eg_score = 25
        elif eps_growth > 20:
            eg_score = 22
        elif eps_growth > 15:
            eg_score = 18
        elif eps_growth > 10:
            eg_score = 14
        elif eps_growth > 5:
            eg_score = 10
        elif eps_growth > 0:
            eg_score = 5
        else:
            eg_score = 0
        score += eg_score
        details["eps_growth_score"] = eg_score

    # Dividend Yield scoring (0-15)
    if div_yield is not None:
        has_any_data = True
        if div_yield > 6:
            dy_score = 8
        elif div_yield > 4:
            dy_score = 15
        elif div_yield > 3:
            dy_score = 13
        elif div_yield > 2:
            dy_score = 10
        elif div_yield > 1:
            dy_score = 7
        elif div_yield > 0:
            dy_score = 3
        else:
            dy_score = 0
        score += dy_score
        details["dividend_score"] = dy_score

    # Revenue Growth scoring (0-15)
    if rev_growth is not None:
        has_any_data = True
        if rev_growth > 25:
            rg_score = 15
        elif rev_growth > 15:
            rg_score = 12
        elif rev_growth > 10:
            rg_score = 10
        elif rev_growth > 5:
            rg_score = 7
        elif rev_growth > 0:
            rg_score = 3
        else:
            rg_score = 0
        score += rg_score
        details["revenue_growth_score"] = rg_score

    # Profit Margin scoring (0-10)
    if profit_margin is not None:
        has_any_data = True
        if profit_margin > 25:
            pm_score = 10
        elif profit_margin > 15:
            pm_score = 8
        elif profit_margin > 10:
            pm_score = 6
        elif profit_margin > 5:
            pm_score = 4
        elif profit_margin > 0:
            pm_score = 2
        else:
            pm_score = 0
        score += pm_score
        details["profit_margin_score"] = pm_score

    # Debt/Equity scoring (0-10)
    if debt_equity is not None:
        has_any_data = True
        if debt_equity < 0.3:
            de_score = 10
        elif debt_equity < 0.5:
            de_score = 8
        elif debt_equity < 1.0:
            de_score = 6
        elif debt_equity < 1.5:
            de_score = 4
        elif debt_equity < 2.0:
            de_score = 2
        else:
            de_score = 0
        score += de_score
        details["debt_equity_score"] = de_score

    # If we have no fundamental data at all, mark as N/A instead of STRONG SELL
    if not has_any_data:
        return 0, "NO DATA", details

    # Determine signal
    if score >= 70:
        signal = "STRONG BUY"
    elif score >= 55:
        signal = "BUY"
    elif score >= 40:
        signal = "HOLD"
    elif score >= 25:
        signal = "SELL"
    else:
        signal = "STRONG SELL"

    return score, signal, details


def process_market(market_name, symbols):
    """Process all stocks in a market using yfinance."""
    print(f"\n{'='*60}")
    print(f"Processing {market_name} ({len(symbols)} stocks)")
    print(f"{'='*60}")

    stocks = []

    # Fetch all data via yfinance (quotes + fundamentals in one go)
    print(f"\nFetching data for {market_name} via yfinance...")
    stock_data = fetch_yfinance_data(symbols)
    print(f"  Got data for {len(stock_data)} / {len(symbols)} stocks")

    # Process each stock
    for symbol in symbols:
        metrics = stock_data.get(symbol)
        if not metrics:
            continue

        # Need at least a price to include the stock
        if not metrics.get("price"):
            continue

        # Compute signal
        score, signal, score_details = compute_signal(metrics)

        stock_entry = {
            "symbol": to_display_symbol(symbol),
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
        stocks.append(stock_entry)

    # Sort by score descending
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

    # Filter for actionable signals
    strong_buys = [s for s in all_stocks if s["signal"] == "STRONG BUY"]
    buys = [s for s in all_stocks if s["signal"] == "BUY"]
    strong_sells = [s for s in all_stocks if s["signal"] == "STRONG SELL"]
    sells = [s for s in all_stocks if s["signal"] == "SELL"]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    body = f"Market Dashboard Scan — {now}\n"
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
    msg["Subject"] = f"Market Dashboard Alert — {len(strong_buys)} Strong Buys, {len(strong_sells)} Strong Sells"
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
    print(f"Market Dashboard Scanner — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Data source: yfinance (Yahoo Finance)")

    # Check yfinance is available
    try:
        import yfinance
        print(f"yfinance version: {yfinance.__version__}")
    except ImportError:
        print("ERROR: yfinance not installed! Run: pip install yfinance")
        sys.exit(1)

    all_stocks = []

    # yfinance has no rate limits like Twelve Data, so we can process all stocks
    # Each stock takes ~1-2s, so 250 stocks ≈ 5-8 minutes total

    # NASDAQ — top 50
    nasdaq_stocks = process_market("NASDAQ", NASDAQ_TOP_100[:50])
    all_stocks.extend(nasdaq_stocks)

    # BSE India — top 50
    bse_stocks = process_market("BSE", BSE_TOP_100[:50])
    all_stocks.extend(bse_stocks)

    # UAE — all 50
    uae_stocks = process_market("UAE", UAE_TOP_STOCKS)
    all_stocks.extend(uae_stocks)

    # Save results
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
            "no_data": len([s for s in all_stocks if s["signal"] == "NO DATA"]),
        }
    }

    # Save to data/
    os.makedirs("data", exist_ok=True)

    with open("data/dashboard.json", "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nSaved data/dashboard.json ({len(all_stocks)} stocks)")

    # Update history
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

    # Keep 90 days of history
    history = history[-90:]

    with open(history_file, "w") as f:
        json.dump(history, f, indent=2, default=str)
    print(f"Updated history ({len(history)} entries)")

    # Send email
    send_email_alert(all_stocks)

    print(f"\nDone!")


if __name__ == "__main__":
    main()
