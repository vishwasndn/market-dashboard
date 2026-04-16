"""
Multi-Market Stock Dashboard Scanner (v2 — Sector & Growth Enhanced)

Fetches top stocks from NASDAQ, BSE (India), and UAE (ADX/DFM).
Scores them using fundamental metrics with sector-relative analysis
and long-term growth potential indicators.

Data source: yfinance (no API key needed, no rate limits)

Scoring breakdown (0-100):
  Value    (25 pts): Sector-relative P/E (15) + PEG Ratio (10)
  Growth   (30 pts): EPS Growth (15) + Revenue Growth (15)
  Quality  (25 pts): Profit Margin (10) + ROE (10) + Debt/Equity (5)
  Outlook  (20 pts): Dividend Yield (5) + Analyst Rating (10) + Target Upside (5)
"""

import os
import json
import time
import sys
from datetime import datetime, timezone
from statistics import median

# ---------------------------------------------------------------------------
# STOCK UNIVERSES
# ---------------------------------------------------------------------------

NASDAQ_TOP_100 = [
    "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","AVGO","COST","NFLX",
    "AMD","PEP","ADBE","CSCO","TMUS","INTC","INTU","CMCSA","TXN","QCOM",
    "AMGN","ISRG","HON","AMAT","BKNG","SBUX","VRTX","LRCX","ADI","GILD",
    "MDLZ","REGN","MU","PANW","KLAC","SNPS","CDNS","MELI","PYPL","CTAS",
    "MAR","CRWD","ORLY","ABNB","MNST","CSX","MRVL","FTNT","WDAY","NXPI",
    "DASH","PCAR","DXCM","ROST","AEP","MCHP","CPRT","ODFL","KDP","PAYX",
    "IDXX","TTD","KHC","CHTR","FAST","VRSK","CTSH","EXC","CSGP","GEHC",
    "FANG","EA","BKR","DDOG","XEL","CCEP","TEAM","ANSS","ON","CDW",
    "ZS","ILMN","BIIB","WBD","DLTR","MRNA","CEG","GFS","ARM","SMCI",
    "COIN","MSTR","PLTR","APP","HOOD","ROKU","LULU","AZN","ASML","PDD",
]

BSE_TOP_100 = [
    "RELIANCE.BSE","TCS.BSE","HDFCBANK.BSE","INFY.BSE","ICICIBANK.BSE",
    "HINDUNILVR.BSE","BHARTIARTL.BSE","SBIN.BSE","ITC.BSE","BAJFINANCE.BSE",
    "LICI.BSE","LT.BSE","HCLTECH.BSE","KOTAKBANK.BSE","AXISBANK.BSE",
    "MARUTI.BSE","SUNPHARMA.BSE","TITAN.BSE","ONGC.BSE","NTPC.BSE",
    "ADANIENT.BSE","ADANIPORTS.BSE","ULTRACEMCO.BSE","ASIANPAINT.BSE",
    "BAJAJFINSV.BSE","WIPRO.BSE","COALINDIA.BSE","POWERGRID.BSE",
    "TATAMOTORS.BSE","JSWSTEEL.BSE","M&M.BSE","NESTLEIND.BSE",
    "TATASTEEL.BSE","TECHM.BSE","HDFCLIFE.BSE","BAJAJ-AUTO.BSE",
    "SBILIFE.BSE","INDUSINDBK.BSE","GRASIM.BSE","DIVISLAB.BSE",
    "DRREDDY.BSE","CIPLA.BSE","BPCL.BSE","BRITANNIA.BSE",
    "APOLLOHOSP.BSE","HINDALCO.BSE","EICHERMOT.BSE","HEROMOTOCO.BSE",
    "TATACONSUM.BSE","VEDL.BSE","ADANIGREEN.BSE","DABUR.BSE",
    "GODREJCP.BSE","HAVELLS.BSE","PIDILITIND.BSE","SIEMENS.BSE",
    "DLF.BSE","AMBUJACEM.BSE","ABB.BSE","BANKBARODA.BSE",
    "ICICIPRULI.BSE","INDIGO.BSE","TORNTPHARM.BSE","SRF.BSE",
    "SBICARD.BSE","CHOLAFIN.BSE","IOC.BSE","TATAPOWER.BSE",
    "PNB.BSE","MARICO.BSE","BERGEPAINT.BSE","MUTHOOTFIN.BSE",
    "CANBK.BSE","PIIND.BSE","PERSISTENT.BSE","LTIM.BSE",
    "MAXHEALTH.BSE","HAL.BSE","BHEL.BSE","IRCTC.BSE",
    "ZOMATO.BSE","PAYTM.BSE","NYKAA.BSE","POLICYBZR.BSE",
    "DMART.BSE","TRENT.BSE","JSWENERGY.BSE","RECLTD.BSE",
    "PFC.BSE","NHPC.BSE","SAIL.BSE","GAIL.BSE",
    "PETRONET.BSE","CONCOR.BSE","MOTHERSON.BSE","VOLTAS.BSE",
    "AUROPHARMA.BSE","LUPIN.BSE","PAGEIND.BSE","MFSL.BSE",
]

UAE_TOP_STOCKS = [
    "FAB.XADS","ETISALAT.XADS","ADNOCDIST.XADS","ALDAR.XADS","IHC.XADS",
    "ADCB.XADS","TAQA.XADS","DANA.XADS","ADIB.XADS","MULTIPLY.XADS",
    "ADNOCDRILL.XADS","FERTIGLB.XADS","PRESIGHT.XADS","ALPHADHABI.XADS",
    "ADNOCLOG.XADS","BUROOJ.XADS","ESG.XADS","QABC.XADS","JULPHAR.XADS",
    "RAKPROP.XADS","ADAVIATION.XADS","NMDC.XADS","AGTHIA.XADS","PALMS.XADS",
    "RAK.XADS",
    "EMAAR.XDFM","DIB.XDFM","DFM.XDFM","DEWA.XDFM","EMIRATESNBD.XDFM",
    "SALIK.XDFM","DAMAC.XDFM","DUBAIISLAMIC.XDFM","MASHR.XDFM","DEYAAR.XDFM",
    "EMAARDEV.XDFM","TECOM.XDFM","SHUAA.XDFM","DUBAIINVEST.XDFM","GFH.XDFM",
    "AMLAK.XDFM","TABREED.XDFM","ARAMEX.XDFM","EIBANK.XDFM","ENBD.XDFM",
    "DNIR.XDFM","PARKIN.XDFM","TALABAT.XDFM","SPINNEYS.XDFM","LULU.XDFM",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def to_yfinance_symbol(symbol):
    if symbol.endswith(".BSE"):
        return symbol.replace(".BSE", ".BO")
    elif symbol.endswith(".XADS"):
        return symbol.replace(".XADS", ".AD")
    elif symbol.endswith(".XDFM"):
        return symbol.replace(".XDFM", ".DU")
    return symbol

def to_display_symbol(symbol):
    for suffix in [".BSE", ".XADS", ".XDFM"]:
        symbol = symbol.replace(suffix, "")
    return symbol

def safe_float(val, default=None):
    if val is None:
        return default
    try:
        f = float(val)
        if f != f or abs(f) == float('inf'):
            return default
        return f
    except (ValueError, TypeError):
        return default

# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_yfinance_data(symbols):
    """Fetch quote + fundamental data using yfinance. Returns {symbol: metrics}."""
    import yfinance as yf
    results = {}
    total = len(symbols)
    batch_size = 10

    for batch_start in range(0, total, batch_size):
        batch = symbols[batch_start:batch_start + batch_size]
        yf_symbols = [to_yfinance_symbol(s) for s in batch]
        print(f"  Fetching batch {batch_start // batch_size + 1}/{(total + batch_size - 1) // batch_size}: {len(batch)} stocks...")

        for orig_sym, yf_sym in zip(batch, yf_symbols):
            try:
                ticker = yf.Ticker(yf_sym)
                info = ticker.info
                if not info:
                    print(f"    {yf_sym}: No data available")
                    continue

                price = info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose")
                if not price:
                    print(f"    {yf_sym}: No price data")
                    continue

                results[orig_sym] = extract_from_yfinance(info)
                name = info.get("shortName", info.get("longName", yf_sym))
                print(f"    {yf_sym}: OK ({name})")
            except Exception as e:
                print(f"    {yf_sym}: Error - {e}")
                continue

        if batch_start + batch_size < total:
            time.sleep(1)

    return results


def extract_from_yfinance(info):
    """Extract comprehensive metrics from yfinance ticker.info."""
    r = {
        # Basic
        "price": None, "change": None, "percent_change": None,
        "volume": None, "name": "", "exchange": "", "currency": "USD",
        "market_cap": None, "52w_high": None, "52w_low": None, "beta": None,
        # Fundamentals
        "pe_ratio": None, "forward_pe": None, "peg_ratio": None,
        "eps_growth": None, "dividend_yield": None, "revenue_growth": None,
        "profit_margin": None, "debt_equity": None, "roe": None,
        # Sector & Industry
        "sector": None, "industry": None,
        # Analyst
        "analyst_target": None, "analyst_rating": None, "analyst_rating_label": None,
        "target_upside": None,
    }

    # Quote data
    r["price"] = safe_float(info.get("currentPrice") or info.get("regularMarketPrice"))
    prev_close = safe_float(info.get("previousClose") or info.get("regularMarketPreviousClose"))
    if r["price"] and prev_close:
        r["change"] = round(r["price"] - prev_close, 4)
        if prev_close > 0:
            r["percent_change"] = round((r["change"] / prev_close) * 100, 4)
    r["volume"] = safe_float(info.get("volume") or info.get("regularMarketVolume"))
    r["name"] = info.get("shortName") or info.get("longName") or ""
    r["exchange"] = info.get("exchange", "")
    r["currency"] = info.get("currency", "USD")

    # Market data
    r["pe_ratio"] = safe_float(info.get("trailingPE"))
    r["forward_pe"] = safe_float(info.get("forwardPE"))
    r["peg_ratio"] = safe_float(info.get("pegRatio"))
    r["market_cap"] = safe_float(info.get("marketCap"))
    r["52w_high"] = safe_float(info.get("fiftyTwoWeekHigh"))
    r["52w_low"] = safe_float(info.get("fiftyTwoWeekLow"))
    r["beta"] = safe_float(info.get("beta"))

    # Sector & Industry
    r["sector"] = info.get("sector") or None
    r["industry"] = info.get("industry") or None

    # Dividend yield (decimal → %)
    div_yield = safe_float(info.get("dividendYield"))
    if div_yield is not None:
        r["dividend_yield"] = round(div_yield * 100, 2)

    # Profit margin (decimal → %)
    pm = safe_float(info.get("profitMargins"))
    if pm is not None:
        r["profit_margin"] = round(pm * 100, 2)

    # Debt to equity (yfinance gives percentage, e.g. 150 → 1.5 ratio)
    de = safe_float(info.get("debtToEquity"))
    if de is not None:
        r["debt_equity"] = round(de / 100, 2)

    # ROE (decimal → %)
    roe = safe_float(info.get("returnOnEquity"))
    if roe is not None:
        r["roe"] = round(roe * 100, 2)

    # EPS growth
    eps_g = safe_float(info.get("earningsGrowth"))
    if eps_g is not None:
        r["eps_growth"] = round(eps_g * 100, 2)
    else:
        eps_g = safe_float(info.get("earningsQuarterlyGrowth"))
        if eps_g is not None:
            r["eps_growth"] = round(eps_g * 100, 2)

    # Revenue growth (decimal → %)
    rev_g = safe_float(info.get("revenueGrowth"))
    if rev_g is not None:
        r["revenue_growth"] = round(rev_g * 100, 2)

    # Analyst data
    r["analyst_target"] = safe_float(info.get("targetMeanPrice"))
    r["analyst_rating"] = safe_float(info.get("recommendationMean"))
    r["analyst_rating_label"] = info.get("recommendationKey") or None

    # Compute target upside %
    if r["analyst_target"] and r["price"] and r["price"] > 0:
        r["target_upside"] = round(((r["analyst_target"] - r["price"]) / r["price"]) * 100, 2)

    return r


# ---------------------------------------------------------------------------
# Sector statistics
# ---------------------------------------------------------------------------

def compute_sector_stats(all_stock_data):
    """
    Calculate median P/E, median profit margin, and median ROE per sector.
    all_stock_data is a list of metric dicts (output of extract_from_yfinance).
    Returns {sector_name: {median_pe, median_margin, median_roe, count}}.
    """
    sector_vals = {}
    for d in all_stock_data:
        sec = d.get("sector")
        if not sec:
            continue
        if sec not in sector_vals:
            sector_vals[sec] = {"pe": [], "margin": [], "roe": []}
        if d.get("pe_ratio") is not None and d["pe_ratio"] > 0:
            sector_vals[sec]["pe"].append(d["pe_ratio"])
        if d.get("profit_margin") is not None:
            sector_vals[sec]["margin"].append(d["profit_margin"])
        if d.get("roe") is not None:
            sector_vals[sec]["roe"].append(d["roe"])

    stats = {}
    for sec, vals in sector_vals.items():
        stats[sec] = {
            "median_pe": median(vals["pe"]) if vals["pe"] else None,
            "median_margin": median(vals["margin"]) if vals["margin"] else None,
            "median_roe": median(vals["roe"]) if vals["roe"] else None,
            "count": len(vals["pe"]),
        }
    return stats


# ---------------------------------------------------------------------------
# Scoring engine
# ---------------------------------------------------------------------------

def compute_signal(stock, sector_stats=None):
    """
    Comprehensive scoring (0-100) with four pillars:

    VALUE (25 pts)
      Sector-relative P/E  (15 pts)
      PEG Ratio             (10 pts)

    GROWTH (30 pts)
      EPS Growth            (15 pts)
      Revenue Growth        (15 pts)

    QUALITY (25 pts)
      Profit Margin         (10 pts)
      Return on Equity      (10 pts)
      Debt / Equity          (5 pts)

    OUTLOOK (20 pts)
      Dividend Yield         (5 pts)
      Analyst Rating        (10 pts)
      Target Price Upside    (5 pts)
    """

    value_score = 0
    growth_score = 0
    quality_score = 0
    outlook_score = 0
    details = {}
    has_any_data = False

    pe = stock.get("pe_ratio")
    forward_pe = stock.get("forward_pe")
    peg = stock.get("peg_ratio")
    eps_growth = stock.get("eps_growth")
    rev_growth = stock.get("revenue_growth")
    profit_margin = stock.get("profit_margin")
    roe = stock.get("roe")
    debt_equity = stock.get("debt_equity")
    div_yield = stock.get("dividend_yield")
    analyst_rating = stock.get("analyst_rating")
    target_upside = stock.get("target_upside")
    sector = stock.get("sector")

    # ---- VALUE PILLAR (25 pts) ----

    # Sector-relative P/E (15 pts)
    if pe is not None:
        has_any_data = True
        sector_median_pe = None
        if sector and sector_stats and sector in sector_stats:
            sector_median_pe = sector_stats[sector].get("median_pe")

        if sector_median_pe and sector_median_pe > 0:
            # Score relative to sector median
            ratio = pe / sector_median_pe
            if pe < 0:
                pe_score = 0
            elif ratio < 0.5:
                pe_score = 15
            elif ratio < 0.7:
                pe_score = 13
            elif ratio < 0.85:
                pe_score = 11
            elif ratio < 1.0:
                pe_score = 9
            elif ratio < 1.15:
                pe_score = 7
            elif ratio < 1.5:
                pe_score = 4
            elif ratio < 2.0:
                pe_score = 2
            else:
                pe_score = 0
        else:
            # Absolute scoring fallback
            if pe < 0:
                pe_score = 0
            elif pe < 10:
                pe_score = 15
            elif pe < 15:
                pe_score = 13
            elif pe < 20:
                pe_score = 10
            elif pe < 25:
                pe_score = 7
            elif pe < 35:
                pe_score = 4
            else:
                pe_score = 0
        value_score += pe_score
        details["pe_score"] = pe_score
        details["sector_median_pe"] = sector_median_pe

    # PEG Ratio (10 pts) — lower = growth at a reasonable price
    if peg is not None:
        has_any_data = True
        if peg < 0:
            peg_score = 0  # negative PEG means negative earnings/growth
        elif peg < 0.5:
            peg_score = 10
        elif peg < 1.0:
            peg_score = 9
        elif peg < 1.5:
            peg_score = 7
        elif peg < 2.0:
            peg_score = 5
        elif peg < 3.0:
            peg_score = 3
        else:
            peg_score = 0
        value_score += peg_score
        details["peg_score"] = peg_score

    # ---- GROWTH PILLAR (30 pts) ----

    # EPS Growth (15 pts)
    if eps_growth is not None:
        has_any_data = True
        if eps_growth > 30:
            eg_score = 15
        elif eps_growth > 20:
            eg_score = 13
        elif eps_growth > 15:
            eg_score = 11
        elif eps_growth > 10:
            eg_score = 9
        elif eps_growth > 5:
            eg_score = 6
        elif eps_growth > 0:
            eg_score = 3
        else:
            eg_score = 0
        growth_score += eg_score
        details["eps_growth_score"] = eg_score

    # Revenue Growth (15 pts)
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
        growth_score += rg_score
        details["revenue_growth_score"] = rg_score

    # ---- QUALITY PILLAR (25 pts) ----

    # Profit Margin (10 pts)
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
        quality_score += pm_score
        details["profit_margin_score"] = pm_score

    # Return on Equity (10 pts)
    if roe is not None:
        has_any_data = True
        if roe > 30:
            roe_score = 10
        elif roe > 20:
            roe_score = 8
        elif roe > 15:
            roe_score = 7
        elif roe > 10:
            roe_score = 5
        elif roe > 5:
            roe_score = 3
        elif roe > 0:
            roe_score = 1
        else:
            roe_score = 0
        quality_score += roe_score
        details["roe_score"] = roe_score

    # Debt/Equity (5 pts)
    if debt_equity is not None:
        has_any_data = True
        if debt_equity < 0.3:
            de_score = 5
        elif debt_equity < 0.5:
            de_score = 4
        elif debt_equity < 1.0:
            de_score = 3
        elif debt_equity < 1.5:
            de_score = 2
        elif debt_equity < 2.0:
            de_score = 1
        else:
            de_score = 0
        quality_score += de_score
        details["debt_equity_score"] = de_score

    # ---- OUTLOOK PILLAR (20 pts) ----

    # Dividend Yield (5 pts)
    if div_yield is not None:
        has_any_data = True
        if div_yield > 6:
            dy_score = 2  # very high yield = risk flag
        elif div_yield > 4:
            dy_score = 5
        elif div_yield > 3:
            dy_score = 4
        elif div_yield > 2:
            dy_score = 3
        elif div_yield > 1:
            dy_score = 2
        elif div_yield > 0:
            dy_score = 1
        else:
            dy_score = 0
        outlook_score += dy_score
        details["dividend_score"] = dy_score

    # Analyst Rating (10 pts) — 1=Strong Buy ... 5=Sell
    if analyst_rating is not None:
        has_any_data = True
        if analyst_rating <= 1.5:
            ar_score = 10
        elif analyst_rating <= 2.0:
            ar_score = 8
        elif analyst_rating <= 2.5:
            ar_score = 6
        elif analyst_rating <= 3.0:
            ar_score = 4
        elif analyst_rating <= 3.5:
            ar_score = 2
        else:
            ar_score = 0
        outlook_score += ar_score
        details["analyst_score"] = ar_score

    # Target Price Upside (5 pts)
    if target_upside is not None:
        has_any_data = True
        if target_upside > 30:
            tu_score = 5
        elif target_upside > 20:
            tu_score = 4
        elif target_upside > 10:
            tu_score = 3
        elif target_upside > 0:
            tu_score = 2
        elif target_upside > -10:
            tu_score = 1
        else:
            tu_score = 0
        outlook_score += tu_score
        details["target_upside_score"] = tu_score

    if not has_any_data:
        return 0, "NO DATA", details, {"value": 0, "growth": 0, "quality": 0, "outlook": 0}

    total = value_score + growth_score + quality_score + outlook_score
    pillar_scores = {
        "value": value_score,
        "growth": growth_score,
        "quality": quality_score,
        "outlook": outlook_score,
    }

    if total >= 70:
        signal = "STRONG BUY"
    elif total >= 55:
        signal = "BUY"
    elif total >= 40:
        signal = "HOLD"
    elif total >= 25:
        signal = "SELL"
    else:
        signal = "STRONG SELL"

    return total, signal, details, pillar_scores


# ---------------------------------------------------------------------------
# Market processing
# ---------------------------------------------------------------------------

def process_market(market_name, symbols):
    """Fetch data and build stock entries (scoring is done later with sector context)."""
    print(f"\n{'='*60}")
    print(f"Processing {market_name} ({len(symbols)} stocks)")
    print(f"{'='*60}")

    print(f"\nFetching data for {market_name} via yfinance...")
    stock_data = fetch_yfinance_data(symbols)
    print(f"  Got data for {len(stock_data)} / {len(symbols)} stocks")

    entries = []
    for symbol in symbols:
        metrics = stock_data.get(symbol)
        if not metrics or not metrics.get("price"):
            continue
        entries.append({
            "symbol": to_display_symbol(symbol),
            "raw_symbol": symbol,
            "market": market_name,
            **metrics,
        })

    return entries


def score_all_stocks(all_entries):
    """Score all stocks using sector-relative context."""
    # First pass: compute sector stats across ALL markets
    sector_stats = compute_sector_stats(all_entries)
    print(f"\nSector statistics computed for {len(sector_stats)} sectors:")
    for sec, stats in sorted(sector_stats.items(), key=lambda x: -x[1]["count"]):
        pe_str = f"median P/E={stats['median_pe']:.1f}" if stats["median_pe"] else "no P/E"
        print(f"  {sec}: {stats['count']} stocks, {pe_str}")

    # Second pass: score each stock with sector context
    for entry in all_entries:
        score, signal, details, pillar_scores = compute_signal(entry, sector_stats)
        entry["score"] = score
        entry["signal"] = signal
        entry["score_details"] = details
        entry["value_score"] = pillar_scores["value"]
        entry["growth_score"] = pillar_scores["growth"]
        entry["quality_score"] = pillar_scores["quality"]
        entry["outlook_score"] = pillar_scores["outlook"]

    return sector_stats


def build_sector_breakdown(all_stocks, sector_stats):
    """Build sector-level summary for the dashboard."""
    sectors = {}
    for s in all_stocks:
        sec = s.get("sector") or "Unknown"
        if sec not in sectors:
            sectors[sec] = {
                "name": sec,
                "stocks": [],
                "count": 0,
                "avg_score": 0,
                "signals": {},
                "median_pe": None,
                "median_margin": None,
                "median_roe": None,
            }
        sectors[sec]["stocks"].append({
            "symbol": s["symbol"],
            "market": s["market"],
            "score": s["score"],
            "signal": s["signal"],
        })
        sectors[sec]["count"] += 1
        sig = s["signal"]
        sectors[sec]["signals"][sig] = sectors[sec]["signals"].get(sig, 0) + 1

    for sec_name, sec_data in sectors.items():
        scores = [st["score"] for st in sec_data["stocks"]]
        sec_data["avg_score"] = round(sum(scores) / len(scores), 1) if scores else 0
        # Sort stocks by score desc and keep top 5
        sec_data["stocks"].sort(key=lambda x: x["score"], reverse=True)
        sec_data["top_stocks"] = sec_data["stocks"][:5]
        del sec_data["stocks"]  # don't duplicate full list
        # Pull in sector stats
        if sec_name in sector_stats:
            sec_data["median_pe"] = round(sector_stats[sec_name]["median_pe"], 1) if sector_stats[sec_name]["median_pe"] else None
            sec_data["median_margin"] = round(sector_stats[sec_name]["median_margin"], 1) if sector_stats[sec_name]["median_margin"] else None
            sec_data["median_roe"] = round(sector_stats[sec_name]["median_roe"], 1) if sector_stats[sec_name]["median_roe"] else None

    return sectors


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def send_email_alert(all_stocks):
    email_user = os.environ.get("EMAIL_USER", "")
    email_pass = os.environ.get("EMAIL_PASS", "")
    email_to = os.environ.get("EMAIL_TO", "")
    if not all([email_user, email_pass, email_to]):
        print("\nEmail credentials not set, skipping alert.")
        return

    strong_buys = [s for s in all_stocks if s["signal"] == "STRONG BUY"]
    strong_sells = [s for s in all_stocks if s["signal"] == "STRONG SELL"]
    buys = [s for s in all_stocks if s["signal"] == "BUY"]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    body = f"Market Dashboard Scan — {now}\n{'='*50}\n\n"

    if strong_buys:
        body += f"STRONG BUY ({len(strong_buys)} stocks):\n{'-'*40}\n"
        for s in strong_buys[:10]:
            body += f"  {s['symbol']:12s} | {s['market']:8s} | Score: {s['score']:3d} | {s.get('sector','N/A')}\n"
            body += f"    P/E: {s.get('pe_ratio','N/A')} | PEG: {s.get('peg_ratio','N/A')} | ROE: {s.get('roe','N/A')}% | Analyst: {s.get('analyst_rating_label','N/A')}\n"
        body += "\n"
    if buys:
        body += f"BUY ({len(buys)} stocks):\n{'-'*40}\n"
        for s in buys[:10]:
            body += f"  {s['symbol']:12s} | {s['market']:8s} | Score: {s['score']:3d}\n"
        body += "\n"
    if strong_sells:
        body += f"STRONG SELL ({len(strong_sells)} stocks):\n{'-'*40}\n"
        for s in strong_sells[:10]:
            body += f"  {s['symbol']:12s} | {s['market']:8s} | Score: {s['score']:3d}\n"
        body += "\n"

    body += f"\nTotal stocks scanned: {len(all_stocks)}\n"
    body += "View full dashboard: https://vishwasndn.github.io/market-dashboard/\n"

    import smtplib
    from email.mime.text import MIMEText
    msg = MIMEText(body)
    msg["Subject"] = f"Market Dashboard — {len(strong_buys)} Strong Buys | {now}"
    msg["From"] = email_user
    msg["To"] = email_to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(email_user, email_pass)
            server.sendmail(email_user, email_to, msg.as_string())
        print(f"\nEmail alert sent to {email_to}")
    except Exception as e:
        print(f"\nEmail error: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"Market Dashboard Scanner v2 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Enhanced: Sector Analysis + Growth Potential + Sector-Relative Scoring")
    print(f"Data source: yfinance (Yahoo Finance)\n")

    try:
        import yfinance
        print(f"yfinance version: {yfinance.__version__}")
    except ImportError:
        print("ERROR: yfinance not installed! Run: pip install yfinance")
        sys.exit(1)

    all_entries = []

    # Fetch data for each market (scoring happens later with sector context)
    nasdaq_entries = process_market("NASDAQ", NASDAQ_TOP_100[:50])
    all_entries.extend(nasdaq_entries)

    bse_entries = process_market("BSE", BSE_TOP_100[:50])
    all_entries.extend(bse_entries)

    uae_entries = process_market("UAE", UAE_TOP_STOCKS)
    all_entries.extend(uae_entries)

    # Score all stocks using sector-relative context
    sector_stats = score_all_stocks(all_entries)

    # Sort by score
    all_entries.sort(key=lambda x: x.get("score", 0), reverse=True)

    # Print summary per market
    for mkt in ["NASDAQ", "BSE", "UAE"]:
        mkt_stocks = [s for s in all_entries if s["market"] == mkt]
        if not mkt_stocks:
            continue
        print(f"\n  {mkt}: {len(mkt_stocks)} stocks")
        signal_counts = {}
        for s in mkt_stocks:
            signal_counts[s["signal"]] = signal_counts.get(s["signal"], 0) + 1
        for sig, cnt in sorted(signal_counts.items()):
            print(f"    {sig}: {cnt}")

    # Build sector breakdown
    sector_breakdown = build_sector_breakdown(all_entries, sector_stats)

    # Save results
    scan_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Clean up entries for JSON (remove raw_symbol)
    for entry in all_entries:
        entry.pop("raw_symbol", None)

    output = {
        "scan_time": scan_time,
        "total_stocks": len(all_entries),
        "version": 2,
        "markets": {
            "NASDAQ": [s for s in all_entries if s["market"] == "NASDAQ"],
            "BSE": [s for s in all_entries if s["market"] == "BSE"],
            "UAE": [s for s in all_entries if s["market"] == "UAE"],
        },
        "sectors": sector_breakdown,
        "summary": {
            "strong_buy": len([s for s in all_entries if s["signal"] == "STRONG BUY"]),
            "buy": len([s for s in all_entries if s["signal"] == "BUY"]),
            "hold": len([s for s in all_entries if s["signal"] == "HOLD"]),
            "sell": len([s for s in all_entries if s["signal"] == "SELL"]),
            "strong_sell": len([s for s in all_entries if s["signal"] == "STRONG SELL"]),
            "no_data": len([s for s in all_entries if s["signal"] == "NO DATA"]),
        },
    }

    os.makedirs("data", exist_ok=True)
    with open("data/dashboard.json", "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nSaved data/dashboard.json ({len(all_entries)} stocks)")

    # History
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
        "total": len(all_entries),
        "strong_buy": output["summary"]["strong_buy"],
        "buy": output["summary"]["buy"],
        "hold": output["summary"]["hold"],
        "sell": output["summary"]["sell"],
        "strong_sell": output["summary"]["strong_sell"],
        "top_buys": [{"symbol": s["symbol"], "market": s["market"], "score": s["score"], "sector": s.get("sector")}
                     for s in sorted(all_entries, key=lambda x: x["score"], reverse=True)[:5]],
    })
    history = history[-90:]
    with open(history_file, "w") as f:
        json.dump(history, f, indent=2, default=str)
    print(f"Updated history ({len(history)} entries)")

    send_email_alert(all_entries)
    print(f"\nDone!")


if __name__ == "__main__":
    main()
