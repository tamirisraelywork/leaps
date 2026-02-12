import yfinance as yf
import pandas as pd
from datetime import datetime
import time
import logging
import streamlit as st
import requests
import json
import sys
import os

# --- API Key from Secrets ---
ALPHA_VANTAGE_KEY = st.secrets["ALPHA_VANTAGE_API_KEY_3"]

# --- Configured logging to track errors and retries ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def format_large_number(num):
    """
    Converts numbers to strings in Millions, Billions, or Trillions.
    """
    if num is None or not isinstance(num, (int, float)):
        return "N/A"

    abs_num = abs(num)
    if abs_num >= 1_000_000_000_000:
        return f"{num / 1_000_000_000_000:.2f} Trillion"
    elif abs_num >= 1_000_000_000:
        return f"{num / 1_000_000_000:.2f} Billion"
    elif abs_num >= 1_000_000:
        return f"{num / 1_000_000:.2f} Million"
    else:
        return f"{num:.2f}"

def get_latest_metric(df, possible_keys):
    """
    Searches for the first matching key in the dataframe and returns
    the value from the most recent period.
    """
    if df is None or df.empty:
        return None, None
    for key in possible_keys:
        if key in df.index:
            try:
                val = df.loc[key].iloc[0]
                if pd.notnull(val):
                    return val, key
            except (IndexError, AttributeError):
                continue
    return None, None

def run_comprehensive_analysis(ticker_symbol):
    # Proxy Configuration from first code
    PROXY_USER = st.secrets["PROXY_USER"]
    PROXY_PASS = st.secrets["PROXY_PASS"]
    PROXY_HOST = "gw.dataimpulse.com"
    PROXY_PORT = "823"
    proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    
    # Proxy dictionary for requests (Alpha Vantage)
    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }

    max_retries = 2  # Total 2 attempts as requested
    retry_count = 0

    results = {"ticker": ticker_symbol, "status": "success", "data": {}, "error": None}

    # Initialize variables for the final report
    current_price = None
    market_cap = None
    low_52 = None
    high_52 = None
    latest_expiry = "N/A"
    insider_val = "N/A"
    total_assets = None
    total_liabilities = None
    al_ratio = None
    runway_val = "N/A"
    ebitda = None
    net_debt_raw = None
    nd_ebitda_val = "N/A"
    severity_val = "N/A"
    share_growth_val = "N/A"
    dol_val = "N/A"
    csp_status = "No converts / ATM"
    shares_outstanding = None

    # Helper to clean Alpha Vantage string values from second code
    def av_clean(val):
        try:
            return float(val) if val and str(val).lower() != "none" else 0.0
        except (ValueError, TypeError):
            return 0.0

    while retry_count < max_retries:
        try:
            # Logging attempt with proxy info as in first code
            logging.info(f"Attempt {retry_count + 1} for {ticker_symbol} using proxy {proxy_url}")
            
            ticker = yf.Ticker(ticker_symbol)

            # Fetching Info and Dataframes
            info = ticker.info
            q_balance_sheet = ticker.quarterly_balance_sheet
            a_balance_sheet = ticker.balance_sheet
            q_cash_flow = ticker.quarterly_cashflow
            a_financials = ticker.financials

            # 1. Price, Low, High, Market Cap (YFinance primary)
            current_price = info.get('currentPrice') or info.get('regularMarketPrice')
            market_cap = info.get('marketCap')
            shares_outstanding = info.get('sharesOutstanding')
            low_52 = info.get('fiftyTwoWeekLow')
            high_52 = info.get('fiftyTwoWeekHigh')

            # --- Alpha Vantage Backup for Price/Cap/Shares/Range (from second code) ---
            if not current_price or not market_cap:
                logging.info(f"Price/Cap missing in YF for {ticker_symbol}. Checking Alpha Vantage...")
                try:
                    ov_url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    ov_resp = requests.get(ov_url, proxies=proxies, timeout=15)
                    ov_data = ov_resp.json()
                    
                    if not current_price:
                        gq_url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                        gq_resp = requests.get(gq_url, proxies=proxies, timeout=15)
                        gq_data = gq_resp.json().get("Global Quote", {})
                        current_price = av_clean(gq_data.get("05. price"))
                    
                    if not market_cap:
                        market_cap = av_clean(ov_data.get("MarketCapitalization"))
                    if not shares_outstanding:
                        shares_outstanding = av_clean(ov_data.get("SharesOutstanding"))
                    if not low_52:
                        low_52 = av_clean(ov_data.get("52WeekLow"))
                    if not high_52:
                        high_52 = av_clean(ov_data.get("52WeekHigh"))
                except Exception as e:
                    logging.error(f"AV Price Backup Error for {ticker_symbol}: {e}")

            # 4. Latest expiration date
            try:
                options = ticker.options
                latest_expiry = options[-1] if options else "N/A"
            except Exception:
                latest_expiry = "N/A"

            # 5. Total insider ownership % (YF primary, AV Backup from second code)
            insider_own_pct = info.get('heldPercentInsiders')
            if insider_own_pct is None:
                try:
                    ov_url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    ov_resp = requests.get(ov_url, proxies=proxies, timeout=15)
                    ov_data = ov_resp.json()
                    insider_own_pct = av_clean(ov_data.get("PercentInsiders")) / 100.0 if ov_data.get("PercentInsiders") else None
                except:
                    pass
            insider_val = f"{insider_own_pct * 100:.2f}%" if insider_own_pct is not None else "N/A"

            # 6. Total Assets & Liabilities (YF primary)
            total_assets, _ = get_latest_metric(q_balance_sheet, ['Total Assets'])
            total_liabilities, _ = get_latest_metric(q_balance_sheet, [
                'Total Liabilities Net Minor Interest', 'Total Liab', 'Total Liabilities'
            ])

            # Fallback for Liabilities (YFinance specific logic from code 1)
            if total_liabilities is None:
                curr_l, _ = get_latest_metric(q_balance_sheet, ['Current Liabilities', 'Total Current Liabilities'])
                non_curr_l, _ = get_latest_metric(q_balance_sheet, [
                    'Total Non Current Liabilities Net Minority Interest', 'Non Current Liabilities'
                ])
                if curr_l is not None or non_curr_l is not None:
                    total_liabilities = (curr_l or 0) + (non_curr_l or 0)

            # --- Alpha Vantage Fallback for Assets/Liabilities (from second code) ---
            if total_assets is None or total_liabilities is None:
                try:
                    av_bs_url = f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    av_bs_resp = requests.get(av_bs_url, proxies=proxies, timeout=15)
                    av_bs_data = av_bs_resp.json()
                    reports = av_bs_data.get("quarterlyReports", [])
                    if reports:
                        if total_assets is None:
                            total_assets = av_clean(reports[0].get("totalAssets"))
                        if total_liabilities is None:
                            total_liabilities = av_clean(reports[0].get("totalLiabilities"))
                except:
                    pass

            # 7. Assets / Liabilities Ratio
            if total_assets and total_liabilities and total_liabilities != 0:
                al_ratio = round(total_assets / total_liabilities, 2)

            # 8. Runway (Quarterly Cash / Monthly Burn)
            current_cash, _ = get_latest_metric(q_balance_sheet, [
                'Cash And Cash Equivalents', 'Cash Cash Equivalents And Short Term Investments'
            ])
            quarterly_ocf, _ = get_latest_metric(q_cash_flow, ['Operating Cash Flow'])

            # --- Alpha Vantage Fallback for Runway (from second code) ---
            if current_cash is None or quarterly_ocf is None:
                try:
                    av_bs_url = f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    av_cf_url = f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    if current_cash is None:
                        av_bs_resp = requests.get(av_bs_url, proxies=proxies, timeout=15)
                        current_cash = av_clean(av_bs_resp.json().get("quarterlyReports", [{}])[0].get("cashAndCashEquivalentsAtCarryingValue"))
                    if quarterly_ocf is None:
                        av_cf_resp = requests.get(av_cf_url, proxies=proxies, timeout=15)
                        quarterly_ocf = av_clean(av_cf_resp.json().get("quarterlyReports", [{}])[0].get("operatingCashflow"))
                except:
                    pass

            if current_cash is not None and quarterly_ocf is not None:
                if quarterly_ocf < 0:
                    monthly_burn = abs(quarterly_ocf) / 3
                    runway_val = f"{current_cash / monthly_burn:.2f} Months"
                else:
                    runway_val = "Positive OCF (No Burn)"

            # 9. Net Debt / EBITDA
            ebitda, _ = get_latest_metric(a_financials, ['EBITDA', 'Normalized EBITDA'])
            net_debt_raw, _ = get_latest_metric(a_balance_sheet, ['Net Debt'])

            if net_debt_raw is None:
                total_debt, _ = get_latest_metric(a_balance_sheet, ['Total Debt'])
                cash_comp, _ = get_latest_metric(a_balance_sheet, ['Cash And Cash Equivalents'])
                if total_debt is not None and cash_comp is not None:
                    net_debt_raw = total_debt - cash_comp

            # --- Comprehensive Alpha Vantage Fallback for EBITDA and Net Debt (from second code) ---
            if ebitda is None or net_debt_raw is None:
                logging.info(f"EBITDA/Net Debt missing for {ticker_symbol} in Yahoo. Querying Alpha Vantage...")
                try:
                    av_bs_url = f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    av_inc_url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"

                    if ebitda is None:
                        inc_resp = requests.get(av_inc_url, proxies=proxies, timeout=15)
                        ebitda = av_clean(inc_resp.json().get("annualReports", [{}])[0].get("ebitda"))

                    if net_debt_raw is None:
                        bs_resp = requests.get(av_bs_url, proxies=proxies, timeout=15)
                        report = bs_resp.json().get("annualReports", [{}])[0]
                        av_cash = av_clean(report.get("cashAndCashEquivalentsAtCarryingValue"))
                        av_st_debt = av_clean(report.get("shortTermDebt"))
                        av_lt_debt = av_clean(report.get("longTermDebt"))
                        net_debt_raw = (av_st_debt + av_lt_debt) - av_cash
                except Exception as av_err:
                    print(f"Alpha Vantage Debt/EBITDA backup failed for {ticker_symbol}: {str(av_err)}", file=sys.stderr)

            if ebitda is not None and ebitda != 0 and net_debt_raw is not None:
                nd_ebitda_val = round(net_debt_raw / ebitda, 2)

            # 10. Cash Burn Severity
            fcf_ttm = None
            if q_cash_flow is not None and 'Free Cash Flow' in q_cash_flow.index:
                fcf_ttm = q_cash_flow.loc['Free Cash Flow'].iloc[:4].sum()

            # --- Alpha Vantage Fallback for FCF (from second code) ---
            if fcf_ttm is None:
                try:
                    av_cf_url = f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    cf_resp = requests.get(av_cf_url, proxies=proxies, timeout=15)
                    q_reports = cf_resp.json().get("quarterlyReports", [])[:4]
                    if q_reports:
                        fcf_ttm = sum([av_clean(r.get("operatingCashflow")) - av_clean(r.get("capitalExpenditures")) for r in q_reports])
                except:
                    pass

            if market_cap and fcf_ttm is not None and fcf_ttm < 0:
                severity_val = f"{(abs(fcf_ttm) / market_cap) * 100:.2f}%"
            elif fcf_ttm is not None and fcf_ttm >= 0:
                severity_val = "0.00% (Positive FCF)"

            # 11. Share Count Growth (Calculation exactly same as in first code)
            try:
                shares_data = ticker.get_shares_full(start=datetime.now() - pd.DateOffset(years=5))
                if shares_data is not None and not shares_data.empty:
                    shares_data = shares_data.sort_index().iloc[~shares_data.index.duplicated(keep='last')]
                    if len(shares_data) > 1:
                        latest_idx = -1
                        target_date = shares_data.index[latest_idx] - pd.DateOffset(years=3)
                        idx_3y = shares_data.index.get_indexer([target_date], method='nearest')[0]
                        if idx_3y != -1 and idx_3y < (len(shares_data) + latest_idx):
                            latest_s = shares_data.iloc[latest_idx]
                            hist_s = shares_data.iloc[idx_3y]
                            years_diff = (shares_data.index[latest_idx] - shares_data.index[idx_3y]).days / 365.25
                            if (pd.notnull(latest_s) and pd.notnull(hist_s) and
                                    hist_s > 0 and latest_s > 0 and years_diff > 0):
                                cagr = ((latest_s / hist_s) ** (1 / years_diff)) - 1
                                share_growth_val = f"{cagr * 100:.2f}%"
            except Exception:
                share_growth_val = "N/A"

            # 12. Degree of Operating Leverage (DOL)
            if a_financials is not None and a_financials.shape[1] >= 2 and 'Total Revenue' in a_financials.index:
                sales = a_financials.loc['Total Revenue']
                ebit_v, ebit_k = get_latest_metric(a_financials, ['EBIT', 'Operating Income'])
                if ebit_v is not None:
                    ebit_row = a_financials.loc[ebit_k]
                    pct_sales = (sales.iloc[0] - sales.iloc[1]) / abs(sales.iloc[1]) if sales.iloc[1] != 0 else 0
                    pct_ebit = (ebit_row.iloc[0] - ebit_row.iloc[1]) / abs(ebit_row.iloc[1]) if ebit_row.iloc[1] != 0 else 0
                    if pct_sales != 0:
                        dol_val = round(pct_ebit / pct_sales, 2)

            # --- Alpha Vantage Fallback for DOL (from second code) ---
            if dol_val == "N/A":
                try:
                    av_inc_url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    inc_resp = requests.get(av_inc_url, proxies=proxies, timeout=15)
                    reports = inc_resp.json().get("annualReports", [])
                    if len(reports) >= 2:
                        s1, s2 = av_clean(reports[0].get("totalRevenue")), av_clean(reports[1].get("totalRevenue"))
                        e1, e2 = av_clean(reports[0].get("operatingIncome")), av_clean(reports[1].get("operatingIncome"))
                        p_sales = (s1 - s2) / abs(s2) if s2 != 0 else 0
                        p_ebit = (e1 - e2) / abs(e2) if e2 != 0 else 0
                        if p_sales != 0:
                            dol_val = round(p_ebit / p_sales, 2)
                except:
                    pass

            # 13. Capital Structure Pressure (CSP)
            debt_to_equity = info.get('debtToEquity', 0)
            
            # --- Alpha Vantage Fallback for DebtToEquity (from second code) ---
            if not debt_to_equity:
                try:
                    ov_url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    ov_resp = requests.get(ov_url, proxies=proxies, timeout=15)
                    debt_to_equity = av_clean(ov_resp.json().get("DebtToEquityRatio")) * 100
                except:
                    pass

            convert_labels = []
            if a_balance_sheet is not None:
                convert_labels = [idx for idx in a_balance_sheet.index if 'convertible' in str(idx).lower()]

            has_converts = len(convert_labels) > 0
            convert_val = a_balance_sheet.loc[convert_labels[0]].iloc[0] if has_converts else 0

            if (debt_to_equity and debt_to_equity > 300):
                csp_status = "Heavy converts / ATM"
            elif has_converts:
                dilution_overhang = (convert_val / market_cap) if market_cap and market_cap > 0 else 0
                if dilution_overhang > 0.05 or (debt_to_equity and debt_to_equity > 150):
                    csp_status = "Heavy converts / ATM"
                else:
                    csp_status = "Minor converts"
            elif debt_to_equity and debt_to_equity > 100:
                csp_status = "Heavy converts / ATM"

            # Formatting results as per final_metrics dictionary in both codes
            final_metrics = {
                "Current stock price": f"{current_price:.2f}" if current_price else "N/A",
                "Market cap": format_large_number(market_cap),
                "Shares Outstanding": format_large_number(shares_outstanding),
                "52 week low": f"{low_52:.2f}" if low_52 else "N/A",
                "52 weeks high": f"{high_52:.2f}" if high_52 else "N/A",
                "latest expiration date": latest_expiry,
                "Total insider ownership %": insider_val,
                "Total Assets": format_large_number(total_assets),
                "Total Liabilities": format_large_number(total_liabilities),
                "Assets / Liabilities Ratio": al_ratio if al_ratio is not None else "N/A",
                "Runway": runway_val,
                "Net Debt": format_large_number(net_debt_raw),
                "EBITDA": format_large_number(ebitda),
                "Net Debt / EBITDA": nd_ebitda_val,
                "Cash Burn Severity": severity_val,
                "Share Count Growth": share_growth_val,
                "Degree of Operating Leverage": dol_val,
                "Capital Structure Pressure": csp_status
            }

            results["data"] = {"Summary": final_metrics}
            return results

        except Exception as e:
            retry_count += 1
            logging.error(f"Error on attempt {retry_count} for {ticker_symbol} using proxy: {str(e)}")

            if retry_count < max_retries:
                logging.info(f"Retrying with proxy in 2 seconds...")
                time.sleep(2)
            else:
                results["status"] = "error"
                results["error"] = f"Final failure for {ticker_symbol} after {max_retries} attempts via proxy: {str(e)}"
                return results
