import streamlit as st
import pandas as pd
import sys
import asyncio
import re

import json
import tempfile
# DB setup
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATASET_ID = st.secrets["DATASET_ID"]
SERVICE_ACCOUNT_JSON = dict(st.secrets["SERVICE_ACCOUNT_JSON"])

# --- WINDOWS ASYNCIO FIX ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from yahoo_finance import run_comprehensive_analysis
from finviz import scrape_finviz
from gurufocus_moat import get_moat_score
from EPS_growth import get_forward_eps_growth
from iv_rank import get_iv_rank_advanced
from simply_wall_street import scrape_risk_rewards
# Integration of the LLM module
from LLM import analyze_ticker

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Stock Insight Pro",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)


# --- PERSISTENT HEADER LOGIC ---
# This function handles the reset logic to go back to the landing page
def reset_to_home():
    st.session_state.report_data = None
    st.session_state.risk_reward_data = None
    st.session_state.llm_analysis = None
    st.session_state.current_ticker = ""
    st.rerun()


# --- PERSISTENT HEADER UI ---
st.markdown("""
    <div class="custom-header">
        <div class="header-button-container">
            <a href="/?nav=new" target="_self" style="text-decoration: none;">
                <button class="btn-new">New Analyses</button>
            </a>
            <button class="btn-past">Past Analysis</button>
        </div>
    </div>

    <style>
        /* Fix the header to the top */
        .custom-header {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 60px;
            background-color: white;
            display: flex;
            align-items: center;
            z-index: 999999;
            border-bottom: 1px solid #eeeeee;
            padding-left: 10%;
        }

        .header-button-container {
            display: flex;
            gap: 15px;
        }

        /* Styling for "New Analysis" - Blue background, black text */
        .btn-new {
            background-color: #007bff;
            color: black !important;
            border: none;
            padding: 8px 20px;
            border-radius: 6px;
            font-weight: 600;
            cursor: pointer;
        }

        /* Styling for "Past Analysis" - Black background, white text */
        .btn-past {
            background-color: black;
            color: white !important;
            border: none;
            padding: 8px 20px;
            border-radius: 6px;
            font-weight: 600;
            cursor: default;
        }

        /* Adjust main content padding to prevent header overlap */
        .stApp {
            margin-top: 60px;
        }

        /* Hide the default streamlit header to make our custom one look better */
        header[data-testid="stHeader"] {
            display: none;
        }
    </style>
""", unsafe_allow_html=True)

# --- NAVIGATION LOGIC ---
# Check if the URL contains the trigger to reset
if st.query_params.get("nav") == "new":
    st.query_params.clear()  # Clear the URL param
    reset_to_home()











# --- PREMIUM UI STYLING ---
st.markdown("""
<style>
    header {visibility: hidden;}
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stApp { background-color: #ffffff; }
    .block-container { padding-top: 1rem !important; max-width: 1200px; }
    .main-header { text-align: center; margin-bottom: 2rem; color: #1e1e1e; }
    .stTextInput > div > div > input { color: #1e1e1e !important; background-color: #ffffff !important; }
    .stButton > button { 
        color: #ffffff !important; 
        background-color: #007bff !important; 
        width: 100%; 
        border-radius: 8px; 
        font-weight: bold;
        transition: 0.3s;
    }
    .stButton > button:hover { background-color: #0056b3 !important; }
    [data-testid="stMetricValue"] { color: #1e1e1e !important; }
    [data-testid="stMetricLabel"] { color: #555555 !important; }
    .stMarkdown, p, span, h1, h2, h3 { color: #1e1e1e !important; }
    hr { border: 0.5px solid #eeeeee !important; }
    .report-card { 
        background-color: #f8f9fa; 
        padding: 20px; 
        border-radius: 12px; 
        border: 1px solid #e9ecef; 
        margin-bottom: 20px; 
        white-space: pre-wrap; 
    }
    .reward-text { color: #28a745 !important; font-weight: 500; }
    .risk-text { color: #dc3545 !important; font-weight: 500; }
</style>
""", unsafe_allow_html=True)


def format_ticker(ticker):
    return ticker.strip().upper()


def parse_llm_response(text):
    """Utility to extract specific sections from the LLM output."""
    data = {
        "description": "N/A",
        "value_proposition": "N/A",
        "moat": "N/A",
        "ceo_ownership": "N/A",
        "classification": "N/A"
    }

    if not text or not isinstance(text, str):
        return data

    # Extract Description
    desc_match = re.search(r"Company Description:\s*(.*?)(?=\n\n|\nValue Proposition:)", text, re.DOTALL)
    if desc_match: data["description"] = desc_match.group(1).strip()

    # Extract Value Proposition
    vp_match = re.search(r"Value Proposition:\s*(.*?)(?=\n\n|\nMoat Analysis:)", text, re.DOTALL)
    if vp_match: data["value_proposition"] = vp_match.group(1).strip()

    # Extract Moat
    moat_match = re.search(r"Moat Analysis:\s*(.*?)(?=\n\n|\nCEO Ownership:)", text, re.DOTALL)
    if moat_match: data["moat"] = moat_match.group(1).strip()

    # Extract CEO Ownership
    own_match = re.search(r"Ownership Percentage:\s*(.*?)(?=\nSource:|\n\n|\nFinal Classification:)", text, re.DOTALL)
    if own_match: data["ceo_ownership"] = own_match.group(1).strip()

    # Extract Classification
    class_match = re.search(r"Category:\s*(.*?)(?=\nPoints:|\n\n|$)", text, re.DOTALL)
    if class_match: data["classification"] = class_match.group(1).strip()

    return data


def clean_val(val):
    """Helper to convert various string formats to float."""
    if val is None or str(val).lower() == 'n/a' or str(val).lower() == 'none' or str(val).lower() == 'error':
        return 0.0
    try:
        # Remove %, $, commas, and non-numeric chars except . and -
        s = re.sub(r'[^\d.-]', '', str(val))
        return float(s) if s else 0.0
    except:
        return 0.0


def calculate_scoring(metric_name, value):
    """The central scoring logic block with robust keyword matching."""
    obtained = 0
    total = 0
    is_rejected = False

    val_num = clean_val(value)
    name_low = str(metric_name).lower()
    val_str_low = str(value).lower()

    if "runway" in name_low:
        total = 10

        # Check if the value is N/A, None, or Error
        if val_str_low in ["n/a", "none", "error"]:
            obtained = 3
        else:

            is_positive_cash = any(
                phrase in val_str_low for phrase in ["positive", "no cash burn", "no burn", "profitable"])

            if is_positive_cash:
                obtained = 10
            elif val_num >= 24:
                obtained = 10
            elif 12 <= val_num < 24:
                obtained = 7
            elif 6 <= val_num < 12:
                obtained = 3
            elif val_num > 0:

                if "month" in val_str_low:
                    is_rejected = True
                else:

                    obtained = 10
            else:
                is_rejected = True

        # Net Debt / EBITDA (FIXED LOGIC)
    elif "net debt / ebitda" in name_low:
        total = 7
        net_debt_val = clean_val(st.session_state.get("net_debt_val"))
        ebitda_val = clean_val(st.session_state.get("ebitda_val"))
        ratio_val = val_num

        if net_debt_val < 0:
            obtained = 7
        elif ebitda_val <= 0:
            is_rejected = True
        elif ratio_val == 0:
            obtained = 7
        elif 0 < ratio_val <= 1.5:
            obtained = 5
        elif 1.5 < ratio_val <= 3:
            obtained = 3
        else:
            is_rejected = True

    # Net Debt / EBITDA
    elif "net debt / ebitda" in name_low:
        total = 7

        net_debt_val = clean_val(st.session_state.get("net_debt_val"))
        ebitda_val = clean_val(st.session_state.get("ebitda_val"))
        ratio_val = val_num  # this metric’s own value

        if net_debt_val < 0:
            obtained = 7

        elif ebitda_val <= 0:
            is_rejected = True

        elif ratio_val == 0:
            obtained = 7
        elif 0 < ratio_val <= 1.5:
            obtained = 5
        elif 1.5 < ratio_val <= 3:
            obtained = 3
        else:
            is_rejected = True

        # Assets / Liabilities
    elif "assets" in name_low and "liabilities" in name_low:
        total = 5
        if val_str_low in ["n/a", "none", "error"]:
            obtained = 1
        elif val_num >= 2.0:
            obtained = 5
        elif 1.5 <= val_num < 2.0:
            obtained = 3
        elif 1.0 <= val_num < 1.5:
            obtained = 1
        else:
            is_rejected = True

    # Cash Burn Severity
    elif "burn" in name_low:
        total = 3

        if val_str_low in ["n/a", "none", "error"]:
            obtained = 1
        elif val_num <= 0:
            obtained = 3
        elif val_num < 10:
            obtained = 2
        elif 10 <= val_num <= 20:
            obtained = 1
        else:
            is_rejected = True

    # share count growth
    elif "share count" in name_low:
        total = 3
        if val_str_low in ["n/a", "none", "error"]:
            obtained = 1
        elif val_num <= 0:
            obtained = 3
        elif val_num < 5:
            obtained = 2
        elif 5 <= val_num <= 10:
            obtained = 1
        else:
            is_rejected = True

        # Latest Expiration Detail
    elif "expiration" in name_low:
        total = 0
        obtained = ""
        is_rejected = False # Explicitly ensure it's False at start

        if val_str_low in ["n/a", "none", "error", ""]:
            is_rejected = True
        else:
            try:
                # Use pd.to_datetime to be more flexible with formats (handles - or /)
                exp_date = pd.to_datetime(value).date()
                today = datetime.date.today()

                diff_months = (exp_date.year - today.year) * 12 + (exp_date.month - today.month)

                if diff_months < 18:
                    is_rejected = True
                else:
                    obtained = "Pass" # Give it a value so you know it cleared
            except:
                # Only reject if it's truly not a date
                is_rejected = True

    # Capital Structure pressure
    elif "capital structure" in name_low:
        total = 2
        v = str(value).lower()

        if v in ["n/a", "none", "error"]:
            obtained = 1
        elif "no convert" in v or "0" in v:
            obtained = 2
        elif "minor" in v:
            obtained = 1
        elif "heavy" in v or "atm" in v:
            is_rejected = True
        else:
            obtained = 0

    # Market Cap
    elif "market cap" in name_low:
        total = 5
        mcap_val = val_str_low
        numeric_part = clean_val(value)
        true_billions = numeric_part
        if 't' in mcap_val:
            true_billions = numeric_part * 1000
        elif 'm' in mcap_val:
            true_billions = numeric_part / 1000
        elif 'b' in mcap_val:
            true_billions = numeric_part

        if true_billions < 2:
            obtained = 5
        elif 2 <= true_billions <= 5:
            obtained = 3
        else:
            is_rejected = True

    # Forward EPS Growth
    elif "eps growth" in name_low:
        total = 7

        if val_str_low in ["n/a", "none", "error"]:
            obtained = 1
        elif val_num >= 30:
            obtained = 7
        elif 20 <= val_num < 30:
            obtained = 5
        elif 10 <= val_num < 20:
            obtained = 3
        else:
            obtained = 0

    # Degree of Operating Leverage
    elif "operating leverage" in name_low or "(dol)" in name_low:
        total = 5
        if val_num >= 3.0:
            obtained = 5
        elif 2.0 <= val_num < 3.0:
            obtained = 4
        elif 1.5 <= val_num < 2.0:
            obtained = 2
        else:
            obtained = 0

    # IV Rank
    elif "iv rank" in name_low:
        total = 3
        if val_str_low in ["n/a", "none", "error"]:
            obtained = 1
        elif val_num < 30:
            obtained = 3
        elif 30 <= val_num <= 60:
            obtained = 2
        else:
            obtained = 0

    # Short Float
    elif "short float" in name_low:
        total = 4
        if val_str_low in ["n/a", "none", "error"]:
            obtained = 1
        elif 10 <= val_num <= 30:
            obtained = 4
        elif 5 <= val_num < 10:
            obtained = 2
        elif val_num < 5:
            obtained = 1
        elif val_num > 30:
            obtained = 0

    # Institutional Ownership
    elif "institutional ownership" in name_low:
        total = 3
        if val_str_low in ["n/a", "none", "error"]:
            obtained = 1
        elif val_num < 40:
            obtained = 3
        elif 40 <= val_num <= 60:
            obtained = 2
        else:
            obtained = 1

    # Total Insider Ownership
    elif "total insider ownership" in name_low:
        total = 6
        if val_str_low in ["n/a", "none", "error"]:
            obtained = 2
        elif 5 <= val_num <= 30:
            obtained = 6
        elif 2 <= val_num < 5:
            obtained = 4
        elif 1 <= val_num < 2:
            obtained = 2
        elif val_num < 1:
            obtained = 0
        elif val_num > 30:
            obtained = 3


    # CEO Ownership
    elif "ceo ownership" in name_low:
        total = 3
        if "not disclosed" in val_str_low or val_str_low in ["n/a", "none", "error"]:
            obtained = 1
        elif val_num >= 5:
            obtained = 3
        elif 2 <= val_num < 5:
            obtained = 2
        elif 1 <= val_num < 2:
            obtained = 1
        else:
            obtained = 0


    elif "buying vs selling" in name_low:
        total = 4
        if val_str_low in ["n/a", "none", "error"]:
            obtained = 1
        elif val_num > 1:
            obtained = 4
        elif 0 < val_num < 1:
            obtained = 2
        elif val_num == 0:
            obtained = 1
        elif val_num < 0:
            obtained = 0

    # Moat Score
    elif "moat score" in name_low:
        total = 15
        if val_num >= 4:
            obtained = 15
        elif val_num >= 3:
            obtained = 9
        elif val_num >= 2:
            obtained = 5
        else:
            obtained = 0

    # Business Model
    elif "business model" in name_low:
        total = 15
        v = str(value).lower()
        if "mission-critical" in v or "infrastructure" in v:
            obtained = 15
        elif "saas" in v or "platform" in v or "high switching" in v:
            obtained = 10
        elif "commodity" in v:
            obtained = 5
        else:
            obtained = 0

    return obtained, total, is_rejected


async def run_parallel_analysis(ticker):
    """Executes all data fetching functions in parallel."""
    ALPHA_VANTAGE_KEY = st.secrets["ALPHA_VANTAGE_API_KEY_1"]

    # Run all tasks concurrently using asyncio.to_thread for synchronous functions
    tasks = [
        asyncio.to_thread(run_comprehensive_analysis, ticker),
        asyncio.to_thread(scrape_finviz, ticker),
        asyncio.to_thread(get_moat_score, ticker),
        asyncio.to_thread(analyze_ticker, ticker),
        asyncio.to_thread(scrape_risk_rewards, ticker),
        asyncio.to_thread(get_iv_rank_advanced, ticker),
        asyncio.to_thread(get_forward_eps_growth, ticker, ALPHA_VANTAGE_KEY)
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results


# db function
def save_analysis_to_bigquery(ticker, report_data, risk_reward, llm_data):
    """
    Creates a BigQuery table named after the Ticker.
    Deletes existing table for the ticker first.
    Columns: Matric name, Source, Value, Obtained Score, Total score, LLM.
    """
    ticker_clean = ticker.strip().upper().replace("-", "_").replace(".", "_")
    table_id = f"{DATASET_ID}.{ticker_clean}"

    # start block
    try:
        if "SERVICE_ACCOUNT_JSON" not in st.secrets:
            sys.stderr.write("ERROR: 'SERVICE_ACCOUNT_JSON' not found in st.secrets\n")
            return "N/A"
        service_info = dict(st.secrets["SERVICE_ACCOUNT_JSON"])

        # 2. Fix the private key (Handle Streamlit's newline escaping)
        if "private_key" in service_info:
            service_info["private_key"] = service_info["private_key"].replace("\\n", "\n")
        else:
            sys.stderr.write("ERROR: 'private_key' missing from service account info\n")
            return "N/A"

        # 3. Initialize BigQuery Client
        try:
            credentials = service_account.Credentials.from_service_account_info(service_info)
            client = bigquery.Client(credentials=credentials, project=service_info.get("project_id"))
        except Exception as e:
            sys.stderr.write(f"ERROR: BigQuery Authentication failed: {e}\n")
            return "N/A"
        # end block

        # 2. USE THE CORRECT AUTH METHOD
        # credentials = service_account.Credentials.from_service_account_info(info)
        # client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        # 2. DELETE EXISTING TABLE
        client.delete_table(table_id, not_found_ok=True)
        logger.info(f"Existing table {table_id} deleted or not found.")

        # 3. DEFINE SCHEMA & CREATE TABLE
        schema = [
            bigquery.SchemaField("Matric name", "STRING"),
            bigquery.SchemaField("Source", "STRING"),
            bigquery.SchemaField("Value", "STRING"),
            bigquery.SchemaField("Obtained Score", "STRING"),
            bigquery.SchemaField("Total score", "STRING"),
            bigquery.SchemaField("LLM", "STRING"),
        ]

        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")



        # --- NEW CODE: WAIT FOR TABLE TO PROPAGATE ---
        import time
        for _ in range(5): # Try for 10 seconds total
            try:
                client.get_table(table_id) # Check if table exists yet
                break
            except:
                time.sleep(2)
        # ---------------------------------------------

        # 4. PREPARE DATA ROWS
        rows_to_insert = []

        # Standard Metrics (LLM column blank)
        for row in report_data:
            if row["Metric Name"] == "TOTAL":
                continue
            rows_to_insert.append({
                "Matric name": row["Metric Name"],
                "Source": row["Source"],
                "Value": str(row["Value"]),
                "Obtained Score": str(row["Obtained points"]),
                "Total score": str(row["Total points"]),
                "LLM": None
            })

        # Risks (LLM column filled, others blank)
        risks_text = "\n".join(risk_reward.get("risks", []))
        rows_to_insert.append({
            "Matric name": "Risks",
            "Source": None, "Value": None, "Obtained Score": None, "Total score": None,
            "LLM": risks_text
        })

        # Rewards (LLM column filled, others blank)
        rewards_text = "\n".join(risk_reward.get("rewards", []))
        rows_to_insert.append({
            "Matric name": "Rewards",
            "Source": None, "Value": None, "Obtained Score": None, "Total score": None,
            "LLM": rewards_text
        })

        # Company Analysis Sections (LLM column filled, others blank)
        llm_sections = [
            ("Company Description", "description"),
            ("Value Proposition", "value_proposition"),
            ("Moat Analysis", "moat")
        ]
        for label, key in llm_sections:
            rows_to_insert.append({
                "Matric name": label,
                "Source": None, "Value": None, "Obtained Score": None, "Total score": None,
                "LLM": llm_data.get(key, "N/A")
            })

        # Final Row: Date Today
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        rows_to_insert.append({
            "Matric name": "DATE",
            "Source": None, "Value": None, "Obtained Score": None, "Total score": None,
            "LLM": today_str
        })

        # 5. INSERT DATA
        errors = client.insert_rows_json(table_id, rows_to_insert)

        if errors == []:
            logger.info(f"SUCCESS: Data inserted into BigQuery table {ticker_clean}.")
            return True
        else:
            logger.error(f"BigQuery Insert Errors: {errors}")
            return False

    except Exception as e:
        logger.error(f"BIGQUERY DATABASE ERROR for {ticker}: {e}")
        return False
    # end of function


def main():
    if 'report_data' not in st.session_state:
        st.session_state.report_data = None
    if 'risk_reward_data' not in st.session_state:
        st.session_state.risk_reward_data = None
    if 'llm_analysis' not in st.session_state:
        st.session_state.llm_analysis = None
    if 'current_ticker' not in st.session_state:
        st.session_state.current_ticker = ""

    # --- HOME VIEW ---
    if st.session_state.report_data is None:
        st.markdown(
            '<div class="main-header"><h1>Stock Insight Pro</h1><p>Institutional-Grade Stock Analysis Engine</p></div>',
            unsafe_allow_html=True
        )

        _, center_col, _ = st.columns([1, 1.5, 1])
        with center_col:
            ticker_input = st.text_input("Ticker", placeholder="e.g. TSLA, NVDA", key="ticker_box",
                                         label_visibility="collapsed")
            if st.button("Generate Comprehensive Report") and ticker_input:
                ticker = format_ticker(ticker_input)
                status_box = st.empty()
                status_box.info(f"Analyzing {ticker}. Running all institutional checks in parallel...")

                # Run parallel analysis
                results = asyncio.run(run_parallel_analysis(ticker))

                # Unpack results safely
                (
                    analysis_results,
                    finviz_results,
                    moat_score,
                    raw_llm_text,
                    sws_data,
                    iv_rank_result,
                    eps_growth_val
                ) = results

                # Handle potential exceptions/errors in parallel results
                if isinstance(raw_llm_text, Exception): raw_llm_text = ""
                llm_parsed = parse_llm_response(raw_llm_text)

                if isinstance(sws_data, Exception): sws_data = {"rewards": [], "risks": []}
                if isinstance(iv_rank_result, Exception): iv_rank_result = "Error"
                if isinstance(eps_growth_val, Exception): eps_growth_val = None
                if isinstance(moat_score, Exception): moat_score = 0

                if not isinstance(analysis_results, Exception) and analysis_results.get("status") == "success":
                    table_rows = []

                    # 1. Yahoo Finance Data
                    source_yf = "Yahoo Finance"
                    data_yf = analysis_results["data"]

                    for section_name, metrics in data_yf.items():
                        for metric_name, value in metrics.items():

                            # ---- CAPTURE ND + EBITDA FOR 3-METRIC LOGIC ----
                            name_low = metric_name.lower()
                            if name_low == "net debt":
                                st.session_state["net_debt_val"] = value
                            elif name_low == "ebitda":
                                st.session_state["ebitda_val"] = value
                            # ------------------------------------------------

                            points, total_pts, rejected = calculate_scoring(metric_name, value)

                            pts_str = "rejected" if rejected else (str(points) if total_pts > 0 else "")
                            total_pts_str = str(total_pts) if total_pts > 0 else ""

                            table_rows.append({
                                "Metric Name": metric_name,
                                "Source": source_yf,
                                "Value": str(value),
                                "Obtained points": pts_str,
                                "Total points": total_pts_str
                            })

                    # 2. Finviz Data
                    source_fv = "Finviz"
                    finviz_metrics_to_show = ["Net Insider Buying vs Selling (%)", "Net Insider Activity",
                                              "Institutional Ownership (%)", "Short Float (%)"]
                    if finviz_results and not isinstance(finviz_results, Exception) and "error" not in finviz_results:
                        for metric_key in finviz_metrics_to_show:
                            if metric_key in finviz_results:
                                val = str(finviz_results[metric_key])
                                points, total_pts, rejected = calculate_scoring(metric_key, val)
                                pts_str = "rejected" if rejected else (str(points) if total_pts > 0 else "")
                                total_pts_str = str(total_pts) if total_pts > 0 else ""
                                table_rows.append({
                                    "Metric Name": metric_key,
                                    "Source": source_fv,
                                    "Value": val,
                                    "Obtained points": pts_str,
                                    "Total points": total_pts_str
                                })

                    # 3. GuruFocus
                    m_val = str(moat_score)
                    points, total_pts, rejected = calculate_scoring("GuruFocus Moat Score", m_val)
                    table_rows.append({
                        "Metric Name": "GuruFocus Moat Score",
                        "Source": "GuruFocus",
                        "Value": m_val,
                        "Obtained points": "rejected" if rejected else str(points),
                        "Total points": str(total_pts)
                    })

                    # 4. Alpha Vantage
                    eps_display = f"{eps_growth_val:.2f}%" if eps_growth_val is not None else "N/A"
                    points, total_pts, rejected = calculate_scoring("Forward EPS Growth (%)", eps_display)
                    table_rows.append({
                        "Metric Name": "Forward EPS Growth (%)",
                        "Source": "Alpha Vantage",
                        "Value": eps_display,
                        "Obtained points": "rejected" if rejected else str(points),
                        "Total points": str(total_pts)
                    })

                    # 5. IV Rank
                    iv_display_value = "N/A"
                    if isinstance(iv_rank_result, str) and "Success!" in iv_rank_result:
                        iv_display_value = iv_rank_result.split(":")[-1].strip()
                    elif isinstance(iv_rank_result, str) and "Error" in iv_rank_result:
                        iv_display_value = "Error"

                    points, total_pts, rejected = calculate_scoring("IV Rank", iv_display_value)
                    table_rows.append({
                        "Metric Name": "IV Rank",
                        "Source": "Unusual Whales",
                        "Value": iv_display_value,
                        "Obtained points": "rejected" if rejected else str(points),
                        "Total points": str(total_pts)
                    })

                    # 6. LLM Integrated Metrics
                    ceo_val = llm_parsed["ceo_ownership"]
                    points, total_pts, rejected = calculate_scoring("CEO Ownership %", ceo_val)
                    table_rows.append({
                        "Metric Name": "CEO Ownership %",
                        "Source": "Perplexity",
                        "Value": ceo_val,
                        "Obtained points": "rejected" if rejected else str(points),
                        "Total points": str(total_pts)
                    })

                    bm_val = llm_parsed["classification"]
                    points, total_pts, rejected = calculate_scoring("Business Model & Value Proposition", bm_val)
                    table_rows.append({
                        "Metric Name": "Business Model & Value Proposition",
                        "Source": "Perplexity",
                        "Value": bm_val,
                        "Obtained points": "rejected" if rejected else str(points),
                        "Total points": str(total_pts)
                    })

                    # Add Total Row
                    sum_obtained = 0
                    sum_total = 0
                    for row in table_rows:
                        if row["Obtained points"] != "rejected" and row["Obtained points"] != "":
                            sum_obtained += float(row["Obtained points"])
                        if row["Total points"] != "":
                            sum_total += float(row["Total points"])

                    table_rows.append({
                        "Metric Name": "TOTAL",
                        "Source": "",
                        "Value": "",
                        "Obtained points": str(sum_obtained),
                        "Total points": str(sum_total)
                    })

                    st.session_state.report_data = table_rows
                    st.session_state.risk_reward_data = sws_data
                    st.session_state.llm_analysis = llm_parsed
                    st.session_state.current_ticker = ticker
                    status_box.empty()
                    st.rerun()
                else:
                    err_msg = analysis_results.get("error") if not isinstance(analysis_results, Exception) else str(
                        analysis_results)
                    status_box.error(f"Analysis failed: {err_msg}")

    # --- REPORT VIEW ---
    else:

        st.markdown("---")

        # SECTION 1: FINANCIAL METRICS
        if st.session_state.report_data:
            st.subheader("Financial Metrics")
            df = pd.DataFrame(st.session_state.report_data)
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Metric Name": st.column_config.TextColumn(width="medium"),
                    "Source": st.column_config.TextColumn(width="small"),
                    "Value": st.column_config.TextColumn(width="small"),
                    "Obtained points": st.column_config.TextColumn(width="small"),
                    "Total points": st.column_config.TextColumn(width="small")
                }
            )

        st.markdown("<br>", unsafe_allow_html=True)

        # --- SUMMARY TABLE ---
        if st.session_state.report_data:
            st.subheader("Score Summary & Verdict")

            def get_pts(keyword1, keyword2=None):
                for r in st.session_state.report_data:
                    n_low = r["Metric Name"].lower()
                    if keyword2:
                        if keyword1 in n_low and keyword2 in n_low:
                            return r["Obtained points"]
                    else:
                        if keyword1 in n_low:
                            return r["Obtained points"]
                return "0"

            m_pts = {
                "runway": get_pts("runway"),
                "nd_ebitda": get_pts("net debt", "ebitda"),
                "al_ratio": get_pts("assets", "liabilities"),
                "expiration": get_pts("expiration"), #newly added
                "burn": get_pts("burn"),
                "share_growth": get_pts("share count"),
                "cap_struct": get_pts("capital structure"),
                "mcap": get_pts("market cap"),
                "eps_growth": get_pts("eps growth"),
                "op_lev": get_pts("operating leverage") if "0" != get_pts("operating leverage") else get_pts("(dol)"),
                "iv_rank": get_pts("iv rank"),
                "short": get_pts("short float"),
                "inst": get_pts("institutional ownership"),
                "total_insider": get_pts("total insider ownership"),
                "ceo_own": get_pts("ceo ownership"),
                "ins_buy": get_pts("buying vs selling"),
                "moat": get_pts("moat score"),
                "biz_model": get_pts("business model")
            }

            has_rejected = any(v == "rejected" for v in m_pts.values())

            def to_f(v):
                return float(v) if v not in ["rejected", ""] else 0.0

            s1 = to_f(m_pts["runway"]) + to_f(m_pts["nd_ebitda"]) + to_f(m_pts["al_ratio"]) + to_f(
                m_pts["burn"]) + to_f(m_pts["share_growth"]) + to_f(m_pts["cap_struct"])
            s2 = to_f(m_pts["mcap"]) + to_f(m_pts["eps_growth"]) + to_f(m_pts["op_lev"]) + to_f(
                m_pts["iv_rank"]) + to_f(m_pts["short"]) + to_f(m_pts["inst"])
            s3 = to_f(m_pts["total_insider"]) + to_f(m_pts["ceo_own"]) + to_f(m_pts["ins_buy"])
            s4 = to_f(m_pts["moat"]) + to_f(m_pts["biz_model"])
            final_score = s1 + s2 + s3 + s4

            verdict = ""
            if has_rejected:
                verdict = "Rejected"
            else:
                if final_score >= 80:
                    verdict = "🔥 Elite LEAPS Candidate"
                elif 70 <= final_score < 80:
                    verdict = "✅ Qualified"
                elif 60 <= final_score < 70:
                    verdict = "⚠️ Watchlist"
                else:
                    verdict = "❌ Reject"

            summary_df = pd.DataFrame([{
                "Ticker": st.session_state.current_ticker,
                "Financial Survival & Balance Sheet": s1,
                "Growth & Asymmetric Upside": s2,
                "Insider Alignment & Behavior": s3,
                "Moat & Qualitative Conviction": s4,
                "Final Score": final_score,
                "Verdict": verdict
            }])

            st.dataframe(summary_df, use_container_width=True, hide_index=True)
            st.markdown("<br>", unsafe_allow_html=True)

        # SECTION 2: RISK AND REWARDS
        st.subheader("Risk and Rewards")
        if st.session_state.risk_reward_data:
            rewards = st.session_state.risk_reward_data.get("rewards", [])
            risks = st.session_state.risk_reward_data.get("risks", [])
            if not rewards and not risks:
                st.info("No specific Risk or Reward data identified for this ticker.")
            else:
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("#### ✅ Rewards")
                    if rewards:
                        for item in rewards:
                            st.markdown(f'<p class="reward-text">• {item}</p>', unsafe_allow_html=True)
                    else:
                        st.write("No specific rewards identified.")
                with col2:
                    st.markdown("#### ⚠️ Risks")
                    if risks:
                        for item in risks:
                            st.markdown(f'<p class="risk-text">• {item}</p>', unsafe_allow_html=True)
                    else:
                        st.write("No significant risks identified.")
        else:
            st.warning("Risk and Reward data unavailable.")

        st.markdown("---")

        # SECTION 3: COMPANY OVERVIEW & MOAT
        st.subheader("Business Profile & Strategic Analysis")
        if st.session_state.llm_analysis:
            llm_data = st.session_state.llm_analysis
            st.markdown("#### 🏢 Company Description")
            st.markdown(f'<div class="report-card">{llm_data["description"]}</div>', unsafe_allow_html=True)
            st.markdown("#### 💎 Value Proposition")
            st.markdown(f'<div class="report-card">{llm_data["value_proposition"]}</div>', unsafe_allow_html=True)
            st.markdown("#### 🛡️ Moat Analysis")
            st.markdown(f'<div class="report-card">{llm_data["moat"]}</div>', unsafe_allow_html=True)
        else:
            st.info("Additional business analysis is currently unavailable.")

        st.markdown("---")

        # db function call
        # --- TRIGGER DB SAVE ---
        # Place this at the very end of the 'else:' (Report View) block
        success = save_analysis_to_bigquery(
            st.session_state.current_ticker,
            st.session_state.report_data,
            st.session_state.risk_reward_data,
            st.session_state.llm_analysis
        )
        if success:
            st.success(f"✅ Table '{st.session_state.current_ticker}' successfully updated in Google BigQuery.")
            # db function call end


if __name__ == "__main__":
    main()