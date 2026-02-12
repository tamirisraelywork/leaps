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

# --- DATABASE HELPERS (FROM DB.PY) ---
MASTER_TABLE_NAME = "master_table"


@st.cache_resource(show_spinner=False)
def get_bigquery_client_history():
    try:
        if "SERVICE_ACCOUNT_JSON" not in st.secrets:
            sys.stderr.write("ERROR: 'SERVICE_ACCOUNT_JSON' not found in st.secrets\n")
            return None
        service_info = dict(st.secrets["SERVICE_ACCOUNT_JSON"])

        if "private_key" in service_info:
            service_info["private_key"] = service_info["private_key"].replace("\\n", "\n")
        else:
            sys.stderr.write("ERROR: 'private_key' missing from service account info\n")
            return None

        try:
            credentials = service_account.Credentials.from_service_account_info(service_info)
            client = bigquery.Client(credentials=credentials, project=service_info.get("project_id"))
            return client
        except Exception as e:
            sys.stderr.write(f"ERROR: BigQuery Authentication failed: {e}\n")
            return None
    except Exception as e:
        st.error(f"Auth Error: {e}")
        return None


@st.cache_data(ttl=300, show_spinner=False)
def get_master_data():
    """Fetches the ticker, date, score, and verdict from the master_table."""
    client = get_bigquery_client_history()
    if not client or not DATASET_ID: return pd.DataFrame()

    try:
        if "." in DATASET_ID:
            table_path = f"{DATASET_ID}.{MASTER_TABLE_NAME}"
        else:
            table_path = f"{client.project}.{DATASET_ID}.{MASTER_TABLE_NAME}"

        query = f"SELECT Ticker, date, Score, Verdict FROM `{table_path}`"
        job = client.query(query)
        return job.to_dataframe()
    except Exception as e:
        st.error(f"Error fetching master_table: {e}")
        return pd.DataFrame()


def delete_ticker_table(ticker):
    """Deletes the specific ticker table and removes the entry from master_table."""
    client = get_bigquery_client_history()
    if client:
        try:
            if "." in DATASET_ID:
                ticker_table_id = f"{DATASET_ID}.{ticker}"
                master_table_id = f"{DATASET_ID}.{MASTER_TABLE_NAME}"
            else:
                ticker_table_id = f"{client.project}.{DATASET_ID}.{ticker}"
                master_table_id = f"{client.project}.{DATASET_ID}.{MASTER_TABLE_NAME}"

            client.delete_table(ticker_table_id, not_found_ok=True)

            delete_query = f"DELETE FROM `{master_table_id}` WHERE Ticker = '{ticker}'"
            client.query(delete_query).result()

            st.cache_data.clear()
            return True
        except Exception as e:
            st.error(f"Error deleting resources for {ticker}: {e}")
            return False
    return False


@st.cache_data(ttl=300, show_spinner=False)
def get_ticker_detail_data(ticker):
    client = get_bigquery_client_history()
    if not client: return pd.DataFrame()
    try:
        if "." in DATASET_ID:
            full_table_path = f"{DATASET_ID}.{ticker}"
        else:
            full_table_path = f"{client.project}.{DATASET_ID}.{ticker}"

        query = f"SELECT * FROM `{full_table_path}`"
        job = client.query(query)
        return job.to_dataframe()
    except Exception as e:
        st.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()


def safe_float(val):
    if val is None or str(val).lower() in ['n/a', 'none', 'rejected', '', 'nan']:
        return 0.0
    try:
        cleaned = re.sub(r'[^\d.-]', '', str(val))
        return float(cleaned) if cleaned else 0.0
    except:
        return 0.0


# --- PERSISTENT HEADER LOGIC ---
def reset_to_home():
    st.session_state.report_data = None
    st.session_state.risk_reward_data = None
    st.session_state.llm_analysis = None
    st.session_state.current_ticker = ""
    # Reset history view states
    st.session_state.db_view = 'history'
    st.session_state.selected_ticker = None
    st.session_state.active_page = 'new'
    st.rerun()


# --- INITIALIZE STATE ---
if 'active_page' not in st.session_state:
    st.session_state.active_page = 'new'
if 'db_view' not in st.session_state:
    st.session_state.db_view = 'history'
if 'selected_ticker' not in st.session_state:
    st.session_state.selected_ticker = None

# --- NAVIGATION CONTROLLER ---
nav_param = st.query_params.get("nav")
if nav_param == "new":
    st.query_params.clear()
    reset_to_home()
elif nav_param == "past":
    st.session_state.active_page = 'past'
    st.query_params.clear()

# --- PERSISTENT HEADER UI ---
st.markdown(f"""
    <div class="custom-header">
        <div class="header-button-container">
            <a href="/?nav=new" target="_self" style="text-decoration: none;">
                <button class="btn-new">New Analysis</button>
            </a>
            <a href="/?nav=past" target="_self" style="text-decoration: none;">
                <button class="btn-past">Past Analyses</button>
            </a>
        </div>
    </div>

    <style>
        .custom-header {{
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
        }}

        .header-button-container {{
            display: flex;
            gap: 15px;
        }}

        /* Styling for "New Analysis" - Blue background, black text */
        .btn-new {{
            background-color: #007bff;
            color: black !important;
            border: none;
            padding: 8px 20px;
            border-radius: 6px;
            font-weight: 600;
            cursor: pointer;
            opacity: {1.0 if st.session_state.active_page == 'new' else 0.6};
        }}

        /* Styling for "Past Analysis" - Black background, white text */
        .btn-past {{
            background-color: black;
            color: white !important;
            border: none;
            padding: 8px 20px;
            border-radius: 6px;
            font-weight: 600;
            cursor: pointer;
            opacity: {1.0 if st.session_state.active_page == 'past' else 0.6};
        }}

        .stApp {{
            margin-top: 60px;
        }}

        header[data-testid="stHeader"] {{
            display: none;
        }}
    </style>
""", unsafe_allow_html=True)

# --- PREMIUM UI STYLING ---
st.markdown("""
<style>
    /* Force white background */
    .stApp { 
        background-color: #ffffff !important; 
    }

    /* Force ALL text to black */
    .stMarkdown, p, span, h1, h2, h3, h4, h5, h6, label, .main-header { 
        color: #000000 !important; 
    }

    /* "Draw" the table with visible borders and a clean background */
    .stTable, table {
        width: 100%;
        border-collapse: collapse;
        background-color: #ffffff !important;
        color: #000000 !important;
        border: 1px solid #dee2e6 !important;
    }

    /* Style the table cells and headers */
    .stTable td, .stTable th, table td, table th {
        border: 1px solid #dee2e6 !important;
        padding: 12px !important;
        color: #000000 !important;
        text-align: left;
    }

    /* Ensure table headers have a slightly different feel */
    .stTable th, table th {
        background-color: #f8f9fa !important;
        font-weight: bold;
    }

    .stButton > button { 
        border-radius: 5px; 
        background-color: black !important; 
    }

    /* Specifically target the text inside the button to override the global black rule */
    .stButton button p {
        color: white !important;
    }

    .reward-text { color: #28a745 !important; font-weight: bold; }
    .risk-text { color: #dc3545 !important; font-weight: bold; }

    /* Additional Premium Styling */
    .block-container { padding-top: 1rem !important; max-width: 1200px; }
    .main-header { text-align: center; margin-bottom: 2rem; }
    hr { border: 0.5px solid #eeeeee !important; }
    .report-card { 
        background-color: #f8f9fa; 
        padding: 20px; 
        border-radius: 12px; 
        border: 1px solid #e9ecef; 
        margin-bottom: 20px; 
        white-space: pre-wrap; 
    }
</style>
""", unsafe_allow_html=True)

# --- PAGE ROUTING LOGIC ---
if st.session_state.active_page == 'past':
    # --- DB.PY UI CONTENT ---
    if st.session_state.db_view == 'history':
        st.markdown("<h1 class='main-header'>Past Analyses</h1>", unsafe_allow_html=True)
        search_query = st.text_input("Search Ticker", placeholder="Enter ticker name...").upper()
        master_df = get_master_data()
        if not master_df.empty:
            if search_query:
                master_df = master_df[master_df['Ticker'].str.upper().str.contains(search_query)]
            cols = st.columns([1, 2.5, 2.2, 1.5, 2, 1, 1])
            cols[0].write("**Index**")
            cols[1].write("**Ticker Name**")
            cols[2].write("**Analysis date**")
            cols[3].write("**Total Score**")
            cols[4].write("**Verdict**")
            cols[5].write("**View**")
            cols[6].write("**Delete**")
            st.divider()
            for idx, row in master_df.iterrows():
                ticker = row['Ticker']
                score = row['Score']
                verdict = row['Verdict']
                analysis_date = row.get('date', 'N/A')
                row_cols = st.columns([1, 2.5, 2.2, 1.5, 2, 1, 1])
                row_cols[0].write(idx + 1)
                row_cols[1].write(f"**{ticker}**")
                row_cols[2].write(f"{analysis_date}")
                row_cols[3].write(f"{score}")
                row_cols[4].write(f"{verdict}")
                if row_cols[5].button("👁️", key=f"view_{ticker}"):
                    st.session_state.selected_ticker = ticker
                    st.session_state.db_view = 'detail'
                    st.rerun()
                if row_cols[6].button("🗑️", key=f"del_{ticker}"):
                    if delete_ticker_table(ticker):
                        st.toast(f"Deleted {ticker}")
                        st.rerun()
        else:
            st.info(f"No records found in '{MASTER_TABLE_NAME}'.")
            if st.button("Refresh List"):
                st.cache_data.clear()
                st.rerun()
    elif st.session_state.db_view == 'detail':
        ticker = st.session_state.selected_ticker
        df = get_ticker_detail_data(ticker)
        if st.button("Back to History"):
            st.session_state.db_view = 'history'
            st.rerun()
        if df.empty:
            st.error(f"No detailed data found for ticker: {ticker}.")
        else:
            st.markdown(f"<h1 style='text-align: center;'>Analysis: {ticker}</h1>", unsafe_allow_html=True)
            m_col = 'Matric name' if 'Matric name' in df.columns else 'Metric Name'
            date_row = df[df[m_col].str.upper() == 'DATE'] if m_col in df.columns else pd.DataFrame()
            date_val = date_row['LLM'].iloc[0] if not date_row.empty else "N/A"
            st.markdown(f"<h3 style='text-align: center;'>Analysis Date: {date_val}</h3>", unsafe_allow_html=True)
            qual_metrics = ["Risks", "Rewards", "Company Description", "Value Proposition", "Moat Analysis", "DATE"]
            metrics_df = df[~df[m_col].isin(qual_metrics)].copy()
            s_col = 'Obtained Score' if 'Obtained Score' in df.columns else 'Obtained points'
            t_col = 'Total score' if 'Total score' in df.columns else 'Total points'
            display_cols = [m_col, "Source", "Value", s_col, t_col]
            available_display_cols = [c for c in display_cols if c in df.columns]
            sum_obtained = metrics_df[s_col].apply(safe_float).sum() if s_col in metrics_df.columns else 0
            total_row = pd.DataFrame(
                [{m_col: "Total Score", "Source": "", "Value": "", s_col: int(round(sum_obtained)), t_col: 100}])
            table_to_show = pd.concat([metrics_df[available_display_cols], total_row], ignore_index=True)
            st.subheader("Financial Metrics")
            st.table(table_to_show)
            st.markdown("### Summary")


            def get_score(metric_list):
                val = df[df[m_col].isin(metric_list)][s_col].apply(safe_float).sum() if s_col in df.columns else 0
                return int(round(val))


            s1_metrics = ["Runway", "Net Debt / EBITDA", "Assets / Liabilities Ratio", "Cash Burn Severity",
                          "Share Count Growth", "Capital Structure Pressure"]
            s2_metrics = ["Market cap", "Forward EPS Growth (%)", "Degree of Operating Leverage", "IV Rank",
                          "Short Float (%)", "Institutional Ownership (%)"]
            s3_metrics = ["Total insider ownership %", "CEO Ownership %", "Net Insider Buying vs Selling (%)"]
            s4_metrics = ["GuruFocus Moat Score", "Business Model & Value Proposition"]
            s1 = get_score(s1_metrics);
            s2 = get_score(s2_metrics);
            s3 = get_score(s3_metrics);
            s4 = get_score(s4_metrics)
            final_score = s1 + s2 + s3 + s4
            score_series = df[s_col].astype(str).str.lower() if s_col in df.columns else pd.Series([])
            is_rejected = "rejected" in score_series.values
            if is_rejected:
                verdict = "❌ Rejected"
            else:
                if final_score >= 80:
                    verdict = "🔥 Elite LEAPS Candidate"
                elif final_score >= 70:
                    verdict = "✅ Qualified"
                elif final_score >= 60:
                    verdict = "⚠️ Watchlist"
                else:
                    verdict = "❌ Reject"
            summary_data = [
                {"Ticker": ticker, "Financial Survival & Balance Sheet": s1, "Growth & Asymmetric Upside": s2,
                 "Insider Alignment & Behavior": s3, "Moat & Qualitative Conviction": s4, "Final Score": final_score,
                 "Verdict": verdict}]
            st.table(pd.DataFrame(summary_data))


            def get_llm_text(metric_name):
                res = df[df[m_col] == metric_name]['LLM'] if m_col in df.columns and 'LLM' in df.columns else pd.Series(
                    [])
                return res.iloc[0] if not res.empty and pd.notnull(res.iloc[0]) else "N/A"


            st.markdown("#### 💰 Rewards")
            st.markdown(f'<p class="reward-text">{get_llm_text("Rewards")}</p>', unsafe_allow_html=True)
            st.markdown("#### 🚨 Risks")
            st.markdown(f'<p class="risk-text">{get_llm_text("Risks")}</p>', unsafe_allow_html=True)
            st.markdown("#### 🏭 Company Description")
            st.write(get_llm_text("Company Description"))
            st.markdown("#### 🤝 Value Proposition")
            st.write(get_llm_text("Value Proposition"))
            st.markdown("#### 🛡️ Moat Analysis")
            st.write(get_llm_text("Moat Analysis"))

else:
    # --- ORIGINAL MAIN APP LOGIC (NEW ANALYSIS) ---
    def format_ticker(ticker):
        return ticker.strip().upper()


    def parse_llm_response(text):
        data = {"description": "N/A", "value_proposition": "N/A", "moat": "N/A", "ceo_ownership": "N/A",
                "classification": "N/A"}
        if not text or not isinstance(text, str): return data
        desc_match = re.search(r"Company Description:\s*(.*?)(?=\n\n|\nValue Proposition:)", text, re.DOTALL)
        if desc_match: data["description"] = desc_match.group(1).strip()
        vp_match = re.search(r"Value Proposition:\s*(.*?)(?=\n\n|\nMoat Analysis:)", text, re.DOTALL)
        if vp_match: data["value_proposition"] = vp_match.group(1).strip()
        moat_match = re.search(r"Moat Analysis:\s*(.*?)(?=\n\n|\nCEO Ownership:)", text, re.DOTALL)
        if moat_match: data["moat"] = moat_match.group(1).strip()
        own_match = re.search(r"Ownership Percentage:\s*(.*?)(?=\nSource:|\n\n|\nFinal Classification:)", text,
                              re.DOTALL)
        if own_match: data["ceo_ownership"] = own_match.group(1).strip()
        class_match = re.search(r"Category:\s*(.*?)(?=\nPoints:|\n\n|$)", text, re.DOTALL)
        if class_match: data["classification"] = class_match.group(1).strip()
        return data


    def clean_val(val):
        if val is None or str(val).lower() == 'n/a' or str(val).lower() == 'none' or str(
            val).lower() == 'error': return 0.0
        try:
            s = re.sub(r'[^\d.-]', '', str(val))
            return float(s) if s else 0.0
        except:
            return 0.0


    def calculate_scoring(metric_name, value):
        obtained = 0;
        total = 0;
        is_rejected = False
        val_num = clean_val(value)
        name_low = str(metric_name).lower()
        val_str_low = str(value).lower()
        if "runway" in name_low:
            total = 10
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
        elif "net debt / ebitda" in name_low:
            total = 7;
            net_debt_val = clean_val(st.session_state.get("net_debt_val"));
            ebitda_val = clean_val(st.session_state.get("ebitda_val"));
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
        elif "expiration" in name_low:
            total = 0;
            obtained = "";
            is_rejected = False
            if val_str_low in ["n/a", "none", "error", ""]:
                is_rejected = True
            else:
                try:
                    exp_date = pd.to_datetime(value).date()
                    today = datetime.date.today()
                    diff_months = (exp_date.year - today.year) * 12 + (exp_date.month - today.month)
                    if diff_months < 18:
                        is_rejected = True
                    else:
                        obtained = "Pass"
                except:
                    is_rejected = True
        elif "capital structure" in name_low:
            total = 2;
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
        elif "market cap" in name_low:
            total = 5;
            mcap_val = val_str_low;
            numeric_part = clean_val(value);
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
        elif "business model" in name_low:
            total = 15;
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
        ALPHA_VANTAGE_KEY = st.secrets["ALPHA_VANTAGE_API_KEY_1"]
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


    def save_analysis_to_bigquery(ticker, report_data, risk_reward, llm_data, final_score, verdict):
        """
        Saves ticker details using Load Job (WRITE_TRUNCATE)
        and updates/inserts a row in the master_table.
        """
        ticker_clean = ticker.strip().upper().replace("-", "_").replace(".", "_")
        ticker_table_id = f"{DATASET_ID}.{ticker_clean}"
        master_table_id = f"{DATASET_ID}.{MASTER_TABLE_NAME}"

        try:
            if "SERVICE_ACCOUNT_JSON" not in st.secrets: return False
            service_info = dict(st.secrets["SERVICE_ACCOUNT_JSON"])
            if "private_key" in service_info:
                service_info["private_key"] = service_info["private_key"].replace("\\n", "\n")

            credentials = service_account.Credentials.from_service_account_info(service_info)
            client = bigquery.Client(credentials=credentials, project=service_info.get("project_id"))

            # 1. PREPARE TICKER DETAIL DATA
            rows_to_insert = []
            for row in report_data:
                if row["Metric Name"] == "TOTAL": continue
                rows_to_insert.append({
                    "Matric name": row["Metric Name"], "Source": row["Source"], "Value": str(row["Value"]),
                    "Obtained Score": str(row["Obtained points"]), "Total score": str(row["Total points"]),
                    "LLM": None
                })

            # Add Qualitative Data
            rows_to_insert.append({"Matric name": "Risks", "LLM": "\n".join(risk_reward.get("risks", []))})
            rows_to_insert.append({"Matric name": "Rewards", "LLM": "\n".join(risk_reward.get("rewards", []))})
            rows_to_insert.append({"Matric name": "Company Description", "LLM": llm_data.get("description", "N/A")})
            rows_to_insert.append({"Matric name": "Value Proposition", "LLM": llm_data.get("value_proposition", "N/A")})
            rows_to_insert.append({"Matric name": "Moat Analysis", "LLM": llm_data.get("moat", "N/A")})
            rows_to_insert.append({"Matric name": "DATE", "LLM": datetime.date.today().strftime("%Y-%m-%d")})

            # 2. UPDATE TICKER TABLE USING LOAD JOB (WRITE_TRUNCATE)
            # This ensures 100% replacement of the specific ticker data
            df_detail = pd.DataFrame(rows_to_insert)
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            client.load_table_from_dataframe(df_detail, ticker_table_id, job_config=job_config).result()

            # 3. UPDATE MASTER TABLE (UPSERT LOGIC)
            # Check if ticker exists
            check_query = f"SELECT Ticker FROM `{master_table_id}` WHERE Ticker = '{ticker}'"
            exists = client.query(check_query).to_dataframe()

            today_str = datetime.date.today().strftime("%Y-%m-%d")

            if not exists.empty:
                # Update existing row
                update_sql = f"""
                    UPDATE `{master_table_id}`
                    SET Score = {int(float(final_score))}, Verdict = '{verdict}', date = '{today_str}'
                    WHERE Ticker = '{ticker}'
                """
            else:
                # Insert new row
                update_sql = f"""
                    INSERT INTO `{master_table_id}` (Ticker, Score, Verdict, date)
                    VALUES ('{ticker}', {int(float(final_score))}, '{verdict}', '{today_str}')
                """

            client.query(update_sql).result()
            return True
        except Exception as e:
            logger.error(f"BigQuery Save Error: {e}")
            return False


    # --- UPDATE THE CALL AT THE BOTTOM OF THE SCRIPT ---
    # Replace the very last lines of your code (the old call and success message) with this:




    if 'report_data' not in st.session_state: st.session_state.report_data = None
    if 'risk_reward_data' not in st.session_state: st.session_state.risk_reward_data = None
    if 'llm_analysis' not in st.session_state: st.session_state.llm_analysis = None
    if 'current_ticker' not in st.session_state: st.session_state.current_ticker = ""

    if st.session_state.report_data is None:
        st.markdown(
            '<div class="main-header"><h1>Stock Insight Pro</h1><p>Institutional-Grade Stock Analysis Engine</p></div>',
            unsafe_allow_html=True)
        _, center_col, _ = st.columns([1, 1.5, 1])
        with center_col:
            ticker_input = st.text_input("Ticker", placeholder="e.g. TSLA, NVDA", key="ticker_box",
                                         label_visibility="collapsed")
            if st.button("Generate Comprehensive Report") and ticker_input:
                ticker = format_ticker(ticker_input)
                status_box = st.empty();
                status_box.info(f"Analyzing {ticker}...")
                results = asyncio.run(run_parallel_analysis(ticker))
                (analysis_results, finviz_results, moat_score, raw_llm_text, sws_data, iv_rank_result,
                 eps_growth_val) = results
                if isinstance(raw_llm_text, Exception): raw_llm_text = ""
                llm_parsed = parse_llm_response(raw_llm_text)
                if isinstance(sws_data, Exception): sws_data = {"rewards": [], "risks": []}
                if not isinstance(analysis_results, Exception) and analysis_results.get("status") == "success":
                    table_rows = []
                    data_yf = analysis_results["data"]
                    for section_name, metrics in data_yf.items():
                        for metric_name, value in metrics.items():
                            if metric_name.lower() == "net debt":
                                st.session_state["net_debt_val"] = value
                            elif metric_name.lower() == "ebitda":
                                st.session_state["ebitda_val"] = value
                            points, total_pts, rejected = calculate_scoring(metric_name, value)
                            table_rows.append(
                                {"Metric Name": metric_name, "Source": "Yahoo Finance", "Value": str(value),
                                 "Obtained points": "rejected" if rejected else (str(points) if total_pts > 0 else ""),
                                 "Total points": str(total_pts) if total_pts > 0 else ""})
                    finviz_metrics_to_show = ["Net Insider Buying vs Selling (%)", "Net Insider Activity",
                                              "Institutional Ownership (%)", "Short Float (%)"]
                    if finviz_results and not isinstance(finviz_results, Exception):
                        for mk in finviz_metrics_to_show:
                            if mk in finviz_results:
                                val = str(finviz_results[mk]);
                                p, tp, r = calculate_scoring(mk, val)
                                table_rows.append({"Metric Name": mk, "Source": "Finviz", "Value": val,
                                                   "Obtained points": "rejected" if r else str(p),
                                                   "Total points": str(tp)})
                    m_val = str(moat_score);
                    p, tp, r = calculate_scoring("GuruFocus Moat Score", m_val)
                    table_rows.append({"Metric Name": "GuruFocus Moat Score", "Source": "GuruFocus", "Value": m_val,
                                       "Obtained points": "rejected" if r else str(p), "Total points": str(tp)})
                    eps_display = f"{eps_growth_val:.2f}%" if eps_growth_val is not None else "N/A"
                    p, tp, r = calculate_scoring("Forward EPS Growth (%)", eps_display)
                    table_rows.append(
                        {"Metric Name": "Forward EPS Growth (%)", "Source": "Alpha Vantage", "Value": eps_display,
                         "Obtained points": "rejected" if r else str(p), "Total points": str(tp)})
                    iv_val = iv_rank_result.split(":")[-1].strip() if "Success!" in str(iv_rank_result) else "N/A"
                    p, tp, r = calculate_scoring("IV Rank", iv_val)
                    table_rows.append({"Metric Name": "IV Rank", "Source": "Unusual Whales", "Value": iv_val,
                                       "Obtained points": "rejected" if r else str(p), "Total points": str(tp)})
                    ceo_val = llm_parsed["ceo_ownership"];
                    p, tp, r = calculate_scoring("CEO Ownership %", ceo_val)
                    table_rows.append({"Metric Name": "CEO Ownership %", "Source": "Perplexity", "Value": ceo_val,
                                       "Obtained points": "rejected" if r else str(p), "Total points": str(tp)})
                    bm_val = llm_parsed["classification"];
                    p, tp, r = calculate_scoring("Business Model & Value Proposition", bm_val)
                    table_rows.append(
                        {"Metric Name": "Business Model & Value Proposition", "Source": "Perplexity", "Value": bm_val,
                         "Obtained points": "rejected" if r else str(p), "Total points": str(tp)})
                    sum_obtained = sum(
                        float(r["Obtained points"]) for r in table_rows if r["Obtained points"] not in ["rejected", ""])
                    sum_total = sum(float(r["Total points"]) for r in table_rows if r["Total points"] != "")
                    table_rows.append(
                        {"Metric Name": "TOTAL", "Source": "", "Value": "", "Obtained points": str(sum_obtained),
                         "Total points": str(sum_total)})
                    st.session_state.report_data = table_rows;
                    st.session_state.risk_reward_data = sws_data;
                    st.session_state.llm_analysis = llm_parsed;
                    st.session_state.current_ticker = ticker
                    status_box.empty();
                    st.rerun()
                else:
                    status_box.error("Analysis failed.")
    else:
        st.markdown("---")
        if st.session_state.report_data:
            st.subheader("Financial Metrics")
            df = pd.DataFrame(st.session_state.report_data)
            # Ensure Score columns are strings for Arrow serialization
            if "Obtained points" in df.columns: df["Obtained points"] = df["Obtained points"].astype(str)
            if "Total points" in df.columns: df["Total points"] = df["Total points"].astype(str)
            st.dataframe(df, use_container_width=True, hide_index=True)

            st.markdown("<br>", unsafe_allow_html=True)
            st.subheader("Score Summary & Verdict")


            def get_pts(k1, k2=None):
                for r in st.session_state.report_data:
                    n = r["Metric Name"].lower()
                    if k2:
                        if k1 in n and k2 in n: return r["Obtained points"]
                    else:
                        if k1 in n: return r["Obtained points"]
                return "0"


            m_pts = {"runway": get_pts("runway"), "nd_ebitda": get_pts("net debt", "ebitda"),
                     "al_ratio": get_pts("assets", "liabilities"), "burn": get_pts("burn"),
                     "share_growth": get_pts("share count"), "expiration": get_pts("expiration"), "cap_struct": get_pts("capital structure"),
                     "mcap": get_pts("market cap"), "eps_growth": get_pts("eps growth"),
                     "op_lev": get_pts("operating leverage"), "iv_rank": get_pts("iv rank"),
                     "short": get_pts("short float"), "inst": get_pts("institutional ownership"),
                     "total_insider": get_pts("total insider ownership"), "ceo_own": get_pts("ceo ownership"),
                     "ins_buy": get_pts("buying vs selling"), "moat": get_pts("moat score"),
                     "biz_model": get_pts("business model")}


            def to_f(v):
                return float(v) if v not in ["rejected", ""] else 0.0


            s1 = to_f(m_pts["runway"]) + to_f(m_pts["nd_ebitda"]) + to_f(m_pts["al_ratio"]) + to_f(
                m_pts["burn"]) + to_f(m_pts["share_growth"]) + to_f(m_pts["cap_struct"])
            s2 = to_f(m_pts["mcap"]) + to_f(m_pts["eps_growth"]) + to_f(m_pts["op_lev"]) + to_f(
                m_pts["iv_rank"]) + to_f(m_pts["short"]) + to_f(m_pts["inst"])
            s3 = to_f(m_pts["total_insider"]) + to_f(m_pts["ceo_own"]) + to_f(m_pts["ins_buy"])
            s4 = to_f(m_pts["moat"]) + to_f(m_pts["biz_model"])
            final_score = s1 + s2 + s3 + s4
            verdict = "❌ Rejected" if any(v == "rejected" for v in m_pts.values()) else (
                "🔥 Elite LEAPS Candidate" if final_score >= 80 else "✅ Qualified" if final_score >= 70 else "⚠️ Watchlist" if final_score >= 60 else "❌ Rejected")

            summary_df = pd.DataFrame([{
                "Ticker": st.session_state.current_ticker,
                "Financial Survival & Balance Sheet": s1,
                "Growth & Asymmetric Upside": s2,
                "Insider Alignment & Behavior": s3,
                "Moat & Qualitative Conviction": s4,
                "Final Score": str(final_score),
                "Verdict": verdict
            }])
            st.dataframe(summary_df, use_container_width=True, hide_index=True)

            st.markdown("<br>", unsafe_allow_html=True)
            st.subheader("Risk and Rewards")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### ✅ Rewards")
                for i in st.session_state.risk_reward_data.get("rewards", []):
                    st.markdown(f'<p class="reward-text">• {i}</p>', unsafe_allow_html=True)
            with col2:
                st.markdown("#### ⚠️ Risks")
                for i in st.session_state.risk_reward_data.get("risks", []):
                    st.markdown(f'<p class="risk-text">• {i}</p>', unsafe_allow_html=True)

            st.markdown("---")
            st.subheader("Business Profile & Strategic Analysis")
            if st.session_state.llm_analysis:
                llm = st.session_state.llm_analysis
                st.markdown("#### 🏢 Company Description")
                st.markdown(f'<div class="report-card">{llm["description"]}</div>', unsafe_allow_html=True)
                st.markdown("#### 💎 Value Proposition")
                st.markdown(f'<div class="report-card">{llm["value_proposition"]}</div>', unsafe_allow_html=True)
                st.markdown("#### 🛡️ Moat Analysis")
                st.markdown(f'<div class="report-card">{llm["moat"]}</div>', unsafe_allow_html=True)




    #New inserted code
    if st.session_state.report_data:
        success = save_analysis_to_bigquery(
            st.session_state.current_ticker,
            st.session_state.report_data,
            st.session_state.risk_reward_data,
            st.session_state.llm_analysis,
            final_score,
            verdict
        )
        if success:
            st.success(f"Analysis for {st.session_state.current_ticker} saved/updated in Master Table.")
        else:
            st.error("Failed to update BigQuery. Check logs.")
