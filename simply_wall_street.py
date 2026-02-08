import requests
import json
import time
import re
import logging
import streamlit as st

POLYGON_API_KEY = st.secrets["POLYGON_API_KEY_2"]
GEMINI_API_KEY = st.secrets["GEMINI_API_KEY"]


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Scraper")


def get_company_name(ticker):

    url = f"https://api.polygon.io/v3/reference/tickers/{ticker.upper()}?apiKey={POLYGON_API_KEY}"

    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            # Extract name
            name = data.get("results", {}).get("name")
            if name:
                logger.info(f"Resolved ticker '{ticker}' to official name: '{name}'")
                return name

    except Exception as e:
        logger.warning(f"Could not resolve name for {ticker} (using ticker as fallback). Error: {e}")

    # Fallback: If API fails or name not found, just return the ticker
    return ticker


def scrape_risk_rewards(ticker):


    official_name = get_company_name(ticker)


    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"

    system_prompt = (
        "You are a precise financial data extractor specialized in the Simply Wall St (SWS) interface. "
        "Your task is to provide the 'Rewards' and 'Risks' section exactly as it appears to a user on the SWS website. \n\n"
        "STRICT EXTRACTION RULES:\n"
        "1. Use Google Search to find the specific simply wall street 'Risk & Reward' analysis for the stock.\n"
        "2. Only extract the  bullet points display in user interface (e.g., 'Trading at 20% below fair value').\n"
        "3. Do NOT provide general analysis or interpret risks and rewards on your own If data is missing, return empty lists.\n"
        "4. CRITICAL: Your final output must be a valid raw JSON object string. Do not include Markdown formatting (like ```json). "
        "The format must be strictly: {\"company\": \"...\", \"rewards\": [\"...\"], \"risks\": [\"...\"]}"
    )

    # We use both the resolved name and ticker to ensure the AI searches for the right thing
    user_prompt = (
        f"Extract the Simply Wall St 'Risks & Rewards' UI bullet points for: "
        f"{official_name} (Ticker: {ticker.upper()})"
    )

    payload = {
        "contents": [{"parts": [{"text": user_prompt}]}],
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "tools": [{"google_search": {}}],  # Search Tool Enabled
        "generationConfig": {
            "temperature": 0.1

        }
    }

    logger.info(f"Asking Gemini to search for: {official_name}...")

    # Retry Logic
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=payload, timeout=60)

            if response.status_code != 200:
                logger.error(f"Gemini API Error {response.status_code}: {response.text}")

            response.raise_for_status()


            result_json = response.json()
            candidates = result_json.get('candidates', [])

            if not candidates:
                logger.warning("Gemini returned no candidates.")
                return {"company": official_name, "rewards": [], "risks": []}

            raw_text = candidates[0].get('content', {}).get('parts', [{}])[0].get('text', "")

            if not raw_text:
                logger.warning(f"No text content received for {ticker}")
                return {"company": official_name, "rewards": [], "risks": []}

            json_match = re.search(r'\{.*\}', raw_text, re.DOTALL)

            if json_match:
                clean_json_str = json_match.group(0)
                data = json.loads(clean_json_str)
            else:
                # Try loading raw text if regex fails
                data = json.loads(raw_text)


            final_data = {
                "company": data.get("company", official_name),
                "rewards": data.get("rewards", []),
                "risks": data.get("risks", [])
            }

            logger.info(f"Success! Found {len(final_data['rewards'])} rewards and {len(final_data['risks'])} risks.")
            return final_data

        except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError) as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed. Retrying...")
                time.sleep(2)
            else:
                logger.error(f"Final failure for {ticker}: {e}")
                return {"company": official_name, "rewards": [], "risks": []}


# --- Main Execution Block ---
if __name__ == "__main__":
    logger.info("--- Starting Scraper Execution ---")
    ticker_input = "veri"
    result = scrape_risk_rewards(ticker_input)
    print("\n--- FINAL OUTPUT ---")
    print(json.dumps(result, indent=2))