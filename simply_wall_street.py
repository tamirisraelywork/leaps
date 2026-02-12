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
            name = data.get("results", {}).get("name")
            if name:
                logger.info(f"Resolved ticker '{ticker}' to official name: '{name}'")
                return name
    except Exception as e:
        logger.warning(f"Could not resolve name for {ticker} (using ticker as fallback). Error: {e}")
    return ticker


def scrape_risk_rewards(ticker):
    official_name = get_company_name(ticker)

    # Updated Endpoint to match prompt requirements
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={GEMINI_API_KEY}"

    system_prompt = (
        "You are a precise financial data extractor specialized in the Simply Wall St (SWS) interface. "
        "Your task is to provide the 'Rewards' and 'Risks' section EXACTLY as it is displayed on the Simply Wall St website frontend. \n\n"
        "STRICT EXTRACTION RULES:\n"
        "1. Use Google Search to find the actual Simply Wall St 'Risk & Reward' analysis page for this specific stock.\n"
        "2. ONLY extract the specific bullet points displayed in the UI (e.g., 'Trading at 20% below fair value' or 'Dividend is not well covered by earnings').\n"
        "3. DO NOT assume, calculate, or interpret risks and rewards yourself.\n"
        "4. DO NOT provide general financial advice or your own analysis.\n"
        "5. CRITICAL: Your final output must be a valid raw JSON object string. Do not include Markdown formatting. "
        "The format must be: {\"company\": \"...\", \"rewards\": [\"...\"], \"risks\": [\"...\"]}"
    )

    user_prompt = (
        f"Extract the Simply Wall St 'Risks & Rewards' UI bullet points for: "
        f"{official_name} (Ticker: {ticker.upper()})"
    )

    payload = {
        "contents": [{"parts": [{"text": user_prompt}]}],
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "tools": [{"google_search": {}}],
        "generationConfig": {
            "temperature": 0.1
        }
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1} for {ticker.upper()} (Gemini Search)...")
            response = requests.post(url, json=payload, timeout=60)

            if response.status_code != 200:
                logger.error(f"Gemini API Error {response.status_code}: {response.text}")
                response.raise_for_status()

            result_json = response.json()
            candidates = result_json.get('candidates', [])

            if not candidates:
                raise ValueError("No candidates returned from Gemini")

            raw_text = candidates[0].get('content', {}).get('parts', [{}])[0].get('text', "")
            if not raw_text:
                raise ValueError("Empty text part in Gemini response")

            # JSON extraction logic
            json_match = re.search(r'\{.*\}', raw_text, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group(0))
            else:
                data = json.loads(raw_text)

            rewards = data.get("rewards", [])
            risks = data.get("risks", [])

            # CHECK: If both are empty, we force a retry
            if not rewards and not risks:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Both Risks and Rewards were empty for {ticker}. Retrying (Attempt {attempt + 2})...")
                    time.sleep(2)
                    continue
                else:
                    logger.warning(f"Final attempt for {ticker} still returned empty data.")

            # If we reach here, either we have data (one or both) OR we have exhausted retries
            final_data = {
                "company": data.get("company", official_name),
                "rewards": rewards,
                "risks": risks
            }

            logger.info(f"Success! Found {len(final_data['rewards'])} rewards and {len(final_data['risks'])} risks.")
            return final_data

        except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError) as e:
            if attempt < max_retries - 1:
                logger.warning(f"Technical error on attempt {attempt + 1}: {e}. Retrying...")
                time.sleep(2)
            else:
                logger.error(f"Final technical failure for {ticker}: {e}")
                return {"company": official_name, "rewards": [], "risks": []}

    return {"company": official_name, "rewards": [], "risks": []}


if __name__ == "__main__":
    logger.info("--- Starting Scraper Execution ---")
    ticker_input = "veri"
    result = scrape_risk_rewards(ticker_input)
    print("\n--- FINAL OUTPUT ---")
    print(json.dumps(result, indent=2))
