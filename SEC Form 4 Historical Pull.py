import requests
from datetime import date, timedelta, datetime
import io
import csv
import os
import xml.etree.ElementTree as ET
import yfinance as yf
import time
import pandas as pd
from collections import defaultdict
import random
from urllib.parse import quote
import re

# --- Google Colab & API Libraries ---
# These will only be imported if the script is run in a Colab environment.
# This allows the script to be portable and run both locally and in the cloud.
try:
    from google.colab import drive
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    IS_COLAB = True
except ImportError:
    IS_COLAB = False
# ---

# --- SEC Edgar Configuration ---
# The SEC requires a custom User-Agent for all automated scripts to identify the source of the traffic.
# It's crucial to replace the placeholder with your own information to avoid being blocked.
# Format: "Your Name or Company Name YourContactEmail@example.com"
SEC_USER_AGENT = "YOUR_NAME_OR_COMPANY YOUR_EMAIL@EXAMPLE.COM"

def get_sic_description(sic_code):
    """
    Converts a 4-digit Standard Industrial Classification (SIC) code to its 
    major industry division description for easier analysis.
    """
    if not sic_code or not sic_code.isdigit() or len(sic_code) < 4:
        return sic_code 

    # Dictionary mapping the first digit of the SIC code to the Division description.
    # For a full list, see: https://www.osha.gov/data/sic-manual
    sic_division_map = {
        '0': 'Agriculture, Forestry, & Fishing', '1': 'Mining & Construction',
        '2': 'Manufacturing', '3': 'Manufacturing',
        '4': 'Transportation, Communications, & Utilities', '5': 'Wholesale & Retail Trade',
        '6': 'Finance, Insurance, & Real Estate', '7': 'Services',
        '8': 'Services', '9': 'Public Administration'
    }
    
    first_digit = sic_code[0]
    return sic_division_map.get(first_digit, sic_code)

def make_request(url, error_log, error_key, proxies_list=None):
    """
    Makes a web request with a compliant User-Agent and handles retries with exponential backoff.
    This makes the script more resilient to temporary network issues or rate limiting.
    """
    headers = {'User-Agent': SEC_USER_AGENT}
    for attempt in range(3):
        try:
            proxies = None
            if proxies_list:
                proxy_url = random.choice(proxies_list)
                proxies = {"http": proxy_url, "https": proxy_url}
            response = requests.get(url, headers=headers, proxies=proxies, timeout=15)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            wait_time = (attempt + 1) * 5
            print(f"Request failed for {url}: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    
    error_log[error_key].append(f"{url} - All retries failed.")
    return None

def get_form4_urls_for_date(target_date, error_log, proxies_list):
    """
    Fetches the SEC EDGAR daily index for a specific date to find all Form 4 filings.
    """
    year, quarter = target_date.year, (target_date.month - 1) // 3 + 1
    formatted_date = target_date.strftime('%Y%m%d')
    sec_base_url = "https://www.sec.gov/Archives/"
    index_url = f"{sec_base_url}edgar/daily-index/{year}/QTR{quarter}/form.{formatted_date}.idx"
    
    response = make_request(index_url, error_log, 'Index File Download Error', proxies_list)
    if not response: return []

    form4_urls = []
    data_started = False
    for line in io.StringIO(response.text):
        if 'Form Type' in line: data_started = True; continue
        if not data_started or not line.strip(): continue
        if line.strip().startswith('4 '):
            form4_urls.append(sec_base_url + line.split()[-1])
    return form4_urls

def parse_form4_filing(url, error_log, proxies_list):
    """
    Downloads and parses a single Form 4 filing. It extracts data from both the
    plain-text header (for SIC code) and the main XML body.
    """
    response = make_request(url, error_log, 'Filing Download Error', proxies_list)
    if not response: return []
        
    try:
        full_text = response.text
        industry_code_match = re.search(r'STANDARD INDUSTRIAL CLASSIFICATION:\s*.*\[(\d{4})\]', full_text)
        industry_code = industry_code_match.group(1) if industry_code_match else ''
        industry_description = get_sic_description(industry_code)

        xml_start = full_text.find('<XML>')
        xml_end = full_text.find('</XML>')
        if xml_start == -1 or xml_end == -1: xml_start = full_text.find('<?xml')
        if xml_start == -1: error_log['XML Content Not Found'].append(url); return []
        
        xml_content = full_text[xml_start + (len('<XML>') if '<XML>' in full_text else 0):xml_end if '</XML>' in full_text else len(full_text)].strip()
        root = ET.fromstring(xml_content)
        
        def get_text(element, path, default=''):
            node = element.find(path)
            if node is not None:
                value_node = node.find('value')
                return (value_node.text or default).strip() if value_node is not None else (node.text or default).strip()
            return default

        footnotes = " ".join([fn.text.strip() for fn in root.findall('.//footnote') if fn.text])
        owner_cik, owner_name = get_text(root, './reportingOwner/reportingOwnerId/rptOwnerCik'), get_text(root, './reportingOwner/reportingOwnerId/rptOwnerName')
        issuer_cik, issuer_name, ticker_symbol = get_text(root, './issuer/issuerCik'), get_text(root, './issuer/issuerName'), get_text(root, './issuer/issuerTradingSymbol')
        is_director, is_officer, officer_title = get_text(root, './reportingOwner/reportingOwnerRelationship/isDirector', '0') == '1', get_text(root, './reportingOwner/reportingOwnerRelationship/isOfficer', '0') == '1', get_text(root, './reportingOwner/reportingOwnerRelationship/officerTitle')

        transactions = []
        for transaction in root.findall('.//nonDerivativeTransaction'):
            transactions.append({
                'owner_name': owner_name, 'owner_cik': owner_cik, 'issuer_name': issuer_name, 'issuer_cik': issuer_cik,
                'ticker_symbol': ticker_symbol, 'industry': industry_description, 'is_director': is_director, 'is_officer': is_officer, 'officer_title': officer_title,
                'security_title': get_text(transaction, './securityTitle'), 'transaction_date': (get_text(transaction, './transactionDate')[:10] if get_text(transaction, './transactionDate') else ''),
                'transaction_code': get_text(transaction, './transactionCoding/transactionCode'), 'transaction_shares': get_text(transaction, './transactionAmounts/transactionShares'),
                'transaction_price_per_share': get_text(transaction, './transactionAmounts/transactionPricePerShare'), 'acquired_disposed_code': get_text(transaction, './transactionAmounts/transactionAcquiredDisposedCode'),
                'shares_owned_after_transaction': get_text(transaction, './postTransactionAmounts/sharesOwnedFollowingTransaction'), 'ownership_nature': get_text(transaction, './ownershipNature/directOrIndirectOwnership'),
                'footnotes': footnotes, 'filing_url': url
            })
        return transactions
    except Exception as e:
        error_log['Unknown Parsing Error'].append(f"{url} - {e}")
        return []

def add_historical_data(transactions, error_log):
    """
    Enriches transaction data with historical stock and market performance using yfinance.
    """
    enriched_transactions = []
    for i, transaction in enumerate(transactions):
        print(f"  ({i+1}/{len(transactions)}) Enriching transaction data...", end='\r')
        ticker, trans_date_str = transaction.get('ticker_symbol'), transaction.get('transaction_date')
        
        default_performance = {k: 'No' if k == 'volume_spike_after_trade' else None for k in ['market_cap_on_trade_date', 'trade_value_as_pct_of_market_cap', 'price_on_trade_date', 'volume_spike_after_trade'] + [f'{p}_{d}d_{s}' for p in ['price', 'pct_change', 'sp500_pct_change', 'alpha'] for d in [30, 60, 90] for s in ['before', 'after']]}
        
        if not ticker or not trans_date_str: enriched_transactions.append({**transaction, **default_performance}); continue
        
        try:
            trans_date_ts = pd.to_datetime(trans_date_str)
            start_fetch, end_fetch = trans_date_ts - timedelta(days=95), trans_date_ts + timedelta(days=95)
            stock_data, spy_data = yf.download(ticker, start=start_fetch, end=end_fetch, progress=False, auto_adjust=True), yf.download('SPY', start=start_fetch, end=end_fetch, progress=False, auto_adjust=True)
            
            if stock_data.empty: error_log['yfinance No Data Found'].append(ticker); enriched_transactions.append({**transaction, **default_performance}); continue

            base_price_series, spy_base_price_series = stock_data.loc[stock_data.index >= trans_date_ts], spy_data.loc[spy_data.index >= trans_date_ts]
            if base_price_series.empty or spy_base_price_series.empty: enriched_transactions.append({**transaction, **default_performance}); continue
            
            price_on_trade_date, spy_price_on_trade_date = base_price_series['Close'].iloc[0].item(), spy_base_price_series['Close'].iloc[0].item()
            default_performance['price_on_trade_date'] = round(price_on_trade_date, 2)
            
            market_cap = yf.Ticker(ticker).fast_info.get('marketCap')
            if market_cap:
                default_performance['market_cap_on_trade_date'] = market_cap
                try:
                    trade_shares, trade_price = float(transaction.get('transaction_shares', 0)), float(transaction.get('transaction_price_per_share', 0))
                    if market_cap > 0: default_performance['trade_value_as_pct_of_market_cap'] = round((trade_shares * trade_price / market_cap) * 100, 6)
                except (ValueError, TypeError):
                    pass
            
            for period in ['before', 'after']:
                for days in [30, 60, 90]:
                    target_date = trans_date_ts + timedelta(days=days) if period == 'after' else trans_date_ts - timedelta(days=days)
                    price_series = stock_data.loc[stock_data.index >= target_date] if period == 'after' else stock_data.loc[stock_data.index <= target_date]
                    spy_price_series = spy_data.loc[spy_data.index >= target_date] if period == 'after' else spy_data.loc[spy_data.index <= target_date]
                    price = price_series['Close'].iloc[0 if period == 'after' else -1].item() if not price_series.empty else None
                    spy_price = spy_price_series['Close'].iloc[0 if period == 'after' else -1].item() if not spy_price_series.empty else None
                    
                    if price and price_on_trade_date > 0:
                        pct_change = ((price - price_on_trade_date) / price_on_trade_date) * 100
                        default_performance[f'price_{days}d_{period}'], default_performance[f'pct_change_{days}d_{period}'] = round(price, 2), round(pct_change, 2)
                    if spy_price and spy_price_on_trade_date > 0:
                        spy_pct_change = ((spy_price - spy_price_on_trade_date) / spy_price_on_trade_date) * 100
                        default_performance[f'sp500_pct_change_{days}d_{period}'] = round(spy_pct_change, 2)
                        if default_performance.get(f'pct_change_{days}d_{period}'):
                            default_performance[f'alpha_{days}d_{period}'] = round(default_performance[f'pct_change_{days}d_{period}'] - spy_pct_change, 2)

            baseline_volume = stock_data.loc[stock_data.index < trans_date_ts]
            post_trade_volume = stock_data.loc[stock_data.index > trans_date_ts]
            if not baseline_volume.empty and not post_trade_volume.empty and baseline_volume['Volume'].mean() > 0 and post_trade_volume['Volume'].max() > (baseline_volume['Volume'].mean() * 2):
                default_performance['volume_spike_after_trade'] = 'Yes'

            enriched_transactions.append({**transaction, **default_performance})
            time.sleep(1)
        except Exception as e:
            error_log[f'yfinance Processing Error for {ticker}'].append(str(e))
            enriched_transactions.append({**transaction, **default_performance})
            
    return enriched_transactions

def export_errors_to_csv(error_log, output_path):
    # This function remains unchanged for brevity
    pass

def authenticate_google(credentials_path, scopes):
    # This function remains unchanged for brevity
    pass

def find_file_id(service, folder_id, file_name):
    # This function remains unchanged for brevity
    pass

def share_file_publicly(service, file_id):
    # This function remains unchanged for brevity
    pass

def append_link_to_sheet(service, spreadsheet_id, link):
    # This function remains unchanged for brevity
    pass

def validate_output_csv(file_path):
    # This function remains unchanged for brevity
    pass

if __name__ == '__main__':
    start_time = time.time()
    
    # ---
    # SCRIPT CONFIGURATION
    # ---
    # Set the date range for which you want to pull data.
    start_date = date(2025, 3, 3)
    end_date = date(2025, 3, 7)
    
    # Check if the script is running in Google Colab to determine file paths.
    IS_COLAB = 'google.colab' in str(get_ipython())
    if IS_COLAB:
        drive.mount('/content/drive', force_remount=True)

    # //////////////////////////////////////////////////////////////////////////
    # /// --- GOOGLE API CONFIGURATION --- ///
    # //////////////////////////////////////////////////////////////////////////
    # To enable automation, you must get credentials from the Google Cloud Console.
    # 1. Upload your downloaded JSON credentials file to your Colab session.
    # 2. Update the file path below.
    CREDENTIALS_FILE_PATH = 'your_credentials_file.json'
    # 3. Get the ID of your 'Master_File_Index' Google Sheet from its URL.
    MASTER_SHEET_ID = 'YOUR_MASTER_SHEET_ID_HERE'
    # 4. Get the ID of your 'Daily Traders' folder in Google Drive from its URL.
    DAILY_TRADERS_FOLDER_ID = 'YOUR_GOOGLE_DRIVE_FOLDER_ID_HERE'
    # //////////////////////////////////////////////////////////////////////////
    
    # //////////////////////////////////////////////////////////////////////////
    # /// --- SCRIPT MODE CONFIGURATION --- ///
    # //////////////////////////////////////////////////////////////////////////
    # Set to True to run in a limited test mode, False for a full run.
    TEST_MODE_ENABLED = False
    TEST_MODE_LIMIT = 100 # Number of filings to process in test mode.
    # //////////////////////////////////////////////////////////////////////////

    # //////////////////////////////////////////////////////////////////////////
    # /// --- PROXY CONFIGURATION --- ///
    # //////////////////////////////////////////////////////////////////////////
    USE_PROXIES = False
    
    proxies = []
    if USE_PROXIES:
        print("Proxy usage is ENABLED.")
        # Replace with your actual proxy credentials.
        proxy_user = 'YOUR_PROXY_USERNAME'
        proxy_pass = 'YOUR_PROXY_PASSWORD'
        proxy_host = 'gate.decodo.com' # Or your provider's hostname
        
        # URL-encode the username and password to handle special characters.
        encoded_user, encoded_pass = quote(proxy_user), quote(proxy_pass)
        
        # Assumes a range of ports from your provider.
        proxies = [f'http://{encoded_user}:{encoded_pass}@{proxy_host}:{port}' for port in range(10001, 10011)]
        
        if proxies:
            yf_proxy_for_config = random.choice(proxies)
            yf.set_config({'proxy': yf_proxy_for_config})
            print(f"yfinance configured to use proxy: {yf_proxy_for_config.split('@')[1]}")
    else:
        print("Proxy usage is DISABLED.")
    # //////////////////////////////////////////////////////////////////////////
    
    master_error_log = defaultdict(list)
    print(f"Starting process for Form 4 filings from {start_date} to {end_date}...")
    
    all_transactions_for_range = []
    
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() >= 5:
            print(f"\nSkipping {current_date.strftime('%Y-%m-%d')} (weekend).")
            current_date += timedelta(days=1)
            continue

        print(f"\n{'='*60}\nProcessing data for: {current_date.strftime('%Y-%m-%d')}\n{'='*60}")
        
        urls = get_form4_urls_for_date(current_date, master_error_log, proxies)
        
        if urls:
            if TEST_MODE_ENABLED:
                print(f"\n--- TEST MODE ENABLED: Processing only the first {min(TEST_MODE_LIMIT, len(urls))} filings. ---\n")
                urls = urls[:TEST_MODE_LIMIT]

            print(f"Found {len(urls)} filings. Parsing all of them...")
            for i, url in enumerate(urls):
                print(f"  ({i+1}/{len(urls)}) Parsing SEC data...", end='\r')
                time.sleep(0.1)
                filing_transactions = parse_form4_filing(url, master_error_log, proxies)
                if filing_transactions:
                    all_transactions_for_range.extend(filing_transactions)
            print(" " * 80, end='\r') 
        
        current_date += timedelta(days=1)

    if all_transactions_for_range:
        high_signal_transactions = [t for t in all_transactions_for_range if t.get('transaction_code') in ['P', 'S']]
        
        print(f"\nSuccessfully parsed {len(all_transactions_for_range)} total transactions.")
        print(f"Found {len(high_signal_transactions)} high-signal (Purchase/Sale) transactions to analyze.")
        
        enriched_data = add_historical_data(high_signal_transactions, master_error_log) if high_signal_transactions else all_transactions_for_range

        if IS_COLAB:
            output_dir = '/content/drive/My Drive/Insider_Trader_Tracking/Daily_Traders'
            error_output_dir = '/content/drive/My Drive/Insider_Trader_Tracking/Error_Output'
        else:
            desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
            output_dir = os.path.join(desktop_path, 'Insider_Trader_Tracking', 'Daily Traders')
            error_output_dir = os.path.join(desktop_path, 'Insider_Trader_Tracking', 'Error_Output')
        os.makedirs(output_dir, exist_ok=True); os.makedirs(error_output_dir, exist_ok=True)

        file_date_str = f'{start_date.strftime("%Y-%m-%d")}' if start_date == end_date else f'{start_date.strftime("%Y-%m-%d")}_to_{end_date.strftime("%Y-%m-%d")}'
        output_filename = f'{file_date_str}_Trades.csv'
        error_filename = f'{file_date_str}_errors.csv'
        
        output_path = os.path.join(output_dir, output_filename)
        error_output_path = os.path.join(error_output_dir, error_filename)

        print(f"\nExporting enriched data to: {output_path}")
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                if enriched_data:
                    preferred_order = ['owner_name', 'owner_cik', 'issuer_name', 'issuer_cik', 'ticker_symbol', 'industry', 'is_director', 'is_officer', 'officer_title', 'security_title', 'transaction_date', 'transaction_code', 'transaction_shares', 'transaction_price_per_share', 'acquired_disposed_code', 'shares_owned_after_transaction', 'ownership_nature', 'footnotes', 'filing_url', 'market_cap_on_trade_date', 'trade_value_as_pct_of_market_cap', 'price_on_trade_date', 'price_30d_before', 'pct_change_30d_before', 'sp500_pct_change_30d_before', 'alpha_30d_before', 'price_60d_before', 'pct_change_60d_before', 'sp500_pct_change_60d_before', 'alpha_60d_before', 'price_90d_before', 'pct_change_90d_before', 'sp500_pct_change_90d_before', 'alpha_90d_before', 'price_30d_after', 'pct_change_30d_after', 'sp500_pct_change_30d_after', 'alpha_30d_after', 'price_60d_after', 'pct_change_60d_after', 'sp500_pct_change_60d_after', 'alpha_60d_after', 'price_90d_after', 'pct_change_90d_after', 'sp500_pct_change_90d_after', 'alpha_90d_after']
                    writer = csv.DictWriter(csvfile, fieldnames=preferred_order)
                    writer.writeheader()
                    writer.writerows(enriched_data)
            print(f"Successfully exported data for the specified date range.")
        except (IOError, IndexError) as e:
            master_error_log['CSV Write Error'].append(f"{output_path} - {e}")
    
        if IS_COLAB and 'output_filename' in locals():
            print("\n--- Starting Google Drive Automation ---")
            scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
            creds = authenticate_google(CREDENTIALS_FILE_PATH, scopes)
            if creds:
                drive_service, sheets_service = build('drive', 'v3', credentials=creds), build('sheets', 'v4', credentials=creds)
                file_id = find_file_id(drive_service, DAILY_TRADERS_FOLDER_ID, output_filename)
                if file_id and share_file_publicly(drive_service, file_id):
                    power_bi_link = f'https://drive.google.com/uc?export=download&id={file_id}'
                    append_link_to_sheet(sheets_service, MASTER_SHEET_ID, power_bi_link)
    
    print(f"\n{'='*60}\n--- SCRIPT EXECUTION COMPLETE: SUMMARY ---\n{'='*60}")
    
    total_parsed = len(all_transactions_for_range) if 'all_transactions_for_range' in locals() else 0
    high_signal_count = len(high_signal_transactions) if 'high_signal_transactions' in locals() else 0
    successful_transactions = len([t for t in enriched_data if t.get('price_on_trade_date')]) if 'enriched_data' in locals() else 0
    failed_transactions = high_signal_count - successful_transactions
    success_rate = (successful_transactions / high_signal_count * 100) if high_signal_count > 0 else 0

    print("--- Quantitative Impact ---")
    print(f"Total Transactions Parsed: {total_parsed}")
    print(f"High-Signal (P/S) Transactions Found: {high_signal_count}")
    print(f"Successfully Enriched Transactions: {successful_transactions}")
    print(f"Failed Transactions: {failed_transactions}")
    print(f"Enrichment Success Rate: {success_rate:.2f}%")
    
    if master_error_log:
        export_errors_to_csv(master_error_log, error_output_path)
    
    print("\n--- Error Log Summary ---")
    if not master_error_log:
        print("No errors were logged during the run.")
    else:
        for error_type, items in master_error_log.items():
            print(f"--- Error Type: '{error_type}' (Count: {len(items)}) ---")
            for item in items[:5]:
                print(f"  - {item}")
            if len(items) > 5: print(f"  - ... and {len(items) - 5} more.")
            print("\n")

    if 'output_path' in locals() and os.path.exists(output_path):
        print("\n--- Verifying file output ---")
        validate_output_csv(output_path)
    else:
        print("ERROR: Main output file was not created, cannot validate.")

    end_time = time.time()
    execution_time = end_time - start_time
    minutes, seconds = int(execution_time // 60), int(execution_time % 60)
    print(f"{'='*60}\nTotal Execution Time: {minutes} minutes and {seconds} seconds.\n{'='*60}")

