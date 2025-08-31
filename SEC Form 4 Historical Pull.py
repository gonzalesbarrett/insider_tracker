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

# --- IMPORTANT ---
# The SEC requests a custom User-Agent for all automated scripts.
# Please replace the placeholder text with your own information.
# Format: "Your Name or Company Name YourContactEmail@example.com"
SEC_USER_AGENT = "Barrett Gonzales gonzalesbarrett@gmail.com"

def get_sic_description(sic_code):
    """
    Converts a 4-digit SIC code to its major industry division description.
    This provides a high-level categorization for easier analysis.
    """
    if not sic_code or not sic_code.isdigit() or len(sic_code) < 4:
        return sic_code # Return original code if invalid

    # Dictionary mapping the first digit of the SIC code to the Division description.
    # See: https://www.osha.gov/data/sic-manual
    sic_division_map = {
        '0': 'Agriculture, Forestry, & Fishing',
        '1': 'Mining & Construction',
        '2': 'Manufacturing',
        '3': 'Manufacturing',
        '4': 'Transportation, Communications, & Utilities',
        '5': 'Wholesale & Retail Trade',
        '6': 'Finance, Insurance, & Real Estate',
        '7': 'Services',
        '8': 'Services',
        '9': 'Public Administration'
    }

    first_digit = sic_code[0]
    return sic_division_map.get(first_digit, sic_code) # Return description or original code if not found

def make_request(url, error_log, error_key, proxies_list=None):
    """
    Makes a request with a compliant User-Agent, optional proxy, and handles retries.
    """
    headers = {'User-Agent': SEC_USER_AGENT}
    for attempt in range(3): # Try up to 3 times
        try:
            proxies = None
            if proxies_list:
                proxy_url = random.choice(proxies_list)
                proxies = {"http": proxy_url, "https": proxy_url}

            response = requests.get(url, headers=headers, proxies=proxies, timeout=15)
            response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
            return response
        except requests.exceptions.RequestException as e:
            wait_time = (attempt + 1) * 5 # Wait 5, 10, then 15 seconds
            print(f"Request failed for {url}: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    error_log[error_key].append(f"{url} - All retries failed.")
    return None

def get_form4_urls_for_date(target_date, error_log, proxies_list):
    """
    Fetches the SEC EDGAR daily index for a specific date.
    """
    year = target_date.year
    quarter = (target_date.month - 1) // 3 + 1
    formatted_date = target_date.strftime('%Y%m%d')
    sec_base_url = "https://www.sec.gov/Archives/"
    index_url = f"{sec_base_url}edgar/daily-index/{year}/QTR{quarter}/form.{formatted_date}.idx"
    print(f"Attempting to fetch index file from: {index_url}")

    response = make_request(index_url, error_log, 'Index File Download Error', proxies_list)
    if not response:
        return []

    form4_urls = []
    file_content = io.StringIO(response.text)
    data_started = False
    for line in file_content:
        if line.strip().startswith('Form Type'):
            data_started = True
            continue
        if not data_started or not line.strip():
            continue
        if line.strip().startswith('4 '):
            parts = line.split()
            file_path = parts[-1]
            full_url = sec_base_url + file_path
            form4_urls.append(full_url)
    return form4_urls

def parse_form4_filing(url, error_log, proxies_list):
    """
    Downloads and parses a single Form 4 filing from its URL.
    """
    response = make_request(url, error_log, 'Filing Download Error', proxies_list)
    if not response:
        return []

    try:
        full_text = response.text

        industry_code = ''
        sic_match = re.search(r'STANDARD INDUSTRIAL CLASSIFICATION:\s*.*\[(\d{4})\]', full_text)
        if sic_match:
            industry_code = sic_match.group(1)

        industry_description = get_sic_description(industry_code)

        xml_content_start = full_text.find('<XML>')
        xml_content_end = full_text.find('</XML>')
        if xml_content_start == -1 or xml_content_end == -1:
            xml_content_start = full_text.find('<?xml')
            if xml_content_start == -1:
                error_log['XML Content Not Found'].append(url)
                return []
            xml_content = full_text[xml_content_start:]
        else:
            xml_content = full_text[xml_content_start + len('<XML>'):xml_content_end].strip()

        root = ET.fromstring(xml_content)

        def get_text(element, path, default=''):
            node = element.find(path)
            if node is not None:
                if node.find('value') is not None:
                    return node.find('value').text.strip() if node.find('value').text else default
                return node.text.strip() if node.text else default
            return default

        footnotes = " ".join([fn.text.strip() for fn in root.findall('.//footnote') if fn.text])

        owner_cik = get_text(root, './reportingOwner/reportingOwnerId/rptOwnerCik')
        owner_name = get_text(root, './reportingOwner/reportingOwnerId/rptOwnerName')
        issuer_cik = get_text(root, './issuer/issuerCik')
        issuer_name = get_text(root, './issuer/issuerName')
        ticker_symbol = get_text(root, './issuer/issuerTradingSymbol')

        is_director_val = get_text(root, './reportingOwner/reportingOwnerRelationship/isDirector', '0') == '1'
        is_officer_val = get_text(root, './reportingOwner/reportingOwnerRelationship/isOfficer', '0') == '1'
        officer_title = get_text(root, './reportingOwner/reportingOwnerRelationship/officerTitle')

        transactions = []
        for transaction in root.findall('.//nonDerivativeTransaction'):
            raw_date = get_text(transaction, './transactionDate')
            cleaned_date = raw_date[:10] if raw_date else ''

            transaction_data = {
                'owner_name': owner_name, 'owner_cik': owner_cik,
                'issuer_name': issuer_name, 'issuer_cik': issuer_cik,
                'ticker_symbol': ticker_symbol, 'industry': industry_description,
                'is_director': is_director_val, 'is_officer': is_officer_val, 'officer_title': officer_title,
                'security_title': get_text(transaction, './securityTitle'),
                'transaction_date': cleaned_date,
                'transaction_code': get_text(transaction, './transactionCoding/transactionCode'),
                'transaction_shares': get_text(transaction, './transactionAmounts/transactionShares'),
                'transaction_price_per_share': get_text(transaction, './transactionAmounts/transactionPricePerShare'),
                'acquired_disposed_code': get_text(transaction, './transactionAmounts/transactionAcquiredDisposedCode'),
                'shares_owned_after_transaction': get_text(transaction, './postTransactionAmounts/sharesOwnedFollowingTransaction'),
                'ownership_nature': get_text(transaction, './ownershipNature/directOrIndirectOwnership'),
                'footnotes': footnotes,
                'filing_url': url
            }
            transactions.append(transaction_data)
        return transactions
    except Exception as e:
        error_log['Unknown Parsing Error'].append(f"{url} - {e}")
    return []

def add_historical_data(transactions, error_log):
    """
    Enriches transaction data with historical stock and market performance.
    """
    enriched_transactions = []

    for i, transaction in enumerate(transactions):
        print(f"  ({i+1}/{len(transactions)}) Enriching transaction data...", end='\r')
        ticker = transaction.get('ticker_symbol')
        trans_date_str = transaction.get('transaction_date')

        performance_data = {
            'market_cap_on_trade_date': None, 'trade_value_as_pct_of_market_cap': None,
            'price_30d_before': None, 'pct_change_30d_before': None, 'sp500_pct_change_30d_before': None, 'alpha_30d_before': None,
            'price_60d_before': None, 'pct_change_60d_before': None, 'sp500_pct_change_60d_before': None, 'alpha_60d_before': None,
            'price_90d_before': None, 'pct_change_90d_before': None, 'sp500_pct_change_90d_before': None, 'alpha_90d_before': None,
            'price_on_trade_date': None,
            'price_30d_after': None, 'pct_change_30d_after': None, 'sp500_pct_change_30d_after': None, 'alpha_30d_after': None,
            'price_60d_after': None, 'pct_change_60d_after': None, 'sp500_pct_change_60d_after': None, 'alpha_60d_after': None,
            'price_90d_after': None, 'pct_change_90d_after': None, 'sp500_pct_change_90d_after': None, 'alpha_90d_after': None,
            'volume_spike_after_trade': 'No'
        }

        if not ticker or not trans_date_str:
            enriched_transactions.append({**transaction, **performance_data})
            continue

        try:
            trans_date = datetime.strptime(trans_date_str, '%Y-%m-%d').date()
            start_fetch_date = trans_date - timedelta(days=95)
            end_fetch_date = trans_date + timedelta(days=95)

            stock_data = yf.download(ticker, start=start_fetch_date, end=end_fetch_date, progress=False, auto_adjust=True)
            spy_data = yf.download('SPY', start=start_fetch_date, end=end_fetch_date, progress=False, auto_adjust=True)

            if stock_data.empty:
                error_log['yfinance No Data Found'].append(ticker)
                enriched_transactions.append({**transaction, **performance_data})
                continue

            base_price_series = stock_data.loc[stock_data.index >= pd.to_datetime(trans_date)]
            spy_base_price_series = spy_data.loc[spy_data.index >= pd.to_datetime(trans_date)]

            if not base_price_series.empty and not spy_base_price_series.empty:
                price_on_trade_date = base_price_series['Close'].iloc[0].item()
                spy_price_on_trade_date = spy_base_price_series['Close'].iloc[0].item()
                performance_data['price_on_trade_date'] = round(price_on_trade_date, 2)
            else:
                enriched_transactions.append({**transaction, **performance_data})
                continue

            try:
                market_cap = yf.Ticker(ticker).fast_info.get('marketCap')
                if market_cap:
                    performance_data['market_cap_on_trade_date'] = market_cap
                    try:
                        trade_shares = float(transaction.get('transaction_shares', 0))
                        trade_price = float(transaction.get('transaction_price_per_share', 0))
                        trade_value = trade_shares * trade_price
                        if market_cap > 0:
                            pct_of_market_cap = (trade_value / market_cap) * 100
                            performance_data['trade_value_as_pct_of_market_cap'] = round(pct_of_market_cap, 6)
                    except (ValueError, TypeError):
                        pass
            except Exception as e:
                 error_log[f'Market Cap Error for {ticker}'].append(str(e))

            for period in ['before', 'after']:
                for days in [30, 60, 90]:
                    if period == 'before':
                        target_date = trans_date - timedelta(days=days)
                        price_series = stock_data.loc[stock_data.index <= pd.to_datetime(target_date)]
                        spy_price_series = spy_data.loc[spy_data.index <= pd.to_datetime(target_date)]
                        price = price_series['Close'].iloc[-1].item() if not price_series.empty else None
                        spy_price = spy_price_series['Close'].iloc[-1].item() if not spy_price_series.empty else None
                    else:
                        target_date = trans_date + timedelta(days=days)
                        price_series = stock_data.loc[stock_data.index >= pd.to_datetime(target_date)]
                        spy_price_series = spy_data.loc[spy_data.index >= pd.to_datetime(target_date)]
                        price = price_series['Close'].iloc[0].item() if not price_series.empty else None
                        spy_price = spy_price_series['Close'].iloc[0].item() if not spy_price_series.empty else None

                    if price is not None and price_on_trade_date > 0:
                        if period == 'before' and price > 0:
                            pct_change = ((price_on_trade_date - price) / price) * 100
                        elif period == 'after' and price_on_trade_date > 0:
                             pct_change = ((price - price_on_trade_date) / price_on_trade_date) * 100
                        else:
                            pct_change = 0

                        performance_data[f'price_{days}d_{period}'] = round(price, 2)
                        performance_data[f'pct_change_{days}d_{period}'] = round(pct_change, 2)

                    if spy_price is not None and spy_price_on_trade_date > 0:
                        if period == 'before' and spy_price > 0:
                            spy_pct_change = ((spy_price_on_trade_date - spy_price) / spy_price) * 100
                        elif period == 'after' and spy_price_on_trade_date > 0:
                             spy_pct_change = ((spy_price - spy_price_on_trade_date) / spy_price_on_trade_date) * 100
                        else:
                            spy_pct_change = 0

                        performance_data[f'sp500_pct_change_{days}d_{period}'] = round(spy_pct_change, 2)

                        if performance_data.get(f'pct_change_{days}d_{period}') is not None:
                           alpha = performance_data[f'pct_change_{days}d_{period}'] - spy_pct_change
                           performance_data[f'alpha_{days}d_{period}'] = round(alpha, 2)

            baseline_volume_data = stock_data.loc[stock_data.index < pd.to_datetime(trans_date)]
            post_trade_volume_data = stock_data.loc[stock_data.index > pd.to_datetime(trans_date)]

            if not baseline_volume_data.empty and not post_trade_volume_data.empty:
                baseline_avg_volume = baseline_volume_data['Volume'].mean()
                max_post_trade_volume = post_trade_volume_data['Volume'].max()
                if baseline_avg_volume > 0 and max_post_trade_volume > (baseline_avg_volume * 2):
                    performance_data['volume_spike_after_trade'] = 'Yes'

            enriched_transactions.append({**transaction, **performance_data})
            time.sleep(1)

        except Exception as e:
            error_log[f'yfinance Processing Error for {ticker}'].append(str(e))
            enriched_transactions.append({**transaction, **performance_data})

    return enriched_transactions

def export_errors_to_csv(error_log, output_path):
    """
    Exports the collected error log to a CSV file.
    """
    error_list = []
    for error_type, items in error_log.items():
        for item in items:
            error_list.append({'error_type': error_type, 'identifier_or_message': item})

    if error_list:
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                header = ['error_type', 'identifier_or_message']
                writer = csv.DictWriter(csvfile, fieldnames=header)
                writer.writeheader()
                writer.writerows(error_list)
            print(f"\nSuccessfully exported error log to: {output_path}")
        except IOError as e:
            print(f"\nCould not write error log to CSV: {e}")

if __name__ == '__main__':
    start_time = time.time()
    start_date = date(2025, 1, 13)
    end_date = date(2025, 1, 17)
    master_error_log = defaultdict(list)

    IS_COLAB = False
    try:
        from google.colab import drive
        drive.mount('/content/drive')
        IS_COLAB = True
    except ImportError:
        pass

    # //////////////////////////////////////////////////////////////////////////
    # /// --- SCRIPT MODE CONFIGURATION --- ///
    # //////////////////////////////////////////////////////////////////////////
    TEST_MODE_ENABLED = False
    TEST_MODE_LIMIT = 100
    # //////////////////////////////////////////////////////////////////////////

    # //////////////////////////////////////////////////////////////////////////
    # /// --- PROXY CONFIGURATION --- ///
    # //////////////////////////////////////////////////////////////////////////
    USE_PROXIES = False

    proxies = []
    if USE_PROXIES:
        print("Proxy usage is ENABLED.")
        proxy_user = 'spkhzh58w4'
        proxy_pass = '=RdhQw7m8tQimw4G0t'
        proxy_host = 'gate.decodo.com'
        proxy_ports = range(10001, 10011)

        encoded_user = quote(proxy_user)
        encoded_pass = quote(proxy_pass)

        proxies = [
            f'http://{encoded_user}:{encoded_pass}@{proxy_host}:{port}'
            for port in proxy_ports
        ]

        if proxies:
            yf_proxy_for_config = random.choice(proxies)
            yf.set_config({'proxy': yf_proxy_for_config})
            print(f"yfinance configured to use proxy: {yf_proxy_for_config.split('@')[1]}")
    else:
        print("Proxy usage is DISABLED.")
    # //////////////////////////////////////////////////////////////////////////

    print(f"Starting process for Form 4 filings from {start_date} to {end_date}...")

    all_transactions_for_range = []

    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() >= 5:
            print(f"\nSkipping {current_date.strftime('%Y-%m-%d')} because it is a weekend.")
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
        else:
            print(f"No Form 4 filing URLs found for {current_date.strftime('%Y-%m-%d')}.")

        current_date += timedelta(days=1)

    if all_transactions_for_range:
        high_signal_transactions = [
            t for t in all_transactions_for_range if t.get('transaction_code') in ['P', 'S']
        ]

        print(f"\nSuccessfully parsed {len(all_transactions_for_range)} total transactions.")
        print(f"Found {len(high_signal_transactions)} high-signal (Purchase/Sale) transactions to analyze.")

        if high_signal_transactions:
            print("Now fetching historical stock data for high-signal transactions...")
            enriched_data = add_historical_data(high_signal_transactions, master_error_log)
        else:
            enriched_data = all_transactions_for_range

        if IS_COLAB:
            output_dir = '/content/drive/My Drive/Insider_Trader_Tracking/Daily_Traders'
            error_output_dir = '/content/drive/My Drive/Insider_Trader_Tracking/Error_Output'
        else:
            output_dir = r'C:\Users\Gonza\OneDrive\Desktop\Insider Trader Tracking\Daily Traders'
            error_output_dir = r'C:\Users\Gonza\OneDrive\Desktop\Insider Trader Tracking\Error_Output'
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(error_output_dir, exist_ok=True)

        if start_date == end_date:
            file_date_str = start_date.strftime("%Y-%m-%d")
            output_filename = f'{file_date_str}_Daily_Trade.csv'
            error_filename = f'{file_date_str}_errors.csv'
        else:
            file_date_str = f'{start_date.strftime("%Y-%m-%d")}_to_{end_date.strftime("%Y-%m-%d")}'
            output_filename = f'{file_date_str}_Trades.csv'
            error_filename = f'{file_date_str}_errors.csv'

        output_path = os.path.join(output_dir, output_filename)
        error_output_path = os.path.join(error_output_dir, error_filename)

        print(f"\nExporting enriched data to: {output_path}")
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                if enriched_data:
                    header = set()
                    for row in enriched_data:
                        header.update(row.keys())

                    preferred_order = [
                        'owner_name', 'owner_cik', 'issuer_name', 'issuer_cik', 'ticker_symbol', 'industry',
                        'is_director', 'is_officer', 'officer_title', 'security_title', 'transaction_date',
                        'transaction_code', 'transaction_shares', 'transaction_price_per_share',
                        'acquired_disposed_code', 'shares_owned_after_transaction', 'ownership_nature', 'footnotes', 'filing_url',
                        'market_cap_on_trade_date', 'trade_value_as_pct_of_market_cap',
                        'price_on_trade_date',
                        'price_30d_before', 'pct_change_30d_before', 'sp500_pct_change_30d_before', 'alpha_30d_before',
                        'price_60d_before', 'pct_change_60d_before', 'sp500_pct_change_60d_before', 'alpha_60d_before',
                        'price_90d_before', 'pct_change_90d_before', 'sp500_pct_change_90d_before', 'alpha_90d_before',
                        'price_30d_after', 'pct_change_30d_after', 'sp500_pct_change_30d_after', 'alpha_30d_after',
                        'price_60d_after', 'pct_change_60d_after', 'sp500_pct_change_60d_after', 'alpha_60d_after',
                        'price_90d_after', 'pct_change_90d_after', 'sp500_pct_change_90d_after', 'alpha_90d_after',
                        'volume_spike_after_trade'
                    ]

                    final_header = [col for col in preferred_order if col in header] + sorted([col for col in header if col not in preferred_order])

                    writer = csv.DictWriter(csvfile, fieldnames=final_header)
                    writer.writeheader()
                    writer.writerows(enriched_data)
            print(f"Successfully exported data for the specified date range.")
        except (IOError, IndexError) as e:
            master_error_log['CSV Write Error'].append(f"{output_path} - {e}")
    else:
        print(f"\nNo transactions were successfully parsed for the entire date range.")

    print(f"\n{'='*60}\n--- SCRIPT EXECUTION COMPLETE: SUMMARY ---\n{'='*60}")

    total_parsed = len(all_transactions_for_range)

    failed_transactions = 0
    if total_parsed > 0:
        for t in enriched_data:
            if t.get('ticker_symbol') and t.get('price_on_trade_date') is None and t.get('transaction_code') in ['P', 'S']:
                failed_transactions +=1

    successful_transactions = len([t for t in enriched_data if t.get('price_on_trade_date') is not None])
    high_signal_count = len(high_signal_transactions) if 'high_signal_transactions' in locals() else 0
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
            if len(items) > 5:
                print(f"  - ... and {len(items) - 5} more.")
            print("\n")

    print("\n--- Verifying file output ---")
    if os.path.exists(output_path):
        print(f"SUCCESS: File was successfully created at: {output_path}")
    else:
        print(f"ERROR: File was NOT created at the expected path: {output_path}")

    end_time = time.time()
    execution_time = end_time - start_time
    minutes = int(execution_time // 60)
    seconds = int(execution_time % 60)
    print(f"{'='*60}\nTotal Execution Time: {minutes} minutes and {seconds} seconds.\n{'='*60}")

