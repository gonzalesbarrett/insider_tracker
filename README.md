[Readme.md](https://github.com/user-attachments/files/22122017/Readme.md)
# **SEC Form 4 Insider Trading Scraper**

This Python script is a comprehensive tool designed to download, parse, and analyze insider trading data from the U.S. Securities and Exchange Commission (SEC). It automates the entire process of gathering Form 4 filings, enriching them with historical stock performance, and preparing a clean dataset for analysis in tools like Power BI.

The primary goal of this project is to provide a dataset that can be used to track the performance of insider trades, identify successful traders, and uncover potential market signals.

## **Key Features**

* **SEC Data Retrieval:** Automatically fetches the daily index of SEC filings and identifies all Form 4 submissions for a given date range.  
* **Robust Parsing:** Parses the complex raw text and XML structure of Form 4 filings to extract key data points, including insider details, transaction information, and explanatory footnotes.  
* **Historical Performance Analysis:** For each high-signal trade (Purchases 'P' and Sales 'S'), the script uses the yfinance library to download historical stock data and calculates:  
  * Pre- and post-trade performance at 30, 60, and 90-day intervals.  
  * **Alpha vs. S\&P 500:** Measures the stock's performance relative to the market to determine if a trade was truly successful.  
  * **Trade Significance:** Calculates the trade's value as a percentage of the company's market cap.  
* **Intelligent Filtering:** To improve speed and focus, the script pre-filters for only high-signal transactions (P and S) before performing the time-consuming historical analysis.  
* **Google Drive & Sheets Automation (Optional):** When run in a Google Colab environment, the script can automatically:  
  * Save the final CSV to a specified Google Drive folder.  
  * Make the file publicly viewable.  
  * Append a direct-download link to a master Google Sheet, creating a fully automated data source for Power BI.  
* **Error Handling & Validation:** Includes a detailed error logging system and a final data validation check to ensure the quality and completeness of the output.

## **Setup & Configuration**

To run this script, you will need to configure the main execution block (if \_\_name\_\_ \== '\_\_main\_\_':) at the bottom of the file.

1. **Dependencies:** Ensure you have the required Python libraries installed: requests, pandas, and yfinance. If using the Google Colab features, you will also need the Google API client libraries.  
2. **User Agent:** You **must** replace the placeholder SEC\_USER\_AGENT with your own name/company and email address to comply with the SEC's fair access policy.  
3. **Date Range:** Set the start\_date and end\_date to define the period for which you want to pull data.  
4. **Test Mode:** For quick debugging, you can set TEST\_MODE\_ENABLED \= True to limit the run to a small number of filings (TEST\_MODE\_LIMIT).  
5. **(Optional) Google API Automation:** To enable the automation features:  
   * Follow the setup guide to create a Google Cloud project, enable the Drive and Sheets APIs, and download a JSON credentials file.  
   * Upload the JSON file to your Colab session.  
   * Update the CREDENTIALS\_FILE\_PATH, MASTER\_SHEET\_ID, and DAILY\_TRADERS\_FOLDER\_ID variables with your specific information.  
6. **(Optional) Proxies:** To avoid IP blocks during very large data pulls, you can enable USE\_PROXIES and provide your proxy credentials.

## **Output**

The script generates a clean, analysis-ready CSV file with a comprehensive set of columns, ready to be imported into Power BI or any other data analysis tool.
