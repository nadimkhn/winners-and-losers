#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Setting up the environment

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
import psycopg2
import json


# In[2]:


# Load the configuration file

file_path = '/Users/nadee/Desktop/PROJECTS/winners-and-losers/config/config.txt'

# Open and load the JSON data
with open(file_path, 'r') as file:
    config = json.load(file)

# Now `data` is a Python dictionary (or list, depending on the file's structure)


# In[3]:


# Pull the required configuration fields from the file

host = config['host']
dbname = config['dbName']
user = config['user']
password = config['password']
port = config['port']


# In[4]:


# Define connection parameters
conn = psycopg2.connect( 
    host = host, # pgAdmin 4 > server properties > connection
    dbname = dbname, # pgAdmin 4 > database properties
    user = user, 
    password = password,
    port = port # pgAdmin 4 > server properties > connection
)

# Fetch all data from market_summary table in the database
query = '''
WITH
	SP500_STOCKS_1 AS (
		SELECT
			SP500_TICKER_METADATA.COMPANY_NAME,
			SP500_TICKER_METADATA.INDUSTRY,
			MARKET_SUMMARY.*
		FROM
			SP500_TICKER_METADATA
			RIGHT JOIN MARKET_SUMMARY ON SP500_TICKER_METADATA.TICKER = MARKET_SUMMARY.TICKER
		WHERE
			COMPANY_NAME IS NOT NULL
		ORDER BY
			TIMESTAMP,
			TICKER
	),
	lagged AS (
		SELECT *,
			LAG("close") OVER (PARTITION BY TICKER ORDER BY TIMESTAMP) AS prev_close
		FROM SP500_STOCKS_1
	),
	SP500_STOCKS_2 AS (
		SELECT
			*,
			("close" - prev_close)/prev_close AS daily_pct_change,
			AVG("close") OVER (
				PARTITION BY
					TICKER
				ORDER BY
					TIMESTAMP ROWS BETWEEN 2 PRECEDING
					AND CURRENT ROW
			) AS MA03,
			AVG("close") OVER (
				PARTITION BY
					TICKER
				ORDER BY
					TIMESTAMP ROWS BETWEEN 4 PRECEDING
					AND CURRENT ROW
			) AS MA05,
			STDDEV("close") OVER (
				PARTITION BY
					TICKER
				ORDER BY
					TIMESTAMP
			) AS VOLATILITY,
			CASE 
				WHEN (
					("close" - prev_close) > 0
				) THEN 'Advanced'
				ELSE 'Declined'
			END AS adr_category
		FROM
			lagged
		ORDER BY
			TICKER
	),
	volatility_percentiles AS (
		SELECT
    		percentile_cont(0.33) WITHIN GROUP (ORDER BY volatility) AS low_threshold,
    		percentile_cont(0.66) WITHIN GROUP (ORDER BY volatility) AS high_threshold
		FROM
	SP500_STOCKS_2
	),
	SP500_STOCKS_3 AS (
		SELECT 
			SP500_STOCKS_2.*,
			CASE 
				WHEN SP500_STOCKS_2.volatility IS NULL THEN NULL
				WHEN SP500_STOCKS_2.volatility < volatility_percentiles.low_threshold THEN 'Low'
				WHEN SP500_STOCKS_2.volatility < volatility_percentiles.high_threshold THEN 'Moderate'
				ELSE 'High'
			END AS volatility_level
		FROM SP500_STOCKS_2
		CROSS JOIN volatility_percentiles
	)


SELECT
	SP500_STOCKS_2.*,
	SP500_STOCKS_3.volatility_level
FROM
	SP500_STOCKS_2
	LEFT JOIN SP500_STOCKS_3 
		ON SP500_STOCKS_2.TIMESTAMP = SP500_STOCKS_3.TIMESTAMP
		AND SP500_STOCKS_2.ticker = SP500_STOCKS_3.ticker
ORDER BY
	TIMESTAMP, ticker
'''

# Save the query to dataframe
market_summary_df = pd.read_sql_query(query, conn)

# Cleaning up
conn.close()


# In[5]:


# Configure Google Sheets API 

SERVICE_ACCOUNT_FILE = '/Users/nadee/Desktop/PROJECTS/winners-and-losers/config/vidhya-etl-dabd5ec76159.json'  # Path to your downloaded JSON file
SPREADSHEET_ID = config['SPREADSHEET_ID'] # From the URL between /d/...../edit
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 
          'https://www.googleapis.com/auth/drive']  # Read & write scope
RANGE_NAME = 'Sheet1'  # Adjust as needed


# In[6]:


# Authenticate

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes = SCOPES)

sheets_service = build('sheets', 'v4', credentials = creds)
drive_service  = build('drive', 'v3', credentials = creds)


# In[7]:


# Clean timestamp columns and drop otc columns

market_summary_df['dw_last_updated'] = market_summary_df['dw_last_updated'].dt.strftime('%Y-%m-%d')

market_summary_df['timestamp'] = market_summary_df['timestamp'].dt.strftime('%Y-%m-%d')

market_summary_df = market_summary_df.drop(columns = ['otc'])

market_summary_df = market_summary_df.fillna('')


# In[8]:


# Convert dataframe to a list of lists

values = [market_summary_df.columns.to_list()] + market_summary_df.values.tolist()


# In[9]:


# Clear the existing data

sheets_service.spreadsheets().values().clear(
    spreadsheetId = SPREADSHEET_ID,
    range = 'Sheet1',
    body = {}
).execute()

# Write the transformed data

sheets_service.spreadsheets().values().update(
    spreadsheetId = SPREADSHEET_ID,
    range = 'Sheet1!A1',
    valueInputOption = 'USER_ENTERED',
    body={'values': values}
).execute()

print(f"Data uploaded to Google Sheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")