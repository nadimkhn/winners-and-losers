#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import argparse
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from polygon import RESTClient


# In[2]:


# Load the configuration file

file_path = '/Users/nadee/Desktop/PROJECTS/winners-and-losers/config/config.txt'

# Open and load the JSON data
with open(file_path, 'r') as file:
    config = json.load(file)

# Now `data` is a Python dictionary (or list, depending on the file's structure)


# In[3]:


# Pull the required configuration fields from the file

Rest_API_key = config['Rest_API_key']
host = config['host']
dbname = config['dbName']
user = config['user']
password = config['password']
port = config['port']


# In[4]:


# Pull yesterday's daily market summary from API by default

client = RESTClient(Rest_API_key)

prevDay = str(datetime.now().date() - timedelta(days = 1))

def extract_daily_market_summary(date_str):
    """
    Fetches US stock market daily summary for a given date from Polygon.io.

    Parameters
    ----------
    date_str: *str*
        Date in 'YYYY-MM-DD' format.
    
    Returns
    -------
    None
    
    """
    if not isinstance(date_str, str):
        raise TypeError('The date argument needs to be a string')
    
    market_summary_data = client.get_grouped_daily_aggs(
       date_str,
       adjusted="true",
     )

    if market_summary_data:
        # Continue only if Dataframe is not empty
        # Convert it to dataframe
        market_summary_df = pd.DataFrame(market_summary_data)
    
        # Fix timestamp column
        market_summary_df['timestamp'] = pd.to_datetime(
            market_summary_df['timestamp'], unit='ms'
        )
    
        # Add a last updated column
        market_summary_df['dw_last_updated'] = str(
            datetime.now().replace(microsecond = 0)
        )
            
        # Save to CSV
        market_summary_df.to_csv('/Users/nadee/Desktop/PROJECTS/winners-and-losers/raw_data/polygon_{}.csv'.format(date_str), index = False)
        print(f'Saved {date_str} market summary as .csv')
        return market_summary_df
    
    else:
        return pd.DataFrame()
        print(f'No data returned for {date_str}.')


# In[5]:


def load_to_db(df):
    
    # Convert dataframe rows into a list of tuples
    rows = list(df.itertuples(index = False, name = None))
    columns = ", ".join(df.columns)

    # Define connection parameters
    conn = psycopg2.connect( 
        host = host, # pgAdmin 4 > server properties > connection
        dbname = dbname, # pgAdmin 4 > database properties
        user = user, 
        password = password,
        port = port # pgAdmin 4 > server properties > connection
    )
    
    # Create a cursor object to run queries
    cur = conn.cursor()
    
    # Insert the data from dataframe to the table in database
    insert_script = f'''
    INSERT INTO market_summary ({columns})
    VALUES %s
    '''
    
    # Using execute_values for multi-row insert
    execute_values(cur, insert_script, rows)
    conn.commit()
    
    # Clean up
    cur.close()
    conn.close()

    print(f'Appended {len(df)} rows to market summary table')


# In[6]:


def main(date_str):
    if not isinstance(date_str, str):
        raise TypeError('The date argument needs to be a string')

    df = extract_daily_market_summary(date_str)

    if not df.empty:
        load_to_db(df)
    else:
        print(f"No data fetched for {date_str}. Skipping CSV and DB write.")


# In[ ]:


if __name__ == '__main__':

    prevDay = str(datetime.now().date() - timedelta(days = 1))
    parser = argparse.ArgumentParser(
        description = 'Fetch daily market summary from Polygon.io'
    )

    parser.add_argument(
        '-d', '--date',
        help = 'Date in YYYY-MM-DD format',
        default = prevDay
    )

    args = parser.parse_args()
    
    main(args.date)