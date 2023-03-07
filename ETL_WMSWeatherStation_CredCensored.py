# -*- coding: utf-8 -*-
"""
#                   Author: Joshua Thompson
#   O__  ----       Email:  joshuajamesdavidthompson@gmail.com
#  c/ /'_ ---
# (*) \(*) --
# ======================== Script  Information =================================
# PURPOSE: Scrape data from WMS Weather Stations using HOBOlink API 
#
# PROJECT INFORMATION:
#   Name: Scrape data from WMS Weather Stations using HOBOlink API
#
# HISTORY:----
#   Date		        Remarks
#	-----------	   ---------------------------------------------------------------
#	 03/06/2023   Created script                                   JThompson (JT)
#===============================  Environment Setup  ===========================
"""

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
import pyodbc


class HobolinkScraper:
    def __init__(self):
        self.eastern = pytz.timezone('US/Eastern')
        self.access_token = self._get_access_token()

    def _get_access_token(self):
        url = "https://webservice.hobolink.com/ws/auth/token"
        body = {
            "grant_type": "client_credentials", 
            "client_id": "userid", #!!censored for github
            "client_secret": "secret", #!!censored for github
            "scope": "api_suburbperformance_read"
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        response = requests.post(url, data=body, headers=headers)

        # Extract the hobo access token from the post response
        data = json.loads(response.text)
        return data["access_token"]

    def run(self):
        now = datetime.now(self.eastern).replace(second=0, minute=0).strftime('%Y-%m-%d %H:%M:%S')
        then = (datetime.now(self.eastern).replace(second=0, minute=0)-timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')

        url = "https://webservice.hobolink.com/ws/data/file/JSON/user/numericid" #!!censored for github
        params = {
            "loggers": "00000001,00000002,00000003", #!!censored for github
            "start_date_time": then, 
            "end_date_time": now    
        }
        header = {'Authorization': "Bearer {}".format(self.access_token)}
        get_resp = requests.get(url, params=params, headers=header)

        try:
            # Get the JSON data from the response
            data_get_resp = get_resp.json()
        except json.JSONDecodeError:
            # Print the response text if it's not in JSON format to see what went wrong
            print(get_resp.text)

        loggers_dfjson = pd.DataFrame(data_get_resp)
        loggers_df = loggers_dfjson.observation_list.apply(pd.Series)
        print(loggers_df)
        
        ## Define BWPR SQL Server connection params
        server = 'server' #!!censored for github
        database = 'BWPR'
        username = 'username' #!!censored for github
        password = 'password' #!!censored for github

        ## Create a connection to the SQL Server database using pyodbc
        conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        cnxn = pyodbc.connect(conn_str)

        # Save the DataFrame to a SQL Server table
        table_name = 'BWPR_WeatherData'
        loggers_df.to_sql(table_name, cnxn, if_exists='append', index=False)

        # Close the database connection
        cnxn.close()
        

if __name__ == "__main__":
    scraper = HobolinkScraper()
    scraper.run()
