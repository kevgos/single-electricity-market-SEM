import re
import pandas as pd
from datetime import datetime, date, timedelta
import xml.etree.ElementTree as ET
import csv
from urllib.request import urlopen
import pytz
import os

def semo_xml_parser(input_xml):
    """Function to parse SEMO xml reports file or URL and return it as a pandas dataframe"""
    if input_xml.startswith('http'):
        response = urlopen(input_xml)
        tree = ET.parse(response)
        file_name = os.path.basename(input_xml)  # Extract the file name from the URL
    else:
        tree = ET.parse(input_xml)
        file_name = os.path.basename(input_xml)  # Extract the file name from the file path
    
    report_type = file_name.split("_")[0:2]  # Split the file name
    report_type = "_".join(report_type)
    #print(f"Report Type: {report_type}")  # Debugging line
    
    root = tree.getroot()
    data = []
    for child in root.findall(f"{report_type}"):
        entry = {}
        for subchild in child.attrib:
            entry[subchild] = child.attrib[subchild]
        data.append(entry)
    
    df = pd.DataFrame(data)
    if 'ROW' in df.columns:
        df.drop(columns='ROW', inplace=True)
    
    return df


def dam_price_from_api(start_date, end_date):
    """Returns a csv file with SEM DAM prices between the specified start_date and end_date in UTC"""
    # create an empty list to add all the dataframes to
    list_of_dfs = []
    
    # API Step 1: Query Report List
    api_url = "https://reports.semopx.com/api/v1/documents/static-reports"
    query_params = {
    "Group": "Market Data",
    "Date": f'>={start_date}<={end_date}', #<=
    "page_size": 500, # Adjust as needed based on the expected number of reports per day
    "DPuG_ID": "EA-001", # market result ID
    "sort_by": "PublishTime", }
   
    response_semo = requests.get(api_url, params=query_params)
    report_list_semo = response_semo.json()

    # from the report list filter for DAM and then get the report name to get the data
    for item in report_list_semo['items']:
        if re.findall(r'MarketResult_SEM-DA', item['ResourceName']):
            prices_dict = {}
            report_name = item['ResourceName'] # assign the name of the DAM bid ask curve file to this variable
            #print(report_name)
            individ_report_url = f"https://reports.semopx.com/documents/{report_name}"
            #print(individ_report_url)

            # use urlopen package and csv reader (both built-in) to read through the url linking csv data
            response = urlopen(individ_report_url)
            #print(response)
            lines = [line.decode('utf-8') for line in response.readlines()]
            csv_reader = csv.reader(lines, delimiter=';')
            for number, row in enumerate(csv_reader):
                if number == 7:
                    prices_dict['Datetime(UTC)'] = row
                if number == 8:
                    prices_dict['€/MWh'] = row
                if number == 11:
                    prices_dict['GBP/MWh'] = row
                else:
                    continue
            prices_df = pd.DataFrame(prices_dict)
            list_of_dfs.append(prices_df)
            final_df = pd.concat(list_of_dfs)
    # change the date to local time for Europe/Dublin from UTC, keep the old UTC column for now too
    final_df['Datetime(Dublin)'] = final_df['Datetime(UTC)'].str.replace('Z', '')
    final_df['Datetime(Dublin)'] = final_df['Datetime(Dublin)'].str.replace('T', ' ')
    final_df['Datetime(Dublin)'] = pd.to_datetime(final_df['Datetime(Dublin)'])
    local_timezone = pytz.timezone('Europe/Dublin')  # assign local timezone to be Europe/Dublin
    final_df['Datetime(Dublin)'] = final_df['Datetime(Dublin)'].dt.tz_localize('UTC').dt.tz_convert(local_timezone)
    final_df['Datetime(Dublin)'] = final_df['Datetime(Dublin)'].dt.strftime('%d-%m-%Y %H:%M')
    final_df['Market'] = 'SEM-DA'
    return(final_df) # return the final pandas dataframe of hourly prices


def market_results_parser(market_file):
    """Function that takes a market results file EA-001 and returns market price and participants positions as a pandas dataframe."""
    structured_data = [] # for market_df to append
    prices_dict = {} # for prices_df to be generated
    
    #first it gathers prices_df
    with open(market_file, 'r') as file:
        reader = csv.reader(file, delimiter = ';') # file is ; delimited and also need to convert , to . for decimal period
        for number, row in enumerate(reader):
            if number == 7:
                prices_dict['Datetime'] = row
            if number == 8:
                prices_dict['€/MWh'] = row
            if number == 11:
                    prices_dict['GBP/MWh'] = row
            else:
                continue
        prices_df = pd.DataFrame(prices_dict)
        prices_df['€/MWh'] = prices_df['€/MWh'].str.replace(',', '.')
        prices_df['GBP/MWh'] = prices_df['GBP/MWh'].str.replace(',', '.')

    # now read the file and gather portfolio data, each time it see 'Portfolio' captures the data
    with open(market_file, 'r') as file:
        reader = csv.reader(file, delimiter = ';')
        rows = list(reader) # convert to a list of rows so don't need iterate
        # can use an index to keep track of the rows
        i = 0 # initialize the indexer
        while i < len(rows):
            if rows[i][0] == "Portfolio":
                # get portfolio info from 1st line
                portfolio_info = rows[i]
                portfolio = portfolio_info[1]
                unit = portfolio_info[2]
                duration = portfolio_info[3]
                currency = portfolio_info[4]

                order_type = rows[i + 1][0]

                datetime = rows[i + 2]
                voeqs = rows[i + 3] # value of executed quantity is (MWh * clearing price) ?
                order_id = rows[i + 4]

                for dt, voeq, id_ in zip(datetime, voeqs, order_id): # these are longer lists than portfolio etc which are only 1 item, need to iterate these too
                    structured_data.append([portfolio, unit, order_type, dt, voeq.replace(',', '.'), id_, currency]) # portfolio is repeated, but datetime etc is added through the above iteration

                # go forward a line and look for next portfolio block
                i += 1
            else:
                i += 1

        columns = ['Portfolio', 'Unit', 'Order Type', 'Datetime', 'VOEQ', 'OrderID', 'Currency']
        market_df = pd.DataFrame(structured_data, columns = columns)
        
        # now have market df want to make prices df and merge on date
        merged_df = pd.merge(market_df, prices_df, how = 'inner', on = 'Datetime')

        # clean up datetime and put it in local time, keep it as a new column
        merged_df['Local time'] = merged_df['Datetime'].str.replace('Z', '')
        merged_df['Local time'] = merged_df['Local time'].str.replace('T', ' ')
        merged_df['Local time'] = pd.to_datetime(merged_df['Local time'])
        local_timezone = pytz.timezone('Europe/Dublin')  # convert ida1 utc to local irish time
        merged_df['Local time'] = merged_df['Local time'].dt.tz_localize('UTC').dt.tz_convert(local_timezone)
        merged_df['Local time'] = merged_df['Local time'].dt.strftime('%d-%m-%Y %H:%M')
        merged_df = merged_df.rename(columns={'Datetime': 'Datetime(UTC)'})

        # add in the market details so have them in case working with different timeframes
        with open(market_file, 'r') as file:
            reader = csv.reader(file, delimiter = ';')
            rows = list(reader) # convert to a list of rows so don't need iterate
            market_name = rows[0][1]
            Eur_to_Pound_FX = rows[4][2]

        merged_df['Market'] = market_name
        merged_df['€_£_FX'] = Eur_to_Pound_FX.replace(',', '.')
        
        
        return merged_df
