# single-electricity-market-SEM
Functions and objects for collecting and parsing data from the Irish Single Electricity Market (SEM).

## Overview
The Single Electricity Market (SEM) in Ireland is an all-island electricity market. Its two main participants are generators and suppliers.
The Single Electricity Market Operator (SEMO) is a monopoly body which is responsible for operating the balancing market. SEMOpx operates 
the day-ahead and intraday spot markets.

As part of their market operator obligations SEMOpx and SEMO publish daily market results and other market data. This is available through
their websites as dynamic or static reports. Static reports are also available through the SEMO/SEMOpx API. Data is shared as XML or CSV files
but is typically not ready for analysis and must be parsed or cleaned in to a dataframe.

This repository provides tools for parsing and analyzing data from the Single Electricity Market Operator (SEMO) in Ireland. It includes utilities to extract and process market prices, participant positions, and XML-based reports.


## Requirements

- Python 3.8+
- pandas
- pytz
- requests

## Code Provided 

Code and functions shared here and aimed at retrieving auction results from the SEMO or SEMOpx API using Python. The data is typically returned in a Pandas
dataframe for analysis.

## Usage

**XML parser**

from modules.semo_parser import semo_xml_parser

df = semo_xml_parser("https://reports.semopx.com/documents/MarketResult_SEM-DA_20250520.xml")
print(df.head())

**Market Results Parser**

from modules.semo_parser import market_results_parser

df = market_results_parser("EA-001_MarketResults_20250520.csv")
print(df.head())


