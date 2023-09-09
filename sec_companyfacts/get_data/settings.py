"""
Parameters used in downloading the dataset
"""

"""
Parameter Name: DATA_SIZE
Parameter Description: The data size used in downloading, number indicate the number of companies to download data for by market cap in descending order
Parameter Available Values: 20, 100, 500
Parameter Variable Type: int
Parameter Default Value: 20
"""
DATA_SIZE = 20

##############################################################
"""
IMPORTANT

THE DEFAULT LOGIC FOR INCLUDE/EXCLUDE PARAMETERS:
INCLUDE ALL

PRIORITY FOR INCLUDE/EXCLUDE PARAMETERS:
1. EXPLICIT OVER IMPLICIT
2. EXPLICIT EXCLUSION OVER EXPLICIT INCLUSION

Consider the following example:

INCLUDE_TICKERS = [
    'AAPL'
    'MSFT'
    'GOOG'
]

EXCLUDE_TICKERS = [
    'AAPL'
]

Since AAPL, MSFT and GOOG are explicitly stated, any other tickers will not be included.
Since AAPL is also explicitly stated to be excluded, it will not be included.

IT IS RECOMMENDED TO USE 
EITHER
 INCLUDE OR EXCLUDE 
BUT 
 NOT BOTH
"""
##############################################################

"""
Parameter Name: INCLUDE_UNITS
Parameter Description: The unit name to be included in downloading
Parameter Available Values: All unit names available, which can be gotten from support/metric_nld.feather file 
Parameter Variable Type: list
Parameter Default Value: [] (not checked against)
"""
INCLUDE_UNITS = [

]

"""
Parameter Name: EXCLUDE_UNITS
Parameter Description: The unit name to be excluded in downloading
Parameter Available Values: All unit names available, which can be gotten from support/metric_nld.feather file 
Parameter Variable Type: list
Parameter Default Value: [] (not checked against)
"""
EXCLUDE_UNITS = [

]

"""
Parameter Name: INCLUDE_METRICS
Parameter Description: The metric name to be includedcluded in downloading
Parameter Available Values: All metric names available, which can be gotten from support/metric_nld.feather file 
Parameter Variable Type: list
Parameter Default Value: [] (not checked against)
"""
INCLUDE_METRICS = [

]

"""
Parameter Name: EXCLUDE_METRICS
Parameter Description: The metric name to be excluded in downloading
Parameter Available Values: All metric names available, which can be gotten from support/metric_nld.feather file 
Parameter Variable Type: list
Parameter Default Value: [] (not checked against)
"""
EXCLUDE_METRICS = [

]

"""
Parameter Name: INCLUDE_TICKERS
Parameter Description: The stock ticker to be included in downloading
Parameter Available Values: All stock tickers available in support/company_tickers_exchange.feather file 
Parameter Variable Type: list
Parameter Default Value: [] (not checked against)
"""
INCLUDE_TICKERS = [

]

"""
Parameter Name: EXCLUDE_TICKERS
Parameter Description: The stock ticker to be excluded in downloading
Parameter Available Values: All stock tickers available in support/company_tickers_exchange.feather file 
Parameter Variable Type: list
Parameter Default Value: [] (not checked against)
"""
EXCLUDE_TICKERS = [

]

"""
Parameter Name: SUPPORT_FILES
Parameter Description: Extra support files to download
Parameter Available Values: metrics_nld
Parameter Variable Type: list
Parameter Default Value: None

Unless explicitly stated in SUPPORT_FILES variable, any extra supporting files will not be downloaded.
"""
SUPPORT_FILES = [
    'metrics_nld',
]

"""
Parameter Name: REBUILD_INDEX
Parameter Description: Rebuilding local files cache index after data download is finished
Parameter Available Values: True/False
Parameter Variable Type: boolean
Parameter Default Value: False

Rebuilding local cache index can be started independently by executing rebuild_index_local.py 
Python file location: sec_companyfacts/get_data/get_ipfs_files/rebuild_index_local.py
"""
REBUILD_INDEX = False
