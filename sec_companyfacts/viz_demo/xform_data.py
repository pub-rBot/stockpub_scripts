"""
Transform data into format suitable for plotting

End Goal: Make a timelapse gif for most popular metrics for selected stocks
Starting Material: SEC company facts dataset, CIK to ticker index file

To realize the end goal, we need to transform the abstract to quantifiable values
For this demo, we will define the following

1.
Timelapse = 1year period from current date going into the past
2.
Most popular = the metrics that exist for most numbers of stocks. Top 10 by count
    - with USD or USD/shares as unit type
3.
selected_stocks = [
    'AAPL',
    'MSFT',
    'GOOG',
    'AMZN',
    'META',
    'ORCL',
]
"""

"""
Package imports
"""

import os
import pandas as pd
from pathlib import Path
from os.path import exists
import sys
from os.path import join as os_path_join
import datetime
from datetime import timedelta

print("xform_data.py has started")
"""
HOUSE KEEPING CODE BLOCKS
"""

"""
General options and vars used throughout the script
"""

pd.set_option('display.max_columns', None)  # set pandas to display all columns
pd.set_option('display.max_rows', None)  # set pandas to display all rows
date_format = "%Y-%m-%d"  # format used for dates as a str type, YYYY-MM-DD format allows us to compare dates as strings until after year 9999
nan = float('nan')  # nan value as a variable

"""
Relative Paths
"""

script_path = os.path.abspath(os.path.realpath(__file__))  # current script path as str
pathlib_path = Path(script_path)   # path lib object of current script's path, make it easier to transverse the path tree
script_p1_name = str(pathlib_path.parent.parent.resolve().name)
script_p3_path = str(pathlib_path.parent.parent.parent.parent.resolve())

data_root_folder = os_path_join(script_p3_path, "stockpub_data")
dataset_folder = os_path_join(data_root_folder, script_p1_name)
data_folder = os_path_join(dataset_folder, "data")
time_machine_folder = os_path_join(data_folder, "time_machine")
support_folder = os_path_join(data_folder, "support")

# The data_folder is where the data should be located,
# the relative paths assumes the default relativity for paths
# uncomment print() and quit() if want to double check,
# recomment after checking
# print(data_folder)
# quit()

"""
DATA TRANSFORMATION CODE BLOCKS
"""

"""
1.
Timelapse = 1year period from current date going into the past

Get time into proper string formats
"""

today_dt = datetime.datetime.today()
today_date = today_dt.strftime(date_format)
today_m1y_date = (today_dt - datetime.timedelta(days=365)).strftime(date_format)

"""
2.
Most popular = the metrics that exist for most numbers of stocks.
    - with USD or USD/shares as unit type

Dataset Storage Structure

stockpub_data
└── sec_companyfacts
    └── data
        ├── support
        │   └── metrics_nld.feather
        └── time_machine
            └── by_units
                └── [unit_name]
                    └── by_metrics
                        └── [metric_name]
                            └── by_ciks
                                └── CIK[0000000000]
                                    └── YYYY-MM-DD.feather

We transverse the data directory and check which metrics have the most CIK (companies)

"""

ciks_count_list = []  # we declare a list to store ciks count, so we can determine most popular metric by number of ciks

by_units_folder = os_path_join(time_machine_folder, "by_units")
by_units_subfolder_names = os.listdir(by_units_folder)

for unit_name in by_units_subfolder_names:  # tree structure location: code line 102-104
    if (unit_name == "USD") or (unit_name == "USD(p)shares"):  # we only take metrics that have USD or USD/shares as unit type
        pass
    else:
        continue
    by_metrics_folder = os_path_join(by_units_folder, unit_name, "by_metrics")
    by_metrics_subfolder_names = os.listdir(by_metrics_folder)

    for metric_name in by_metrics_subfolder_names:  # tree structure location:  code line 104-106
        by_ciks_folder = os_path_join(by_metrics_folder, metric_name, "by_ciks")
        by_ciks_subfolder_names = os.listdir(by_ciks_folder)
        by_ciks_count = len(by_ciks_subfolder_names)  # get number of ciks in the metric folder

        ciks_count_dict = {}
        ciks_count_dict['unit_name'] = unit_name
        ciks_count_dict['metric_name'] = metric_name
        ciks_count_dict['count'] = by_ciks_count
        ciks_count_list.append(ciks_count_dict)  # append the dictionary to the list we declared earlier

if len(ciks_count_list) == 0:
    sys.exit("ciks_count_list is empty")  # we exit out of script with a message if we cant get any viable files after transversing
else:
    ciks_count_df = pd.DataFrame(ciks_count_list)  # we convert the list to a dataframe so it can be sorted easily
    # we sort the dataframe by count in descending order and metric name in ascending order,
    # and we do this in place instead of creating another dataframe
    ciks_count_df.sort_values(by=['count', 'metric_name'], ascending=[False, True], inplace=True)

    # we first get the cut off count limit for top 10 by count
    cutoff_count = ciks_count_df['count'].iloc[9]  # panda indexing starts at 0
    # we then slice the dataframe so that metrics with equal count at cut off limit are also included
    top_metrics_df = ciks_count_df[(ciks_count_df['count'] >= cutoff_count)]

"""
3.
selected_stocks = [
    'AAPL',
    'MSFT',
    'GOOG',
    'AMZN',
    'META',
    'ORCL',
]

Bridge exchange tickers to SEC CIKs
"""
selected_stocks = [
    'AAPL',
    'MSFT',
    'GOOG',
    'AMZN',
    'META',
    'ORCL',
]
tickers_index_fileloc = os_path_join(support_folder, "company_tickers_exchange.feather")  # get the file location of the index
tickers_index_df = pd.read_feather(tickers_index_fileloc)  # read the file into pandas dataframe

tickers_index_df = tickers_index_df[(tickers_index_df['ticker'].isin(selected_stocks))]  # only keep the rows where the ticker value is in selected_stocks list

# Explicitly check for empty dataframe
# Note: while it is not need for the demo, code line 172 could return an empty dataframe
# We exit out of script if top_metrics_df is empty or tickers_index_df is empty
if (top_metrics_df.empty) or (tickers_index_df.empty):
    sys.exit("top_metrics_df or tickers_index_df is empty")

"""
TRANSFORM DATA

Note:
The data transformation techniques used below are generalizable
Using the transformed data for comparative analysis is debatable
"""

top_metrics_rec = top_metrics_df.to_dict('records')  # convert dataframe to records for faster looping
ciks_rec = tickers_index_df.to_dict('records')  # convert cik_str column to a list for faster looping
viz_datum_frames = []  # empty list to store all the data files
for top_metrics_row in top_metrics_rec:
    unit_name = top_metrics_row['unit_name']
    metric_name = top_metrics_row['metric_name']
    by_ciks_folder = os_path_join(time_machine_folder, "by_units", unit_name, "by_metrics", metric_name, "by_ciks")  # get the path for by_ciks folder

    for ciks_row in ciks_rec:
        cik_str = ciks_row['cik_str']
        ticker = ciks_row['ticker']
        cik_folder_name = "CIK" + cik_str
        cik_folder = os_path_join(by_ciks_folder, cik_folder_name)  # the path for specific cik
        if exists(cik_folder):
            files = os.listdir(cik_folder)  # the files in the specific cik folder
            file_dates = [x.replace(".feather", "") for x in files if x.endswith(".feather")]  # double check if the file ends with .feather, then get the YYYY-MM-DD file date
            file_date_max = max(file_dates)  # get the most recent date (for the demo, we will only use the most recent data instance file)

            if file_date_max < today_m1y_date:
                continue  # if we don't have a file that is >= last years date, we move on

            data_fileloc = os_path_join(cik_folder, file_date_max + ".feather")
            # read the most recent file into dataframe
            # since we know what we want in the end
            # to save memory and improve speed, we only need to take 3 columns
            # ["filed", "take_val_sp", "take_count_sp"]
            data_file_df = pd.read_feather(data_fileloc, columns=["filed", "take_val_sp", "take_count_sp"])

            # to use the full data file and see what it looks like:
            # uncomment the follow 3 code lines
            # data_file_df = pd.read_feather(data_fileloc)
            # print(data_file_df)
            # quit()  #  exit out of script, so we can see what 1 data file looks like

            # so far, we have checked the viability of the file externally,
            # we also need to check the viability of the file internally
            take_count_sp = data_file_df['take_count_sp'].iloc[-1]  # get the value of how many rows we can take that are viable from the last row of the dataframe
            if take_count_sp == 0:
                continue  # we move on if we don't have a viable row to take

            data_file_df = data_file_df.tail(take_count_sp)  # we then take only those rows that are viable

            # we then get the list of filed dates
            filed_col_list = data_file_df['filed'].to_list()
            # since we are making a time-lapse,
            # we want the dataframe to have a bit of an over hang,
            # at or exceeding last year's date, so we don't have missing values at beginning of the year
            filed_col_list = [x for x in filed_col_list if x <= today_m1y_date]

            if len(filed_col_list) == 0:
                continue  # if we don't have a date for overhang, we move on

            take_from_date = max(filed_col_list)  # we then take the max date of the list to use
            data_file_df = data_file_df[(data_file_df['filed'] >= take_from_date)]  # we then take only those filed dates that are >= the take_from_date

            if abs(len(data_file_df) - 5) > 1:
                # we do 1 last check for regularity for the dataframe we have sanitized so far
                # if you go back to code line 206-207
                # we are using the most recent file
                # but most recent can fall under any dates between now and 1 year ago
                # stockPub take_count_sp checks for regularity as far back for a particular data file instance
                # after slicing the original dataframe, it is best to explicitly have some way to check for regularity for the datapoints
                # publicly traded U.S. based companies are required by law to report quarterly financial results, or 4 reports per year
                # since we are also including an overhang datapoint, we will use 5 to check
                # we will also use 1 report as the "wiggle" difference in case our timeframe included or excluded the two extreme date points at both ends
                continue  # if the dataframe is not regular, we move on

            """
            Transform the data
            
            We are finally ready to transform the data,
            If you noticed, we had to make quite a few judgement calls to arrive at this stage
            
            And we will be forced to make more in the transformation stage
            
            Identify the problems
            1. multivariate
                metric values comes in all sizes
            2. metric values can be <= 0
            
            To solve both problems
                Binning (more precisely, min-max scaling)
            """

            # for a quick look at what the dataframe looks like at this point
            # uncomment the two code lines below
            # print(data_file_df)
            # quit()

            # companies reports quarterly earnings
            # that is not a lot of data points for a year
            # nor it is smooth between each transition
            # we can solve that by filling in the blanks between data points
            # whether this technique can be used for comparative analysis is debatable
            # since it uses future data point to fill in missing values of the past
            # but it does make a better looking time-lapse
            # we first convert the filed column to pandas datetime so we can easily get difference in days
            data_file_df['filed'] = data_file_df['filed'].apply(pd.to_datetime)
            # we then shift the filed and take_val_sp columns down by 1 so we can get the difference in days and value
            data_file_df['filed_prev'] = data_file_df['filed'].shift(1)
            data_file_df['take_val_sp_prev'] = data_file_df['take_val_sp'].shift(1)
            # get the difference in days and value
            data_file_df['days_diff'] = (data_file_df['filed'] - data_file_df['filed_prev']).dt.days
            data_file_df['vals_diff'] = (data_file_df['take_val_sp'] - data_file_df['take_val_sp_prev'])
            # we then drop the first row of the dataframe since it is no longer needed
            data_file_df.drop(data_file_df.head(1).index, inplace=True)
            last_filed_val = data_file_df['filed'].iloc[-1]
            # we then compile the filled-in dataframe
            data_file_rec = data_file_df.to_dict('records')  # convert df to records
            smooth_data_list = []
            for data_file_row in data_file_rec:
                # necessary vars
                filed = data_file_row['filed']
                take_val_sp = data_file_row['take_val_sp']
                vals_diff = data_file_row['vals_diff']
                days_diff = data_file_row['days_diff']

                # append actual filed date value and document it
                smooth_data_list.append({"date": filed, "val": take_val_sp, 'actual': 1})

                # extend viz_data_list to have filled in values
                vals_diff_per_day = vals_diff / days_diff
                smooth_data_list.extend([{"date": filed - timedelta(days=x), "val": take_val_sp - (vals_diff_per_day * x), 'actual': 0} for x in range(1, int(days_diff))])

                if filed == last_filed_val:
                    # we fill in the remainder values with the most current value for a prettier graph
                    filed_dt = filed.to_pydatetime()  # convert pandas date time to python date time
                    days_forward = (today_dt - filed_dt).days  # get days between today's date and last filed date
                    smooth_data_list.extend([{"date": filed + timedelta(days=x), "val": take_val_sp, 'actual': 0} for x in range(1, int(days_forward + 1))])  # plus 1 so we get today's date as well

            #  convert the list to dataframe and take dates > last year date
            smooth_data_df = pd.DataFrame(smooth_data_list)
            smooth_data_df = smooth_data_df[(smooth_data_df['date'] > today_m1y_date)]
            smooth_data_df.sort_values(by=['date'], ascending=True, inplace=True)
            val_max = smooth_data_df['val'].max()
            val_min = smooth_data_df['val'].min()
            if (val_max != val_max) or (val_min != val_min):
                # hackish way to test for NaN values
                # if NaN, move on
                continue
            val_range = val_max - val_min

            if val_range <= 0:
                # if range is <= 0, there is nothing to bin
                continue

            # transform the data to be between 0 and 1
            smooth_data_df['xformed_val'] = smooth_data_df.apply(lambda row: (row['val'] - val_min) / val_range, axis=1)

            # min max scaling is essentially binning
            # uncomment next 2 lines to get a ranked_val column
            # import math
            # smooth_data_df['ranked_val'] = smooth_data_df.apply(lambda row: max(math.ceil(row['xformed_val'] * 10), 0), axis=1)

            # uncomment next 3 lines to see the before transformation dataframe and smoothed dataframe
            # print(data_file_df)
            # print(smooth_data_df)
            # quit()

            """
            Plotting specific operations
            """
            # change the scale from [0:1] to [-1:1]
            smooth_data_df['xformed_val'] = (smooth_data_df['xformed_val'] - 0.5) * 2

            # add identifying columns
            smooth_data_df['unit_name'] = unit_name
            smooth_data_df['metric_name'] = metric_name
            smooth_data_df['cik_str'] = cik_str
            smooth_data_df['ticker'] = ticker
            viz_datum_frames.append(smooth_data_df)  # append the data frame to list

if len(viz_datum_frames) > 0:
    viz_data_df = pd.concat(viz_datum_frames)  # we concat all the datum frames to one data frame
    # we convert the date column back to just date with no 00:00:00 when saving to some formats
    # this also allows us to use it as a file name
    viz_data_df['date'] = viz_data_df['date'].dt.date.astype(str)
    viz_datum_dfs = dict(tuple(viz_data_df.groupby(viz_data_df['date'])))  # we then split the data frame into multiple data frames based on date

    # make a relative directory to store the viz data
    derived_data_root_folder = os_path_join(script_p3_path, "stockpub_data_derived")
    script_parent_name = str(pathlib_path.parent.resolve().name)
    viz_demo_folder = os_path_join(derived_data_root_folder, script_parent_name)
    viz_data_folder = os_path_join(viz_demo_folder, "data")
    if not exists(viz_data_folder):
        os.makedirs(viz_data_folder)

    # loop and save each dataframe to disk
    for date, df in viz_datum_dfs.items():

        df_fileloc = os_path_join(viz_data_folder, date + ".feather")
        df.reset_index(drop=True, inplace=True)  # need to reset index if out of order for saving feather files
        df.to_feather(df_fileloc)

        # uncomment to also save a csv version
        # NaN will be represented by "na"
        # We don't save the row index
        # We want the column header names
        # df.to_csv(df_fileloc.replace(".feather", ".csv"), na_rep="na", index=False, header=True)

print("xform_data.py has finished")
up_next = """
UP NEXT:
see
plot_data.py
"""
print(up_next)
