import shutil
import pandas as pd
from os.path import exists
from os.path import join as os_path_join
import os
import importlib.util
import sys
from pathlib import Path
import datetime
import numpy as np
import warnings
import pickle

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
script_path = os.path.abspath(os.path.realpath(__file__))

mssg = script_path + " has started at: "
print_out_len = 128
print_out_len_m1 = 128 - 1
print("-" * print_out_len)
print((mssg + str(datetime.datetime.today())).ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")


def get_keep_status(df_row, tm_folder_loc, cids_list, include_units, exclude_units, include_metrics, exclude_metrics, include_ciks, exclude_ciks):
    keep_status = 1

    row_unit = df_row['unit']
    row_metric = df_row['metric']
    row_cik_str = df_row['cik_str']
    row_date = df_row['date']
    row_CID = df_row['CID']

    local_file_path = os_path_join(tm_folder_loc, "by_units", row_unit, "by_metrics", row_metric, "by_ciks", ("CIK" + row_cik_str), (row_date + ".feather"))
    if (cids_list != []) and (exists(local_file_path)):
        if row_CID in cids_list:
            keep_status = 0
    if (include_units != []) and (keep_status == 1):
        if row_unit not in include_units:
            keep_status = 0
    if (exclude_units != []) and (keep_status == 1):
        if row_unit in exclude_units:
            keep_status = 0
    if (include_metrics != []) and (keep_status == 1):
        if row_metric not in include_metrics:
            keep_status = 0
    if (exclude_metrics != []) and (keep_status == 1):
        if row_metric in exclude_metrics:
            keep_status = 0
    if (include_ciks != []) and (keep_status == 1):
        if row_cik_str not in include_ciks:
            keep_status = 0
    if (exclude_ciks != []) and (keep_status == 1):
        if row_cik_str in exclude_ciks:
            keep_status = 0
    return keep_status


pathlib_path = Path(script_path)
script_p1_path_pathlib = pathlib_path.parent.parent.resolve()
script_p1_path = str(script_p1_path_pathlib)

script_p2_path_pathlib = pathlib_path.parent.parent.parent.resolve()
script_p2_name = script_p2_path_pathlib.name

script_p4_path_pathlib = pathlib_path.parent.parent.parent.parent.parent.resolve()
script_p4_path = str(script_p4_path_pathlib)

data_root_folder = os_path_join(script_p4_path, "stockpub_data")
dataset_folder = os_path_join(data_root_folder, script_p2_name)
data_folder = os_path_join(dataset_folder, "data")
time_machine_folder = os_path_join(data_folder, "time_machine")
support_folder = os_path_join(data_folder, "support")
temp_files_folder = os_path_join(support_folder, "temp_files")
data_cids_index_local_fileloc = os_path_join(temp_files_folder, "data_cids_index_local.feather")
data_cids_index_get_parafolder = os_path_join(temp_files_folder, "data_cids_index_get")
if not exists(data_cids_index_get_parafolder):
    os.makedirs(data_cids_index_get_parafolder)
else:
    shutil.rmtree(data_cids_index_get_parafolder)
    os.makedirs(data_cids_index_get_parafolder)

data_cids_index_local_list = []
if exists(data_cids_index_local_fileloc):
    data_cids_index_local = pd.read_feather(data_cids_index_local_fileloc)
    data_cids_index_local_list = data_cids_index_local['CID'].to_list()

company_tickers_fileloc = os_path_join(support_folder, "company_tickers.feather")
company_tickers_exchange_fileloc = os_path_join(support_folder, "company_tickers_exchange.feather")

if exists(company_tickers_exchange_fileloc):
    try:
        company_tickers = pd.read_feather(company_tickers_exchange_fileloc)
    except Exception as e:
        if exists(company_tickers_fileloc):
            try:
                company_tickers = pd.read_feather(company_tickers_fileloc)
            except Exception as e:
                print(e)
                sys.exit("Something went wrong trying to read sec company ticker index file, exiting ...")
        else:
            sys.exit("Please run get_http_files.py first to get central indexes")

elif exists(company_tickers_fileloc):
    try:
        company_tickers = pd.read_feather(company_tickers_fileloc)
    except Exception as e:
        print(e)
        sys.exit("Something went wrong trying to read sec company ticker index file, exiting ...")
else:
    sys.exit("Please run get_http_files.py first to get central indexes")


if not company_tickers.empty:
    company_tickers['include'] = 1
    company_tickers['exclude'] = 0
else:
    sys.exit("Company ticker index is empty, exiting ...")

src_module_name = "settings"
src_module_path = os_path_join(script_p1_path, "settings.py")
spec = importlib.util.spec_from_file_location(src_module_name, src_module_path)
settings = importlib.util.module_from_spec(spec)
sys.modules[src_module_name] = settings
spec.loader.exec_module(settings)

data_cids_index = pd.DataFrame()
DATA_SIZES = [20, 100, 500]
try:
    DATA_SIZE = settings.DATA_SIZE
    DATA_SIZE = max([x for x in DATA_SIZES if x <= DATA_SIZE])
    DATA_SIZE = str(DATA_SIZE)
    try:
        data_cids_index_filename = "".join(["data_cids_index_", DATA_SIZE, ".feather"])
        data_cids_index_fileloc = os_path_join(support_folder, data_cids_index_filename)
        data_cids_index = pd.read_feather(data_cids_index_fileloc)
    except Exception as e:
        print(e)
        sys.exit("Something went wrong trying to read data cids index, exiting ...")
except Exception as e:
    print(e)
    sys.exit("DATA_SIZE not found in settings, exiting ...")

if data_cids_index.empty:
    sys.exit("Data cid index is empty, exiting ...")

data_cids_index_len_total = len(data_cids_index)

INCLUDE_UNITS = []
try:
    INCLUDE_UNITS = settings.INCLUDE_UNITS
    if type(INCLUDE_UNITS) is not list:
        warnings.warn("The specified INCLUDE_UNITS is not a list, including all unit types ...", stacklevel=2)
except Exception as e:
    pass
EXCLUDE_UNITS = []
try:
    EXCLUDE_UNITS = settings.EXCLUDE_UNITS
    if type(EXCLUDE_UNITS) is not list:
        warnings.warn("The specified EXCLUDE_UNITS is not a list, excluding none of the unit types ...", stacklevel=2)
except Exception as e:
    pass
INCLUDE_METRICS = []
try:
    INCLUDE_METRICS = settings.INCLUDE_METRICS
    if type(INCLUDE_METRICS) is not list:
        warnings.warn("The specified INCLUDE_METRICS is not a list, including all metric types ...", stacklevel=2)
except Exception as e:
    pass
EXCLUDE_METRICS = []
try:
    EXCLUDE_METRICS = settings.EXCLUDE_METRICS
    if type(EXCLUDE_METRICS) is not list:
        warnings.warn("The specified EXCLUDE_METRICS is not a list, excluding none of the metric types ...", stacklevel=2)
except Exception as e:
    pass

INCLUDE_TICKERS = []
INCLUDE_CIKS = []
try:
    INCLUDE_TICKERS = settings.INCLUDE_TICKERS
    if type(INCLUDE_TICKERS) is not list:
        warnings.warn("The specified INCLUDE_TICKERS is not a list, including all tickers ...", stacklevel=2)
    else:
        if len(INCLUDE_TICKERS) > 0:
            INCLUDE_TICKERS_sec_standard = [(x.replace(".", "-")).upper() for x in INCLUDE_TICKERS if x != ""]
            INCLUDE_TICKERS_period = [x for x in INCLUDE_TICKERS_sec_standard if x not in INCLUDE_TICKERS]
            if len(INCLUDE_TICKERS_period) > 0:
                warnings.warn(f"Share class converted to SEC standard using dashes for following tickers: {INCLUDE_TICKERS_period}", stacklevel=2)

            if len(INCLUDE_TICKERS_sec_standard) > 0:
                INCLUDE_TICKERS = INCLUDE_TICKERS_sec_standard
                company_tickers['include'] = company_tickers.apply(lambda row: 1 if row['ticker'] in INCLUDE_TICKERS else 0, axis=1)
                INCLUDE_CIKS = ((company_tickers[(company_tickers['include'] == 1)])['cik_str']).to_list()
except Exception as e:
    pass

EXCLUDE_TICKERS = []
EXCLUDE_CIKS = []
try:
    EXCLUDE_TICKERS = settings.EXCLUDE_TICKERS
    if type(EXCLUDE_TICKERS) is not list:
        warnings.warn("The specified EXCLUDE_TICKERS is not a list, excluding none of the tickers ...", stacklevel=2)
    else:
        if len(EXCLUDE_TICKERS) > 0:
            EXCLUDE_TICKERS_sec_standard = [(x.replace(".", "-")).upper() for x in EXCLUDE_TICKERS if x != ""]
            EXCLUDE_TICKERS_period = [x for x in EXCLUDE_TICKERS_sec_standard if x not in EXCLUDE_TICKERS]
            if len(EXCLUDE_TICKERS_period) > 0:
                warnings.warn(f"Share class converted to SEC standard using dashes for following tickers: {EXCLUDE_TICKERS_period}", stacklevel=2)

            if len(EXCLUDE_TICKERS_sec_standard) > 0:
                EXCLUDE_TICKERS = EXCLUDE_TICKERS_sec_standard
                company_tickers['exclude'] = company_tickers.apply(lambda row: 1 if row['ticker'] in EXCLUDE_TICKERS else 0, axis=1)
                EXCLUDE_CIKS = ((company_tickers[(company_tickers['exclude'] == 1)])['cik_str']).to_list()
except Exception as e:
    pass


data_cids_index['keep_status'] = data_cids_index.apply(lambda row: get_keep_status(row, time_machine_folder, data_cids_index_local_list, INCLUDE_UNITS, EXCLUDE_UNITS, INCLUDE_METRICS, EXCLUDE_METRICS, INCLUDE_CIKS, EXCLUDE_CIKS), axis=1)
data_cids_index = data_cids_index[(data_cids_index['keep_status'] == 1)]
data_cids_index.drop('keep_status', axis=1, inplace=True)

PARA_PROCS_default = 4
PARA_PROCS_use = PARA_PROCS_default
try:
    PARA_PROCS = settings.PARA_PROCS
    try:
        PARA_PROCS = int(PARA_PROCS)
        if PARA_PROCS <= 0:
            warnings.warn(f"The specified PARA_PROCS can not be converted to int type, using default value of {PARA_PROCS_default}", stacklevel=2)
        else:
            PARA_PROCS_use = PARA_PROCS
    except Exception as e:
        print(e)
        warnings.warn(f"The specified PARA_PROCS can not be converted to int type, using default value of {PARA_PROCS_default}", stacklevel=2)

except Exception as e:
    pass
    #print(e)
    #warnings.warn(f"Something went wrong getting PARA_PROCS from settings, using default value of {PARA_PROCS_default}", stacklevel=2)

data_cids_index_len = len(data_cids_index)
if data_cids_index_len > 0:
    PARA_PROCS_use_min = min(PARA_PROCS_use, data_cids_index_len)
    if data_cids_index_len < PARA_PROCS_use:
        warnings.warn(f"The length of index: {data_cids_index_len} is < than the PARA_PROCS specified: {PARA_PROCS_use}, the value of {PARA_PROCS_use_min} will be used", stacklevel=2)
    else:
        if PARA_PROCS_use_min >= 24:
            PARA_PROCS_use_min = PARA_PROCS_default

        index_splits = np.array_split(data_cids_index, min(data_cids_index_len, PARA_PROCS_use_min))
        index_split_counter = 0

        for index_split in index_splits:
            index_split_counter += 1
            index_split_counter_pad = (str(index_split_counter).zfill(4))

            index_split_filename = index_split_counter_pad + ".feather"
            index_split_filelocation = os_path_join(data_cids_index_get_parafolder, index_split_filename)
            index_split.reset_index(drop=True, inplace=True)
            index_split.to_feather(index_split_filelocation)
        
else:
    warnings.warn(f"The number of needed files is <= 0", stacklevel=2)

data_cids_index_local_parafolder = os_path_join(temp_files_folder, "data_cids_index_local")
if not exists(data_cids_index_local_parafolder):
    os.makedirs(data_cids_index_local_parafolder)
else:
    shutil.rmtree(data_cids_index_local_parafolder)
    os.makedirs(data_cids_index_local_parafolder)

data_cids_index_len_new = len(data_cids_index)
data_cids_index_len_dict = {"total": data_cids_index_len_total, "new": data_cids_index_len_new}
data_cids_index_len_filelocation = os_path_join(data_cids_index_get_parafolder, "data_cids_index_len.pkl")
with open(data_cids_index_len_filelocation, 'wb') as file:
    pickle.dump(data_cids_index_len_dict, file)

print((mssg + str(datetime.datetime.today())).replace("started", "ended").ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")
print("#" * print_out_len)

