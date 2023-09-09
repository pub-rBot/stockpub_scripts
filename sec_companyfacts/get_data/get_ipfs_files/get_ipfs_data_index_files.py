import pandas as pd
from os.path import exists
from os.path import join as os_path_join
import os
import importlib.util
import subprocess
import sys
from pathlib import Path
import datetime
import re
import warnings
script_path = os.path.abspath(os.path.realpath(__file__))
mssg = script_path + " has started at: "
print_out_len = 128
print_out_len_m1 = 128 - 1
print("-" * print_out_len)
print((mssg + str(datetime.datetime.today())).ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")

check_ipfs_online_cmd = ['ipfs', 'swarm', "peers"]
check_ipfs_online_cmd_output = (subprocess.Popen(check_ipfs_online_cmd, stdout=subprocess.PIPE).communicate()[0]).decode()

if check_ipfs_online_cmd_output == "":
    sys.exit("Start IPFS in terminal with 'ipfs daemon'")


def get_ipfs_file(cid, folder, filename, local_cids_index_df):
    local_loc = os_path_join(folder, filename)
    get_ipfs = True
    if exists(local_loc):
        if not local_cids_index_df.empty:
            local_cids_index_df_slice = local_cids_index_df[(local_cids_index_df['folder'] == folder) & (local_cids_index_df['filename'] == filename)]
            if len(local_cids_index_df_slice) > 0:
                local_cid = local_cids_index_df_slice['CID'].iloc[0]
                if local_cid == cid:
                    get_ipfs = False

    if get_ipfs:
        get_cmd = f'ipfs cat /ipfs/{cid} > "{local_loc}"'
        # subprocess.Popen(get_cmd, shell=True, executable='/bin/bash', stdout=subprocess.PIPE)
        subprocess.Popen(get_cmd, shell=True)
        pin_cmd = ['ipfs', 'add', local_loc]
        cmd_output = (subprocess.Popen(pin_cmd, stdout=subprocess.PIPE).communicate()[0]).decode()
        if ("added" in cmd_output) and (filename in cmd_output):
            regex = 'added(.*)' + filename
            result = re.search(regex, cmd_output)
            if result:
                new_cid = result.group(1).strip()
                new_cids_list = []
                new_cids_dict = {}
                new_cids_dict['folder'] = folder
                new_cids_dict['filename'] = filename
                new_cids_dict['CID'] = new_cid
                new_cids_list.append(new_cids_dict)
                new_cids_df = pd.DataFrame(new_cids_list)
                local_cids_index_df = pd.concat([local_cids_index_df, new_cids_df])
                local_cids_index_df.drop_duplicates(subset=['folder', 'filename'], keep="last", inplace=True)

    return local_loc, local_cids_index_df


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
support_folder = os_path_join(data_folder, "support")

temp_files_folder = os_path_join(support_folder, "temp_files")

sp_index_cids_fileloc = os_path_join(temp_files_folder, "sp_index_cids.feather")
sp_index_cids_local_fileloc = os_path_join(temp_files_folder, "sp_index_cids_local.feather")
sp_index_cids_local = pd.DataFrame()
if exists(sp_index_cids_local_fileloc):
    sp_index_cids_local = pd.read_feather(sp_index_cids_local_fileloc)

company_tickers_fileloc = os_path_join(support_folder, "company_tickers.feather")
company_tickers_exchange_fileloc = os_path_join(support_folder, "company_tickers_exchange.feather")
if not exists(sp_index_cids_fileloc):
    sys.exit("Please run get_http_files.py first to get central indexes")
else:
    try:
        sp_index_cids = pd.read_feather(sp_index_cids_fileloc)
    except Exception as e:
        print(e)
        sys.exit("Something went wrong trying to read stockPub central index file, exiting ...")

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

DATA_SIZE_default = str(20)
try:
    DATA_SIZE = settings.DATA_SIZE
    DATA_SIZE = str(DATA_SIZE)
    try:
        data_slice = sp_index_cids[(sp_index_cids['idx_type'] == "data") & (sp_index_cids['idx_id'] == DATA_SIZE)]
        if len(data_slice) == 0:
            data_slice = sp_index_cids[(sp_index_cids['idx_type'] == "data") & (sp_index_cids['idx_id'] == DATA_SIZE_default)]
            warnings.warn(f"The specified DATA_SIZE can not be found in index, using default value of {DATA_SIZE_default}", stacklevel=2)

        data_cids_index_cid = data_slice['CID'].iloc[0]
        data_cids_index_filename = "".join(["data_cids_index_", DATA_SIZE, ".feather"])
        _, sp_index_cids_local = get_ipfs_file(data_cids_index_cid, support_folder, data_cids_index_filename, sp_index_cids_local)

    except Exception as e:
        data_slice = sp_index_cids[(sp_index_cids['idx_type'] == "data") & (sp_index_cids['idx_id'] == DATA_SIZE_default)]
        data_cids_index_cid = data_slice['CID'].iloc[0]
        data_cids_index_filename = "".join(["data_cids_index_", DATA_SIZE_default, ".feather"])
        _, sp_index_cids_local = get_ipfs_file(data_cids_index_cid, support_folder, data_cids_index_filename, sp_index_cids_local)
        print(e)
        warnings.warn(f"Something went wrong trying to get stockPub central index file slice, using default value of {DATA_SIZE_default}", stacklevel=2)
except Exception as e:
    data_slice = sp_index_cids[(sp_index_cids['idx_type'] == "data") & (sp_index_cids['idx_id'] == DATA_SIZE_default)]
    data_cids_index_cid = data_slice['CID'].iloc[0]
    data_cids_index_filename = "".join(["data_cids_index_", DATA_SIZE_default, ".feather"])
    _, sp_index_cids_local = get_ipfs_file(data_cids_index_cid, support_folder, data_cids_index_filename, sp_index_cids_local)
    print(e)
    warnings.warn(f"Something went wrong getting DATA_SIZE from settings, using default value of {DATA_SIZE_default}", stacklevel=2)

sp_index_cids_local.reset_index(drop=True, inplace=True)
sp_index_cids_local.to_feather(sp_index_cids_local_fileloc)

print((mssg + str(datetime.datetime.today())).replace("started", "ended").ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")
print("#" * print_out_len)

