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

src_module_name = "settings"
src_module_path = os_path_join(script_p1_path, "settings.py")
spec = importlib.util.spec_from_file_location(src_module_name, src_module_path)
settings = importlib.util.module_from_spec(spec)
sys.modules[src_module_name] = settings
spec.loader.exec_module(settings)

support_slice = pd.DataFrame()
try:
    SUPPORT_FILES = settings.SUPPORT_FILES
    if type(SUPPORT_FILES) is list:
        SUPPORT_FILES = [str(x).strip() for x in SUPPORT_FILES if x != ""]
        if len(SUPPORT_FILES) > 0:
            try:
                sp_index_cids_support_slice = sp_index_cids[(sp_index_cids['idx_type'] == "support")].copy()
                sp_index_cids_support_slice['support_status'] = sp_index_cids_support_slice.apply(lambda row: 1 if row['idx_id'] in SUPPORT_FILES else 0, axis=1)
                support_slice_idx_id_list = sp_index_cids_support_slice['idx_id'].to_list()

                SUPPORT_FILES_not_found = [x for x in SUPPORT_FILES if x not in support_slice_idx_id_list]

                if len(SUPPORT_FILES_not_found) > 0:
                    warnings.warn("Some values provided in SUPPORT_FILES are not found in index: " + str(SUPPORT_FILES_not_found) + " skipping them ...", stacklevel=2)

                support_slice = sp_index_cids_support_slice[sp_index_cids_support_slice['support_status'] == 1]
                support_slice_rec = support_slice.to_dict('records')
                for support_slice_rec_row in support_slice_rec:
                    support_cid = support_slice_rec_row['CID']
                    support_filename = (support_slice_rec_row['idx_id']) + ".feather"
                    _, sp_index_cids_local = get_ipfs_file(support_cid, support_folder, support_filename, sp_index_cids_local)

            except Exception as e:
                print(e)
                warnings.warn(f"Something went wrong trying to get CIDs for supporting files: {e} skipping getting support files ...", stacklevel=2)

    else:
        warnings.warn("The specified SUPPORT_FILES is not a list, skipping getting support files ...", stacklevel=2)

except Exception as e:
    warnings.warn("SUPPORT_FILES is not found in settings.py, we suggest adding SUPPORT_FILES = ['metrics_nld'] to get name label and description of the metrics.", stacklevel=2)
    pass

sp_index_cids_local.reset_index(drop=True, inplace=True)
sp_index_cids_local.to_feather(sp_index_cids_local_fileloc)

print((mssg + str(datetime.datetime.today())).replace("started", "ended").ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")
print("#" * print_out_len)
