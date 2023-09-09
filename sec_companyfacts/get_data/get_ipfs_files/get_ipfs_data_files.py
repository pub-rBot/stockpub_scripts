import pandas as pd
from os.path import exists
from os.path import join as os_path_join
import os
import subprocess
import sys
from pathlib import Path

import re

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
script_path = os.path.abspath(os.path.realpath(__file__))

check_ipfs_online_cmd = ['ipfs', 'swarm', "peers"]
check_ipfs_online_cmd_output = (subprocess.Popen(check_ipfs_online_cmd, stdout=subprocess.PIPE).communicate()[0]).decode()

if check_ipfs_online_cmd_output == "":
    sys.exit("Start IPFS in terminal with 'ipfs daemon'")

if len(sys.argv) > 1:
    index_num = int(sys.argv[1])
else:
    index_num = 1

index_num_str = str(index_num).zfill(4)


def get_ipfs_file(df_row, tm_folder_loc):
    success = False
    row_unit = df_row['unit']
    row_metric = df_row['metric']
    row_cik_str = df_row['cik_str']
    row_date = df_row['date']
    row_CID = df_row['CID']
    filename = row_date + ".feather"
    local_folder_loc = os_path_join(tm_folder_loc, "by_units", row_unit, "by_metrics", row_metric, "by_ciks", ("CIK" + row_cik_str))
    if not exists(local_folder_loc):
        try:
            os.makedirs(local_folder_loc)
        except:
            pass
    if not exists(local_folder_loc):
        try:
            os.makedirs(local_folder_loc)
        except:
            pass

    local_file_loc = os_path_join(local_folder_loc, filename)

    get_cmd = f'ipfs cat /ipfs/{row_CID} > "{local_file_loc}"'
    #subprocess.Popen(get_cmd, shell=True, executable='/bin/bash', stdout=subprocess.PIPE)
    subprocess.Popen(get_cmd, shell=True)
    if exists(local_file_loc):
        pin_cmd = ['ipfs', 'add', local_file_loc]
        cmd_output = (subprocess.Popen(pin_cmd, stdout=subprocess.PIPE).communicate()[0]).decode()
        if ("added" in cmd_output) and (filename in cmd_output):
            regex = 'added(.*)' + filename
            result = re.search(regex, cmd_output)
            if result:
                new_cid = result.group(1).strip()
                df_row['CID'] = new_cid
                success = True
    if success:
        return df_row
    else:
        return None


pathlib_path = Path(script_path)

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
data_cids_index_get_parafolder = os_path_join(temp_files_folder, "data_cids_index_get")

data_cids_index_local_parafolder = os_path_join(temp_files_folder, "data_cids_index_local")

para_index_cids_filename = (index_num_str + ".feather")
para_index_fileloc = os_path_join(data_cids_index_get_parafolder, para_index_cids_filename)
if exists(para_index_fileloc):
    try:
        para_index = pd.read_feather(para_index_fileloc)
    except Exception as e:
        print(e)
        sys.exit("Something went wrong reading index file number: " + index_num_str + ", exiting ...")

    if para_index.empty:
        sys.exit("Index file number: " + index_num_str + " is empty, exiting ...")
else:
    sys.exit(para_index_fileloc + " does not exist, exiting ...")

para_index_rec = para_index.to_dict('records')
para_index_cids_list = []
for para_index_rec_row in para_index_rec:
    para_index_rec_row = get_ipfs_file(para_index_rec_row, time_machine_folder)
    if para_index_rec_row is not None:
        para_index_cids_list.append(para_index_rec_row)

if len(para_index_cids_list) > 0:
    para_index_cids = pd.DataFrame(para_index_cids_list)
    para_index_cids_fileloc = os_path_join(data_cids_index_local_parafolder, para_index_cids_filename)
    para_index_cids.to_feather(para_index_cids_fileloc)

os.remove(para_index_fileloc)

