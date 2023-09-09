import pandas as pd
from os.path import exists
from os.path import join as os_path_join
import os
import subprocess
import sys
from pathlib import Path
import datetime
import re
import shutil
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
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
    # subprocess.Popen(get_cmd, shell=True, executable='/bin/bash', stdout=subprocess.PIPE)
    subprocess.Popen(get_cmd, shell=True)
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
data_cids_index_local_fileloc = os_path_join(temp_files_folder, "data_cids_index_local.feather")

data_cids_index_local_frames = []

if exists(data_cids_index_local_fileloc):
    data_cids_index_local = pd.read_feather(data_cids_index_local_fileloc)
    data_cids_index_local_frames.append(data_cids_index_local)

if exists(data_cids_index_local_parafolder):

    parafolder_files = os.listdir(data_cids_index_local_parafolder)
    parafolder_files = [x for x in parafolder_files if x.endswith(".feather")]

    for parafolder_file in parafolder_files:
        parafolder_fileloc = os_path_join(data_cids_index_local_parafolder, parafolder_file)

        parafolder_index = pd.read_feather(parafolder_fileloc)
        data_cids_index_local_frames.append(parafolder_index)

if len(data_cids_index_local_frames) > 0:
    data_cids_index_local = pd.concat(data_cids_index_local_frames)
    data_cids_index_local.drop_duplicates(subset=['key'], keep="last", inplace=True)
    data_cids_index_local.reset_index(drop=True, inplace=True)
    data_cids_index_local.to_feather(data_cids_index_local_fileloc)

if exists(data_cids_index_get_parafolder):
    shutil.rmtree(data_cids_index_get_parafolder)

if exists(data_cids_index_local_parafolder):
    shutil.rmtree(data_cids_index_local_parafolder)

print((mssg + str(datetime.datetime.today())).replace("started", "ended").ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")
print("#" * print_out_len)
