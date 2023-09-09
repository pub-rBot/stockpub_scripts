import pandas as pd
from os.path import exists
from os.path import join as os_path_join
import os
import subprocess
import sys
from pathlib import Path
import datetime
import re

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

pathlib_path = Path(script_path)

script_p2_path_pathlib = pathlib_path.parent.parent.parent.resolve()
script_p2_name = script_p2_path_pathlib.name

script_p4_path_pathlib = pathlib_path.parent.parent.parent.parent.parent.resolve()
script_p4_path = str(script_p4_path_pathlib)

data_root_folder = os_path_join(script_p4_path, "stockpub_data")
dataset_folder = os_path_join(data_root_folder, script_p2_name)
data_folder = os_path_join(dataset_folder, "data")
support_folder = os_path_join(data_folder, "support")
temp_files_folder = os_path_join(support_folder, "temp_files")
time_machine_folder = os_path_join(data_folder, "time_machine")
by_units_folder = os_path_join(time_machine_folder, "by_units")

pinned_index_fileloc = os_path_join(temp_files_folder, "data_cids_index_local.feather")
pinned_index = pd.DataFrame()
pinned_index_key_list = []
pinned_cid_list = []
if exists(pinned_index_fileloc):
    pinned_index = pd.read_feather(pinned_index_fileloc)

    check_ipfs_pinned_cmd = ['ipfs', 'pin', "ls", "--type=all"]
    check_ipfs_pinned_cmd_output = (subprocess.Popen(check_ipfs_pinned_cmd, stdout=subprocess.PIPE).communicate()[0]).decode()
    pinned_cid_list = check_ipfs_pinned_cmd_output.splitlines()
    pinned_cid_list = [(x.split()[0]).strip() for x in pinned_cid_list]

    pinned_index['pinned_status'] = pinned_index.apply(lambda row: 1 if row['CID'] in pinned_cid_list else 0, axis=1)
    pinned_index = pinned_index[(pinned_index['pinned_status'] == 1)]
    pinned_index.drop('pinned_status', axis=1, inplace=True)
    pinned_index.reset_index(drop=True, inplace=True)

    pinned_index.to_feather(pinned_index_fileloc)
    pinned_index_key_list = pinned_index['key'].to_list()

if exists(by_units_folder):
    by_units_subfolders = list(os.listdir(by_units_folder))
    by_units_subfolders.sort()
else:
    sys.exit(by_units_folder + " does not exist, exiting ...")


pinned_new_redun_list = []
for by_units_subfolder in by_units_subfolders:
    by_metrics_folder = os_path_join(by_units_folder, by_units_subfolder, "by_metrics")

    if exists(by_metrics_folder):
        by_metrics_subfolders = list(os.listdir(by_metrics_folder))
        by_metrics_subfolders.sort()
    else:
        continue

    by_metrics_subfolders_len = len(by_metrics_subfolders)
    by_metrics_subfolder_loop = 0
    for by_metrics_subfolder in by_metrics_subfolders:
        by_metrics_subfolder_loop += 1

        by_ciks_folder = os_path_join(by_metrics_folder, by_metrics_subfolder, "by_ciks")
        if exists(by_ciks_folder):
            by_ciks_subfolders = list(os.listdir(by_ciks_folder))
            by_ciks_list = [x.replace("CIK", "") for x in by_ciks_subfolders]
            by_ciks_list.sort()
        else:
            continue

        ciks_list_len = len(by_ciks_list)
        cik_str_loop = 0

        for cik_str in by_ciks_list:
            cik_str_loop += 1
            by_ciks_subfolder = "CIK" + cik_str
            cik_folder = os_path_join(by_ciks_folder, by_ciks_subfolder)
            if exists(cik_folder):
                cik_files = list(os.listdir(cik_folder))
                cik_files.sort()
            else:
                continue

            cik_files_len = len(cik_files)
            cik_file_loop = 0
            for cik_file in cik_files:
                cik_file_loop += 1
                cik_filedate = cik_file.replace(".feather", "")

                key = "#".join([by_units_subfolder, by_metrics_subfolder, cik_str, cik_filedate])
                if key in pinned_index_key_list:
                    continue

                to_pin_fileloc = os_path_join(cik_folder, cik_file)
                cmd = ['ipfs', 'add', to_pin_fileloc]

                cmd_output = (subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]).decode()
                if ("added" in cmd_output) and (cik_file in cmd_output):
                    regex = 'added(.*)' + cik_file
                    result = re.search(regex, cmd_output)

                    if result:
                        CID = result.group(1).strip()
                        pinned_new_redun_dict = {}
                        pinned_new_redun_dict['unit'] = by_units_subfolder
                        pinned_new_redun_dict['metric'] = by_metrics_subfolder
                        pinned_new_redun_dict['cik_str'] = cik_str
                        pinned_new_redun_dict['date'] = cik_filedate
                        pinned_new_redun_dict['key'] = key
                        pinned_new_redun_dict['CID'] = CID
                        pinned_new_redun_list.append(pinned_new_redun_dict)


if len(pinned_new_redun_list) > 0:
    pinned_new_redun_index = pd.DataFrame(pinned_new_redun_list)
    pinned_index = pd.concat([pinned_index, pinned_new_redun_index])
    pinned_index.drop_duplicates(inplace=True)
    pinned_index.reset_index(drop=True, inplace=True)
    pinned_index.to_feather(pinned_index_fileloc)

print((mssg + str(datetime.datetime.today())).replace("started", "ended").ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")
print("#" * print_out_len)
