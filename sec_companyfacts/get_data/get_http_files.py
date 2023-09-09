import os
import sys
import requests
import pandas as pd
import ast
from os.path import exists
from os.path import join as os_path_join
from pathlib import Path
import datetime
script_path = os.path.abspath(os.path.realpath(__file__))
mssg = script_path + " has started at: "
print_out_len = 128
print_out_len_m1 = 128 - 1
print("-" * print_out_len)
print((mssg + str(datetime.datetime.today())).ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")

pathlib_path = Path(script_path)
script_p1_path_pathlib = pathlib_path.parent.parent.resolve()
script_p1_name = script_p1_path_pathlib.name
script_p3_path_pathlib = pathlib_path.parent.parent.parent.parent.resolve()
script_p3_path = str(script_p3_path_pathlib)


data_root_folder = os_path_join(script_p3_path, "stockpub_data")
dataset_folder = os_path_join(data_root_folder, script_p1_name)
data_folder = os_path_join(dataset_folder, "data")
support_folder = os_path_join(data_folder, "support")
temp_files_folder = os_path_join(support_folder, "temp_files")

if not exists(temp_files_folder):
    os.makedirs(temp_files_folder)


urls_dict = {
    "stockpub": "http://127.0.0.1:8000/keychain",
    "sec": "https://www.sec.gov/files/company_tickers.json",
    "sec_exchange": "https://www.sec.gov/files/company_tickers_exchange.json"
}


for url_key, url in urls_dict.items():

    if url_key == "stockpub":
        try:
            r = requests.get(url)
            r_dec = r.content.decode()
            cid_dict = ast.literal_eval(r_dec)

            sec_companyfacts_dict = cid_dict['SEC']['companyfacts']
            sp_index_cids_list = []
            for idx_type, c_dict in sec_companyfacts_dict.items():
                for idx_id, cc_dict in c_dict.items():
                    CID = cc_dict['CID']

                    sp_index_cids_dict = {}
                    sp_index_cids_dict['idx_type'] = str(idx_type)
                    sp_index_cids_dict['idx_id'] = str(idx_id)
                    sp_index_cids_dict['CID'] = str(CID)
                    sp_index_cids_list.append(sp_index_cids_dict)

            if len(sp_index_cids_list) > 0:
                sp_index_cids = pd.DataFrame(sp_index_cids_list)
                sp_index_cids_fileloc = os_path_join(temp_files_folder, "sp_index_cids.feather")
                sp_index_cids.to_feather(sp_index_cids_fileloc)
            else:
                sys.exit("Something went wrong getting stockpub central index file, length of df should not be 0, exiting ...")
        except Exception as e:
            print(e)
            sys.exit("Something went wrong getting stockpub central index file, exiting ...")

    if url_key == "sec":
        try:
            json_get = requests.get(url)  # (your url)
            json_data = json_get.json()

            s = pd.Series(json_data)
            df = pd.DataFrame(s.values.tolist())
            df['cik_str'] = df.apply(lambda row: str(row['cik_str']).zfill(10), axis=1)

            df_file_name = (url.rsplit("/", 1)[-1]).rsplit(".", 1)[0]
            df_file_loc = os_path_join(support_folder, (df_file_name + ".feather"))
            df.drop_duplicates(inplace=True)
            df.reset_index(drop=True, inplace=True)
            df.to_feather(df_file_loc)
        except Exception as e:
            print(e)
            sys.exit("Something went wrong getting sec company ticker index file, exiting ...")

    if url_key == "sec_exchange":
        try:
            json_get = requests.get(url)  # (your url)
            json_data = json_get.json()

            s = pd.Series(json_data['data'])
            df = pd.DataFrame(s.values.tolist())
            df.columns = json_data['fields']

            df['cik'] = df.apply(lambda row: str(row['cik']).zfill(10), axis=1)
            df.rename(columns={'cik': 'cik_str'}, inplace=True)
            df_file_name = (url.rsplit("/", 1)[-1]).rsplit(".", 1)[0]
            df_file_loc = os_path_join(support_folder, (df_file_name + ".feather"))
            df.drop_duplicates(inplace=True)
            df.reset_index(drop=True, inplace=True)
            df.to_feather(df_file_loc)
        except Exception as e:
            print(e)
            sys.exit("Something went wrong getting sec company ticker exchange index file, exiting ...")

print((mssg + str(datetime.datetime.today())).replace("started", "ended").ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")
print("#" * print_out_len)
