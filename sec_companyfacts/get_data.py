import os
import subprocess
from pathlib import Path
from os.path import join as os_path_join
import sys
import datetime
import time
script_s = int(time.time())
secs_day = 24 * 60 * 60
script_path = os.path.abspath(os.path.realpath(__file__))
mssg = script_path + " has started at: "
print_out_len = 128
print_out_len_m1 = 128 - 1
print("-" * print_out_len)
print((mssg + str(datetime.datetime.today())).ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")

get_ipfs_files_str = "get_ipfs_files"
pathlib_path = Path(script_path)
script_stem = str(pathlib_path.stem)
script_parent_path_pathlib = pathlib_path.parent.resolve()
script_parent_path = str(script_parent_path_pathlib)

get_data_folder = os_path_join(script_parent_path, script_stem)


if __name__ == '__main__':
    subprocess.run([sys.executable, os_path_join(get_data_folder, "get_http_files.py")])
    subprocess.run([sys.executable, os_path_join(get_data_folder, get_ipfs_files_str, get_ipfs_files_str + ".py")])

print((mssg + str(datetime.datetime.today())).replace("started", "ended").ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")
print("#" * print_out_len)
script_e = int(time.time())
script_d = script_e - script_s
elapsed_str = f"{int(script_d / secs_day)} Day(s) {time.strftime('%H:%M:%S', time.gmtime(script_d))}"
print(("GET DATA ELAPSED TIME: " + elapsed_str).ljust(print_out_len_m1)[-(print_out_len_m1):] + "#")
print("#" * print_out_len)
mssg_mi = "for more information, visit: ".title() + "https://stockpub.net/godata/"
print(mssg_mi.ljust(print_out_len_m1)[-(print_out_len_m1):] + "#")
print("#" * print_out_len)
