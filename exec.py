import os
import subprocess
from pathlib import Path
from os.path import join as os_path_join
import sys
import datetime

script_path = os.path.abspath(os.path.realpath(__file__))
mssg = script_path + " has started at: "
print_out_len = 128
print("-" * print_out_len)
print((mssg + str(datetime.datetime.today())).ljust(print_out_len - 1)[-(print_out_len - 1):] + "|")

script_parent_path_pathlib = Path(__file__).parent.resolve()
script_parent_path = str(script_parent_path_pathlib)


if __name__ == '__main__':
    subprocess.run([sys.executable, os_path_join(script_parent_path, "sec_companyfacts", "get_data.py")])

print((mssg + str(datetime.datetime.today())).replace("started", "ended").ljust(print_out_len - 1)[-(print_out_len - 1):] + "|")
print("#" * print_out_len)
