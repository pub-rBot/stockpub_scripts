import os
import multiprocessing
import subprocess
from pathlib import Path
from os.path import join as os_path_join
import sys
import datetime
import importlib.util
import warnings
import pickle
from os.path import exists

script_path = os.path.abspath(os.path.realpath(__file__))
mssg = script_path + " has started at: "
print_out_len = 128
print_out_len_m1 = 128 - 1
print("-" * print_out_len)
print((mssg + str(datetime.datetime.today())).ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")


python_cmd = 'python'

try:
    out = subprocess.check_output(["python3", "-V"])
    python_cmd = "python3"
except:
    out = subprocess.check_output(["python", "-V"])


def execute(process):
    os.system(f'{python_cmd} {process}')


pathlib_path = Path(script_path)
script_parent_path_pathlib = pathlib_path.parent.resolve()
script_parent_path = str(script_parent_path_pathlib)
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
data_cids_index_len_folder = os_path_join(temp_files_folder, "data_cids_index_get")

src_module_name = "settings"
src_module_path = os_path_join(script_p1_path, "settings.py")
spec = importlib.util.spec_from_file_location(src_module_name, src_module_path)
settings = importlib.util.module_from_spec(spec)
sys.modules[src_module_name] = settings
spec.loader.exec_module(settings)

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

REBUILD_INDEX = False
try:
    REBUILD_INDEX = settings.REBUILD_INDEX
except Exception as e:
    pass


if __name__ == '__main__':

    subprocess.run([sys.executable, os_path_join(script_parent_path, "get_ipfs_data_index_files.py")])
    subprocess.run([sys.executable, os_path_join(script_parent_path, "get_ipfs_support_files.py")])
    subprocess.run([sys.executable, os_path_join(script_parent_path, "get_ipfs_data_pi.py")])

    get_ipfs_data_files_py_path = os_path_join(script_parent_path, "get_ipfs_data_files.py")
    para_procs_tuple = tuple([" ".join([get_ipfs_data_files_py_path, str(x+1)]) for x in range(0, PARA_PROCS_use)])

    process_pool = multiprocessing.Pool(processes=len(para_procs_tuple))
    print("-" * print_out_len)
    mssg_02 = get_ipfs_data_files_py_path + " has started at: "
    print((mssg_02 + str(datetime.datetime.today())).ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")
    print("*" * print_out_len)
    print(('{:*^127}'.format('Data Root Path:')) + "|")
    print(('{:*^127}'.format(os_path_join(script_p4_path, "stockpub_data")[-(print_out_len_m1):])) + "|")
    print("*" * print_out_len)
    data_cids_index_len_filelocation = os_path_join(data_cids_index_len_folder, "data_cids_index_len.pkl")
    len_total = 0
    len_new = 0
    if exists(data_cids_index_len_filelocation):
        with open(data_cids_index_len_filelocation, 'rb') as file:
            data_cids_index_len_dict = pickle.load(file)
            len_total = data_cids_index_len_dict['total']
            len_new = data_cids_index_len_dict['new']
        os.remove(data_cids_index_len_filelocation)

    if (len_total > 0) and (len_new > 0):
        mssg_len = f'Data Instances Total: {len_total}'
        print(f'{mssg_len:*^127}' + "|")
        mssg_len = f'Data Instances New: {len_new}'
        print(f'{mssg_len:*^127}' + "|")
    print("*" * print_out_len)
    print(('{:*^127}'.format('Data Instance Details Link:')) + "|")
    print(('{:*^127}'.format('https://stockpub.net/pubmanual/SEC-Company-Facts-Dataset-Details/')) + "|")
    print("*" * print_out_len)
    process_pool.map(execute, para_procs_tuple)
    print((mssg_02 + str(datetime.datetime.today())).replace("started", "ended").ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")
    print("#" * print_out_len)

    if REBUILD_INDEX:
        subprocess.run([sys.executable, os_path_join(script_parent_path, "compile_data_cids_index_local.py")])

print((mssg + str(datetime.datetime.today())).replace("started", "ended").ljust(print_out_len_m1)[-(print_out_len_m1):] + "|")
print("#" * print_out_len)
