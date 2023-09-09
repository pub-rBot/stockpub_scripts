import sys
import subprocess
import os
from pathlib import Path
from os.path import join as os_path_join

script_path = os.path.abspath(os.path.realpath(__file__))
pathlib_path = Path(script_path)
script_parent_path = str(pathlib_path.parent.resolve())

sys_exec_path = os_path_join(script_parent_path, "xform_data.py")
subprocess.run([sys.executable, sys_exec_path])

sys_exec_path = os_path_join(script_parent_path, "plot_data.py")
subprocess.run([sys.executable, sys_exec_path])

sys_exec_path = os_path_join(script_parent_path, "make_gif.py")
subprocess.run([sys.executable, sys_exec_path])
