import glob
import contextlib
from PIL import Image
from os.path import join as os_path_join
import os
from pathlib import Path
import sys


def convert(src, dst):
    with Image.open(src) as img:
        print(f'file opened = {src}')
        if hasattr(img, 'is_animated') and img.is_animated:
            print(f'frames = {img.n_frames}')
            img.save(dst, quality=90, save_all=True)
        else:
            print('not animated')
            img.save(dst, quality=90)


script_path = os.path.abspath(os.path.realpath(__file__))
pathlib_path = Path(script_path)   # path lib object of current script's path, make it easier to transverse the path tree
script_p1_name = str(pathlib_path.parent.parent.resolve().name)
script_p3_path = str(pathlib_path.parent.parent.parent.parent.resolve())

derived_data_root_folder = os_path_join(script_p3_path, "stockpub_data_derived")
script_parent_name = str(pathlib_path.parent.resolve().name)
viz_demo_folder = os_path_join(derived_data_root_folder, script_parent_name)
viz_data_folder = os_path_join(viz_demo_folder, "data")

viz_plots_folder = os_path_join(viz_demo_folder, "plots")
outputs_folder = os_path_join(viz_demo_folder, "outputs")

viz_plots_files = os.listdir(viz_plots_folder)
viz_plots_files = [x for x in viz_plots_files if x.endswith(".png")]

if len(viz_plots_files) == 0:
    sys.exit("There are no images to make a gif from, exiting ... ")

# File paths for the inputs and output
pngs_in = os_path_join(viz_plots_folder, "*.png")
gif_out = os_path_join(outputs_folder, "time_lapse.gif")

# use exit stack to automatically close opened images
with contextlib.ExitStack() as stack:

    # lazily load images
    imgs = (stack.enter_context(Image.open(f))
            for f in sorted(glob.glob(pngs_in)))

    # extract  first image from iterator
    img = next(imgs)
    # https://pillow.readthedocs.io/en/stable/handbook/image-file-formats.html#gif
    img.save(fp=gif_out,
             format='GIF',
             append_images=imgs,
             save_all=True,
             duration=100,
             )


# print out gif location to terminal
print(f"Gif saved to:\n {gif_out}")
