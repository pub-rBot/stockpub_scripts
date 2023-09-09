# Getting Started

## Dependencies

Uncomment the Python dependencies in requirements.txt

requirements.txt should look like below:

```text
requests==2.31.0
pandas==2.0.3
pyarrow==13.0.0
numpy==1.25.2

# viz_demo requirements
altair==5.0.1
vl-convert-python==0.12.0
Pillow==10.0.0
```

Open a terminal window, navigate to the requirements.txt directory and run the following:

```commandline
pip install -r requirements.txt
```


## Make a Time-lapse Gif

After installing all the packages, in the terminal window, navigate to the viz_demo directory:

Your terminal window should be pointing to a directory similar to: .../sec_companyfacts/viz_demo

Run the following command:

```commandline
python exec.py
```

After the command is finished, you should see something like the following in your terminal window:

```text
Gif saved to:
 .../stockpub_data_derived/viz_demo/outputs/time_lapse.gif
```

## Scripts Order

The scripts are run in the following order:

```text
exec.py
    xform_data.py
    plot_data.py
    make_gif.py    
```

exec.py script initializes the other scripts

## Exploring the Scripts
Each script is annotated if you wish to explore them. The annotations assume you will be reading the scripts in the order in which they are executed.
