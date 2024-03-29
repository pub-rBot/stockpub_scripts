# What's Inside?
- Scripts to download preprocessed SEC company facts datasets from stockPub
  - Page link: <https://www.stockpub.net/godata/SEC-Company-Facts-Dataset>
- Visualization demo using the stockPub dataset

# Prerequisites

## Supported OS
- Linux Based Distro 
- Mac OS

Code tested using Ubuntu 22.04. 
You can find installation instructions here:

<https://ubuntu.com/download>

## Language
- Python 3

The code is written in Python 3, and it is assumed you have Python 3 running on your OS.
You can find installation instructions here:

<https://www.python.org/downloads/>

## File Sharing Protocol
If you have suggestion(s) on how we can sustainably release the dataset to the public using a better method, send us a message through our contact page. We appreciate your input(s).

<https://stockpub.net/contact>

## IPFS
IPFS is the protocol used to share the dataset.

### Step 1:
Install IPFS Kubo

<https://docs.ipfs.tech/install/command-line/>

### Step 2:
Open a terminal window and type in these commands:

Initialize IPFS repository:
```commandline
ipfs init
```

Start IPFS daemon:
```commandline
ipfs daemon
```

if everything works, you should see something similar to below:

```text
> Initializing daemon...
> API server listening on /ip4/127.0.0.1/tcp/5001
> Gateway server listening on /ip4/127.0.0.1/tcp/8080
```

For more detailed instruction on getting IPFS daemon running, you can follow the official instructions at the link below:

<https://docs.ipfs.tech/how-to/command-line-quick-start/>

***IMPORTANT: IPFS daemon need to be running to download the dataset over the IPFS network.***

# Repository

Clone or download this repository

* If you have git installed, open a terminal window and type in the following command:

```commandline
git clone git@github.com:pub-rBot/stockpub_scripts.git
```

* To download as a .zip file, find the <>Code button at the top on the repo page, then Download ZIP

After acquiring the source code, open a terminal window.

Navigate to your local source code folder, should be named stockpub_scripts.

Type in these commands:

```commandline
pip install -r requirements.txt
```

Depending on your Python set up, in your terminal window, type in:

```commandline
python exec.py
```

or

```commandline
python3 exec.py
```

By default, the dataset will be downloaded to stockpub_data folder. The folder will be in the same directory as stockpub_scripts.

Details about the dataset can be found at:
<https://stockpub.net/pubmanual/SEC-Company-Facts-Dataset-Details>

## Interacting With the Dataset

Dataset Storage Structure

```text
stockpub_data
└── sec_companyfacts
    ├── data
    │   └── time_machine
    │       └── by_units
    │           └── [unit_name]
    │               └── by_metrics
    │                   └── [metric_name]
    │                       └── by_ciks
    │                           └── CIK[0000000000]
    │                               └── YYYY-MM-DD.feather
    └── support
        └── support_file.feather
```

Each data instance is stored in feather format, the file can be read, analyzed and manipulated using pandas library.

## Dataset Download Settings

The python file controlling various parameters for getting the dataset can be found at sec_companyfacts -> get_data -> settings.py.

sec_companyfacts folder is located in the same directory as exec.py.

Details about each parameter is documented in the settings.py file.

## Visualization Demo

A time-lapse visualization demo is also included in the repository. The demo folder is located under sec_companyfacts folder. It contains many annotated scripts to help you get you started in analyzing and manipulating the dataset.

Details about how to run the demo is included in its README.md

# Helpful Notes

In the support folder, there are many helpful files, notably:

- metrics_nld: name, label and description of the metrics
- company_tickers: CIK to stock tickers index, _exchange version has exchange info

Cache mechanism

There is a built-in cache mechanism for files, to ensure the cache mechanism functions properly:

- Script and data folder are in the same directory

```text
root_dir
└── stockpub_data
└── stockpub_scripts
        └── exec.py
```

- Any derived files from the downloaded dataset should NOT be stored in stockpub_data folder and/or its subfolders

