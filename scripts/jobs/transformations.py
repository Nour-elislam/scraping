import time
import csv
import json
import pandas as pd
import requests
import pycountry
import datetime as dt
from bs4 import BeautifulSoup
import logging
import re
import glob
import numpy as np

from scripts.utils.utils_extract import *
from scripts.utils.utils_file import *

# Config logger
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

# Get config user
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(ROOT_DIR, '../conf' + os.path.sep + 'config_user.yaml')
config_user = read_yaml_file(config_path)

# Get file path of input
input_path = config_user.get('input_path')
input_path_file = create_file_path(input_path, config_user)

# Get file path of output
output_path = config_user.get('output_path')
output_path_path_file = create_file_path(output_path, config_user)

all_files = glob.glob(input_path_file + os.path.sep + "*.csv")

# loop for concat if others csv exist
for file in all_files:
    file_name = file.split('/')[-1].split('\\')[-1]

    separator = config_user['separator']
    delimitor = config_user['delimitor']
    encoding = config_user['encoding']
    header = 0
    names = file_name
    dtype_dict = {"str": "str"}
    na_values = config_user['na_values']

    df = read_csv_file(file, separator=config_user['separator'],
                       delimitor=config_user['delimitor'],
                       encoding=config_user['encoding'],
                       header=0,
                       names=None,
                       dtype_dict={"int": "int", "str": "str"},
                       na_values=config_user['na_values']
                       )

    # Get name and date of data source from filename
    date_source = file_name.split("_")[-1].split('.')[0]
    name_source = file_name.split("_")[0].split('.')[0]
    logging.log(20, f'Add new columns of source')

    # add columns source and date of source
    df['C_SOURCE'] = name_source
    df['DT_SOURCE'] = date_source

    df['ID'] = pd.RangeIndex(1, 1 + len(df))

    # reorder columns
    #df = df[["ID","PAYS","C_RISK","C_DATE","C_SOURCE","DT_SOURCE"]]

    # Create directory input
    create_dir(output_path_path_file)

    # write to csv
    df.to_csv(path_or_buf=output_path_path_file + os.path.sep + file_name, sep=';', encoding='utf-8', index=False)
    logging.log(20, f'Successfully write csv {output_path_path_file + os.path.sep + file_name} ')
