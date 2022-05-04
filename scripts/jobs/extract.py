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
input_path_file = create_file_path(input_path,config_user)



# Get user agent
user_agent = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:55.0) Gecko/20100101 Firefox/55.0',
}

# Get url
url = 'https://www.knowyourcountry.com/fatf-aml-list/'

# Scraping of web page
response = requests.get(url, headers=user_agent)
soup = BeautifulSoup(response.text, 'lxml')
jobs = soup.find_all('div', class_='elementor-widget-container')
list_of_countries = []
for job in jobs:
    for p in job:
        list_of_countries.append(job.find_all('p'))

# Get name and date of data source
source_date = str(list_of_countries[0][1])
date_source, name_source = get_source_date(source_date)

logging.log(20, f' Scraping data {name_source} for : {date_source} from knowyourcountry')

# Get black list
country_black_list = get_country_list(list_of_countries[0][4])
logging.log(20, f' Successfully scraping country black list')

# Create parse blacklist in df
df_black_list = pd.DataFrame(country_black_list, columns=['PAYS'])
df_black_list['C_RISK'] = 'black list'

# Get grey list
country_greys_list = get_country_list(list_of_countries[0][8])
logging.log(20, f' Successfully scraping country grey list')

# Create parse grey list in df
df_grey_list = pd.DataFrame(country_greys_list, columns=['PAYS'])
df_grey_list['C_RISK'] = 'grey list'

# Get white list
country_white_list = get_country_list(list_of_countries[0][10])
logging.log(20, f' Successfully scraping country white list')

# Create parse white list in df
df_white_list = pd.DataFrame(country_white_list, columns=['PAYS'])
df_white_list['C_RISK'] = 'white list'

# concat df
df = pd.concat([df_grey_list, df_black_list,df_white_list],ignore_index=True)


# Normalise date, name
date_normalized = dt.datetime.strptime(str(date_source.replace(' ', '-')), "%B-%Y")
name_normalized = name_source.replace(" ", "-")

# Add date of classification in df
df['C_DATE'] = date_normalized.strftime("%Y-%m-%d")


# Create filename
dt_today = dt.date.today().strftime('%Y%m%d')
filename = str(name_normalized + '_' + dt_today)

# Create directory input
create_dir(input_path_file)

# write to csv
df.to_csv(path_or_buf=input_path_file + os.path.sep + filename + '.csv', sep=';', encoding='utf-8', index=False)

