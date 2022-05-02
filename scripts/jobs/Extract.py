import time
import csv
import json
import pandas as pd
import requests
import pycountry
from bs4 import BeautifulSoup

from scripts.utils.utils_extract import *
# Get user agent
user_agent = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:55.0) Gecko/20100101 Firefox/55.0',
}

# Get url
url = 'https://www.knowyourcountry.com/fatf-aml-list/'

response = requests.get(url,headers=user_agent)

soup = BeautifulSoup(response.text, 'lxml')
jobs = soup.find_all('div',class_ = 'elementor-widget-container')
list_of_countries = []
for job in jobs:
    for p in job:
        list_of_countries.append(job.find_all('p'))
print(list_of_countries)

# Get name and date of data source
source_date = str(list_of_countries[0][1])
date_source, name_source = get_source_date(source_date)
print("date",date_source)

# Get black list
country_black_list = get_country_list(list_of_countries[0][4])
country_gris_list = get_country_list(list_of_countries[0][8])

print(country_gris_list)
print(country_black_list)




# Create data frame
#df = pd.DataFrame(columns=['main_source','url_source','pays','date','risk'])


# This function creates a list of countries after scrapping country names
# Also, it saves a list of countries in .csv file
# We will need .csv file with the country names for the second stage.
""""
list_of_countries = []
for country in countries:
    list_of_countries.append(country.text)
    f = open('1Countries.csv','w', newline='')
    with f:
        writer = csv.writer(f)
        writer.writerow(list_of_countries)
"""
# Closing web browser
time.sleep(2)