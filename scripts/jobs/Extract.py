from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import csv
import json
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import requests
import pycountry
from bs4 import BeautifulSoup

gecko_path = "C:/Users/nour.khettache/PycharmProjects/scraping/venv/scripts/geckodriver.exe"
chrome_path= "C:/Users/nour.khettache/PycharmProjects/scraping/venv/scripts/chromedriver.exe"

# Get Firefox driver
"""
options = webdriver.firefox.options.Options()
options.headless = False
driver = webdriver.Firefox(options = options, executable_path = gecko_path)
"""
"""
# Get Chrome driver
options = Options()
options.add_argument("start-maximized")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
driver.get("https://www.google.com")
"""
# Get user agent
user_agent = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:55.0) Gecko/20100101 Firefox/55.0',
}

# Get url
url = 'https://www.knowyourcountry.com/fatf-aml-list/'

response = requests.get(url,headers=user_agent)

soup = BeautifulSoup(response.text, 'lxml')
jobs = soup.find_all('div',class_ = 'elementor-widget-container').find_all('p')
print(type(jobs))
for job in jobs:
    paragraphe = jobs.find_all('p').text



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