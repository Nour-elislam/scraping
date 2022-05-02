from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import csv
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

gecko_path = "C:/Users/nour.khettache/PycharmProjects/scraping/venv/scripts/geckodriver.exe"
chrome_path= "C:/Users/nour.khettache/PycharmProjects/scraping/venv/scripts/chromedriver.exe"

# Get Firefox driver
"""
options = webdriver.firefox.options.Options()
options.headless = False
driver = webdriver.Firefox(options = options, executable_path = gecko_path)
"""

# Get Chrome driver
options = Options()
options.add_argument("start-maximized")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
driver.get("https://www.google.com")


url = 'https://www.knowyourcountry.com/fatf-aml-list/'
driver.get(url)
wait = WebDriverWait(driver, 5)
driver.implicitly_wait(5)

filename = driver.find_element_by_tag_name('h5')
print(filename.text)

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