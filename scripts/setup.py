from setuptools import setup, find_packages

with open('VERSION.txt') as f:
    VERSION = f.read()

with open('requirements.txt', 'r', encoding='ascii') as f:
    REQUIREMENTS = f.read()

#In order to install all local modules, use pip install -e . in the same directory as setup.py

setup(
    name='scraping_test_technic',
    version=VERSION,
    install_requires=REQUIREMENTS,
    packages=find_packages()
)