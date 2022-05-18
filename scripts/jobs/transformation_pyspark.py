import time
import csv
import json
import pandas as pd
import requests
import pycountry
import datetime as dt
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
import sys
from pyspark.sql.types import StringType,StructType,StructField
from bs4 import BeautifulSoup
import logging
import re
import glob
import numpy as np

from scripts.utils.utils_extract import *
from scripts.utils.utils_file import *
from scripts.utils.utils_pyspark import *

def run_job(spark, config_path='', config_file=None, arguments=None):
    # Config logger
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    # Get config user
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(ROOT_DIR, '../conf' + os.path.sep + 'config_user.yaml')
    config_user = read_yaml_file(config_path)

    # Get file path of output
    output_path = config_user.get('transformations_path')
    output_path_path_file = create_file_path(output_path, config_user)

    all_files = glob.glob(output_path + os.path.sep + "*.parquet")

    data = read_parquet_spark(spark,output_path_path_file)
    show = data.show()

    list_value = ['Albania']
    if 'Albania' in list(data.filter(col('PAYS').contains('Albania')).toPandas()['PAYS']):
        print("true")

    if 'Albania' in list(data.select(data.PAYS).toPandas()['PAYS']):
        print("deux")
    show = data.filter(data.PAYS == 'Albania').rdd.flatMap(lambda x:x).collect()

    data_struct = StructType(
        [
            StructField("PAYS", StringType(), True),
            StructField("C_RISK", StringType(), True),
            StructField("C_DATE", StringType(), True),
            StructField("C_SOURCE", StringType(), True),
            StructField("DT_SOURCE", StringType(), True),

        ]
    )


    today = dt.datetime.today().strftime('"%Y-%m-%d"')
    columns = ['PAYS', 'C_RISK', 'C_DATE', 'C_SOURCE', 'DT_SOURCE']
    value = [('PAYS', 'C_RISK', today, 'Source_user', today)]
    new_data = spark.createDataFrame(data=value, schema=data_struct)
    new_data.show()
    # add the newly provided values
    #new_df = data.union(new_data).show()




if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("SparkSessionEX") \
        .config("spark.debug.maxToStringFields", "50") \
        .getOrCreate()
    try:
        config_file = sys.argv[1]
    except:
        config_file = 'conf/conf_user.yaml'

    run_job(spark, config_path='', config_file=config_file, arguments=None)
