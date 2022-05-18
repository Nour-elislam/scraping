import pyspark
import logging


def read_parquet_spark(spark, file_path, logger=None):
    """
    Function to read a parquet file to create a Spark Dataframe

    :param file_path: file path to read
    :type file_path: str
    :param logger: logger to use in the function
    :type logger: Logger
    :return: Spark Dataframe
    """
    f_name = 'read_parquet_spark'

    try:
        logging.log(10 ,'Function read_parquet_spark  started')

        # Read parquet with spark
        df = spark.read.format('parquet').load(file_path)

        logging.log(20, f'{file_path} successfully read')
        return df
    except Exception as err:
        logging.log(40, f'{file_path} cannot be read')



