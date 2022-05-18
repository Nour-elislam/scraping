import yaml
import logging
import os
from pandas import DataFrame, read_csv, read_parquet, DataFrame


def read_yaml_file(file_path, logger=None):
    """
    Open YAML file and return a dict type object

    :param file_path: This is path to YAML file
    :type file_path: str
    :param logger: Logger instance to use
    :type logger: Logger
    :return: Dict type object
    """
    f_name = 'read_yaml_file'
    logging.log(10, f'Function read_yaml_file() started')
    if not file_path:
        logging.log(20, f'File path is null')

    try:
        with open(file_path, 'r') as conf_file:

            dict_data = yaml.safe_load(conf_file)
            logging.log(10, "YAML read")
        logging.log(20, os.path.basename(file_path) + " successfully read ")
        return dict_data

    except TypeError as err:
        logging.log(40, f'Invalid file name type ({err})')
        raise err


def create_dir(path="scraping/input"):
    """
    Create a new directory
    :param path:
    :type path:
    :return:
    :rtype:
    """

    # Check whether the specified path exists or not
    isExist = os.path.exists(path)

    if not isExist:
        try:
            # Create a new directory because it does not exist
            os.makedirs(path)
            logging.log(20, f'The new directory is created! {path}')

        except Exception as e:
            logging.log(40, "Cannot create directory please check path in conf file")


def create_file_path(path, conf_user):
    """
    create path file according to os system
    :param conf_user:
    :type conf_user:
    :param path:
    :type path: str
    :return:
    :rtype: str
    """

    # Check os name
    if os.name == 'nt':
        prefix = conf_user.get('windows_prefix')
    else:
        prefix = conf_user.get('linux_prefix')

    return prefix + path


def read_csv_file(file_path, separator=',', delimitor=None, encoding=None, header=None, names=None, dtype_dict=None,
                  na_values=None, logger=None):
    """
    Open csv file and return panda dataframe

    :param na_values:
    :param file_path: path to csv file
    :type file_path: str
    :param separator: separator between fields
    :type separator: str
    :param delimitor: character to quote fields. This parameter is optional, if None do not set this parameter. The
    default value is None
    :type delimitor: str
    :param encoding: encode of the file. This parameter is optional, if None do not set this parameter. The default
    value is None. Max len is 1
    :type encoding: str
    :param header: number of the row which has the columns names. This parameter is optional, if None do not set this
    parameter. The default value is None
    :type header: int
    :param names: Names used to rename {old_name: new_name}
    :type names: Python dict
    :param dtype_dict: Dict with {name_of_column: type}
    :type dtype_dict: python Dict
    :param logger: Logger instance to use
    :type logger: Logger
    :return: panda dataframe with all datas of the csv file
    """
    f_name = 'read_csv_file'
    logging.log(10, "Function read_csv_file() started")

    # ------------------------------------------------------------------------------------------------------------------
    # Test optional parameters
    if not encoding:
        logging.log(30, "Encoding is empty, it will be set wih value utf-8")
        encoding = 'utf-8'

    if not header:
        logging.log(10, "Header is empty, it will be set at None")

    logging.log(10, "Optional parameters checked")

    logging.log(10,
                "Parameters for read_csv_file() : file_path=" + str(file_path) +
                ", separator=" + str(separator) + ", delimitor=" + str(delimitor) +
                ", encoding=" + str(encoding) + ", header=" + str(header))
    # ------------------------------------------------------------------------------------------------------------------

    try:

        if delimitor is not None and len(delimitor) == 1:
            # Read csv file and put result into dataframe
            dataframe = read_csv(file_path, sep=separator, quotechar=delimitor, doublequote=False, dtype=dtype_dict,
                                 header=header, names=names, encoding=encoding, index_col=None, na_values=na_values,
                                 keep_default_na=False)
        else:
            # Read csv file and put result into dataframe
            dataframe = read_csv(file_path, sep=separator, dtype=dtype_dict, header=header, names=names,
                                 encoding=encoding, na_values=na_values, keep_default_na=False, index_col=None)

        logging.log(20, f'successfully read csv file {file_path}')

        # Return the panda DataFrame
        return dataframe

    except FileNotFoundError as err:
        raise Exception(f'File not found ({err})')

    except TypeError as err:
        raise Exception(f'Separator or delimitor type is not correct ({err})')

    except Exception as err:
        raise Exception(f'{err}')


def write_parquet_file(dataframe, dir_path, file_name, logger=None):
    """
    Compress dataframe into parquet files

    :param dataframe: Dataframe to compress
    :type dataframe: pandas.DataFrame
    :param dir_path: Path of the folder where to store files
    :type dir_path: str
    :param file_name: Name of the compressed files in the folder
    :type file_name: str
    :param logger: Logger instance to use
    :type logger: Logger
    """

    f_name = 'write_parquet_file'

    if dir_path is not None and len(dir_path) == 0:
        raise TypeError(f'dir_path should not be empty')

    if file_name is not None and len(file_name) == 0:
        raise TypeError(f'file_name should not be empty')

    filename = "{}/{}.parquet".format(dir_path, file_name)

    try:
        logging.log(10, 'Function write parquet started')

        with open(filename, 'wb') as fileHandler:
            dataframe.to_parquet(fileHandler, engine='pyarrow', compression='snappy')

    except FileNotFoundError as err:
        logging.log(40, f'file not found {filename} ')
        raise err

    except Exception as err:
        logging.log(40, f'canot write file {filename} ')
        raise err

    logging.log(10, f'write successfully {filename} to parquet file')
    return filename
