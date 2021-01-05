'''
utility file for common python fuctions
'''

import os
import re
import string
import configparser
import datetime as dt

import pandas as pd

today = str(dt.date.today())

def list_files_in_directory(path):
    '''docstring for list_files_in_directory'''
    x = []
    for root, dirs, files in os.walk(path):
        for file in files:
            x.append(root + '/' + file)
    return x


def process_cols_v2(cols):
    '''docstring for process_cols
    for processing: remove special characters
    '''
    chars = re.escape(string.punctuation)
    clean = [re.sub(r'[' + chars + ']', '', my_str) for my_str in cols]
    clean = [i.lower().replace(' ', '_') for i in clean]
    clean = ['product_code_sku' if 'product_code' in i else i for i in clean]
    return clean


def most_recent_product_file(path):
    '''most_recent_product_file docstring'''
    files = list_files_in_directory(path)
    x = [i for i in files if 'products-' in i]
    x.sort()
    filepath = x[-1]
    return filepath


def most_recent_order_file(path):
    '''most_recent_product_file docstring'''
    files = list_files_in_directory(path)
    x = [i for i in files if 'orders-' in i]
    x.sort()
    filepath = x[-1]
    return filepath


def import_configs():
    config = configparser.ConfigParser()
    config.read('project.cfg')
    return config


def create_directory(folders, logger=None):
    '''create_directory docstring'''
    for folder in folders:
        try:
            os.mkdir(folder)
        except FileExistsError as e:
            if logger:
                logger.info(e)
            else:
                print(e)


def save_res_dict_to_xlsx(res, filename, folder='results'):
    '''docstring
    # write to multiple sheets
    '''
    create_directory([folder])
    with pd.ExcelWriter(folder + '/' + today + filename) as writer:
        for sheet, df in res.items():
            df.to_excel(writer, sheet_name=sheet)
