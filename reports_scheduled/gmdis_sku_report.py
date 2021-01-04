'''gmdis_sku_report.py docstring

### This script is for generating a product-level list of orders, 
    specific to skus containing 'GMDIS'

TODO

- make file name a system input
- rework entire script into class
- alternative ways of exporting orders that include line level information

Author: William C. Wright
'''

import datetime as dt
import configparser

import pandas as pd
import numpy as np

import my_utils


def gmdis_check(ele):
    if 'GMDIS' in ele:
        return True
    else:
        return False


def products_to_dict(ele):
    x = [i.split(',') for i in ele.split('|')]
    return [{i.split(':')[0]: i.split(':')[1]
             for i in j if ':' in i} for j in x]


def run():
    today = str(dt.datetime.today()).split(' ')[0]

    config = my_utils.import_configs()
    config_dict = dict(config.items('dir_info'))
    path = config_dict['gen_path']
    order_path = my_utils.most_recent_order_file(path)
    df = pd.read_csv(order_path)

    df['product_details_bool'] = df['Product Details'].apply(
        lambda x: gmdis_check(x))
    ndf = df.loc[df['product_details_bool'] == True]

    order_cols = ['Order ID', 'Order Status', 'Order Date']
    dfs = []
    for row in range(len(ndf)):
        row_dict = {col: ndf.iloc[row][col] for col in order_cols}
        ele = ndf.iloc[row]['Product Details']
        data = products_to_dict(ele)
        df = pd.DataFrame(data)
        for col in order_cols:
            df[col] = row_dict[col]
        dfs.append(df)

    df = pd.concat(dfs)
    df.columns = [i.strip() for i in list(df)]
    df['gmdis_bool'] = df['Product SKU'].apply(lambda x: 'GMDIS' in x)
    df = df.loc[df.gmdis_bool]

    folder = 'results'
    my_utils.create_directory([folder])
    df.to_csv(folder + '/' + today + '_gdmis_orders.csv', index=False)
    return 0


if __name__ == '__main__':
    run()
