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


def products_to_dict(ele):
    x = [i.split(',') for i in ele.split('|')]
    return [{i.split(':')[0]: i.split(':')[1]
             for i in j if ':' in i} for j in x]


def split_to_order_lines(ndf, order_cols):
    '''docstring
    order_cols: retain these columns from the order row, fanning out into the order lines table
    '''
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
    return df


def run():
    today = str(dt.datetime.today()).split(' ')[0]

    config = my_utils.import_configs()
    config_dict = dict(config.items('dir_info'))
    path = config_dict['gen_path']
    order_path = my_utils.most_recent_order_file(path)
    print(order_path)
    df = pd.read_csv(order_path)
    df['Order Date'] = pd.to_datetime(df['Order Date'])
    df = df.loc[df['Order Date'] > '2020-01-01']

    res = {}
    res['orders_from_2020-01-01'] = df

    df['product_details_bool'] = df['Product Details'].apply(
        lambda x: 'GMDIS' in x)
    ndf = df.loc[df['product_details_bool'] == True]

    order_cols = [
        'Order ID', 'Order Status', 'Order Date', 'Order Total (inc tax)',
        'Gift Certificate Code', 'Gift Certificate Amount Redeemed'
    ]

    df = split_to_order_lines(ndf, order_cols)
    df['gmdis_bool'] = df['Product SKU'].apply(lambda x: 'GMDIS' in x)
    df = df.loc[df.gmdis_bool]
    res['gmdis_order_lines'] = df

    filename = '_gdmis_orders.xlsx'
    my_utils.save_res_dict_to_xlsx(res, filename)
    return 0


if __name__ == '__main__':
    run()
