'''customer_purchase_tagging.py docstring

### Auto-tagging

- create tags for each product based on website hierarchy
- fan out orders into an order line table by product
- join product details table onto order lines table
- aggregate on customer info, concat tags by comma delimiter

### TODO
- add tags column: https://mailchimp.com/help/format-guidelines-for-your-import-file/

Author: William C. Wright
'''

import my_utils
import pandas as pd
import numpy as np
import datetime as dt

today = str(dt.datetime.today()).split(' ')[0]

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def products_to_dict(ele):
    x = [i.split(',') for i in ele.split('|')]
    return [{i.split(':')[0]: i.split(':')[1]
             for i in j if ':' in i} for j in x]


full_filelist = my_utils.list_files_in_directory('.')
filename = 'orders-2020-12-01_everythin_export_template.csv'

filepath = [i for i in full_filelist if filename in i][0]
df = pd.read_csv(filepath)
df.columns = my_utils.process_cols_v2(list(df))

# key columns to retain after expanding product details column
# order_cols = ['Order ID','Order Status','Order Date'] # ADD CUSTOMER INFO
key_cols = [
    'order_id', 'order_status', 'billing_name', 'order_date',
    'shipping_country', 'shipping_zip', 'shipping_state', 'customer_email'
]

dfs = []
prod_col = 'product_details'
for row in range(len(df)):
    row_dict = {col: df.iloc[row][col] for col in key_cols}
    ele = df.iloc[row][prod_col]
    data = products_to_dict(ele)
    temp = pd.DataFrame(data)
    for col in key_cols:
        temp[col] = row_dict[col]
    dfs.append(temp)

ndf = pd.concat(dfs)
ndf.columns = [i.strip() for i in list(ndf)]
temp = pd.DataFrame(ndf.count()).reset_index()
temp.columns = ['cols', 'col_counts']
cols = list(temp.loc[temp.col_counts > 100].cols)
ndf = ndf[cols]
ndf.columns = [
    i if i in key_cols else my_utils.process_cols_v2([i])[0] for i in list(ndf)
]
ndf.to_csv(today + '_order_lines.csv', index=False)
