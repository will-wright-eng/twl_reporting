'''mailchimp_tag_csv_gen.py docstring

### Generate Tagging csvs for MailChimp

- each tag needs to be imported separately by email list, referencing the existing audience


Author: William C. Wright
'''

import my_utils
import pandas as pd
import numpy as np

from collections import Counter
import datetime as dt

today = str(dt.datetime.today()).split(' ')[0]


def count_table_from_df(df):
    temp = pd.DataFrame(df.count()).reset_index()
    temp.columns = ['cols', 'col_counts']
    temp = temp.sort_values('col_counts', ascending=False)
    return temp


def check_for_tag(ele, tag):
    if tag in ele:
        return True
    else:
        return False


def gen_bool_tag_cols(df, tags_col):
    df = df.loc[pd.isnull(df[tags_col]) == False]
    for tag in key_tags:
        tag_col = 'tag_' + my_utils.process_cols_v2([tag])[0]
        df[tag_col] = df[tags_col].apply(lambda x: check_for_tag(x, tag))
    return df


key_cols = [
    'order_id', 'order_status', 'billing_name', 'order_date',
    'shipping_country', 'shipping_zip', 'shipping_state', 'customer_email'
]

full_filelist = my_utils.list_files_in_directory('.')

filename = 'products-2020-11-28.csv'
filepath = [i for i in full_filelist if filename in i][0]
df = pd.read_csv(filepath)
df.columns = my_utils.process_cols_v2(list(df))

df = df.loc[df.product_type == 'P']
df = df.loc[pd.isnull(df.product_code_sku) == False]
df = df.dropna(axis=1, how='all')
df['tags'] = df.category.apply(
    lambda x: [i.split(';')[0] for i in x.split('/')])

# df.tags

# l = list(df.category.apply(lambda x: [i.split(';')[0] for i in x.split('/')]))
l = list(df.tags)
l_flat = [y for x in l for y in x]

x = Counter(l_flat)
temp = pd.DataFrame(
    x.most_common())  #.to_csv('tag_frequency.csv',index=False,header=False)
key_tags = list(temp.iloc[:20][0])

ndf = pd.read_csv('2020-12-07_order_lines.csv')
ndf = ndf.loc[pd.isnull(ndf.product_id) == False]
ndf.product_id = ndf.product_id.astype(int)
df2 = ndf.merge(df, on='product_id', how='left')

temp = df2[key_cols + ['tags']]
temp = temp.loc[pd.isnull(temp.tags) == False]
# temp = temp.groupby(['customer_email','billing_name','shipping_zip']).agg({
temp = temp.groupby(['customer_email']).agg({
    'order_date': ['min', 'max', 'count'],
    'tags': ['sum'],
})
temp.reset_index(inplace=True)
temp.columns = [
    ' '.join(col).strip().replace(' ', '_') for col in temp.columns.values
]
temp = gen_bool_tag_cols(temp, 'tags_sum')
