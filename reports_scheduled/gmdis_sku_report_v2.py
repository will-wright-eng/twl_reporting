"""gmdis_sku_report.py docstring

### This script is for generating a product-level list of orders, 
    specific to skus containing 'GMDIS'

TODO

- make file name a system input
- rework entire script into class
- alternative ways of exporting orders that include line level information

Author: William C. Wright
"""

import datetime as dt
import configparser

import pandas as pd
import numpy as np

import my_utils


def products_to_dict(ele):
    x = [i.split(",") for i in ele.split("|")]
    return [{i.split(":")[0]: i.split(":")[1] for i in j if ":" in i} for j in x]


def run():
    outputs = {}
    today = str(dt.datetime.today()).split(" ")[0]

    config = my_utils.import_configs()
    config_dict = dict(config.items("dir_info"))
    path = config_dict["gen_path"]
    order_path = my_utils.most_recent_order_file(path)
    print("\n", order_path, "\n")
    df = pd.read_csv(order_path)

    # convert datetime
    df["Order Date"] = pd.to_datetime(df["Order Date"])
    df = df.loc[df["Order Date"] > "2020-08-01"]
    outputs["raw_data_post_2020-08-01"] = df

    df.reset_index(inplace=True, drop=True)
    df.columns = [i.strip() for i in list(df)]

    df_products = []
    prods = []
    for row in range(len(df)):
        prod_dict = products_to_dict(df["Product Details"][row])
        order_id = str(df["Order ID"][row]).strip()
        df["Order ID"][row] = order_id
        order_dict = {"Order ID": order_id}
        [i.update(order_dict) for i in prod_dict]
        prods.append(prod_dict)

    ndf = pd.DataFrame([item for sublist in prods for item in sublist])

    metadf = pd.DataFrame([list(ndf), ndf.dtypes, ndf.count()]).T
    metadf.columns = ["name", "type", "counts"]
    metadf.sort_values("counts", ascending=False, inplace=True)
    cols = list(metadf.loc[metadf.counts > len(ndf) * 0.9].name)

    ndf = ndf[[col for col in list(ndf) if col in cols]]

    ndf = ndf.merge(df, how="outer", on="Order ID")
    ndf.columns = [i.strip() for i in list(ndf)]
    ndf["Product SKU"] = ndf["Product SKU"].apply(lambda x: str(x).strip())

    outputs["order_lines_table"] = ndf

    ndf["gmdis_bool"] = ndf["Product SKU"].apply(lambda x: "GMDIS" in x)
    ndf = ndf.loc[ndf["gmdis_bool"]]

    outputs["order_lines_table-by_gdmis_skus"] = ndf

    filename = "_gdmis_orders.xlsx"
    my_utils.save_res_dict_to_xlsx(outputs, filename)
    return True


if __name__ == "__main__":
    run()
