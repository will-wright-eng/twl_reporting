"""inventory_valutation.py docstring

### This script is for generating a inventory valuation pivot 
    table from a product export

TODO

- how to validate/verify this data is correct?
- make file name a system input
- rework entire script into class

Author: William C. Wright
"""

import collections
import datetime as dt
import configparser

import pandas as pd
import numpy as np

import my_utils


def lists_calc(list0, list1):
    a = list0
    b = list1
    a_multiset = collections.Counter(a)
    b_multiset = collections.Counter(b)
    overlap = list((a_multiset & b_multiset).elements())
    a_remainder = list((a_multiset - b_multiset).elements())
    b_remainder = list((b_multiset - a_multiset).elements())
    return overlap, a_remainder, b_remainder


def convert_zeros(ele):
    if ele == 0:
        return np.nan
    else:
        return ele


def bool_fxn(col1, col2, col_bool):
    if col_bool:
        return col2
    else:
        return col1


def clean_csv(file_path):
    df = pd.read_csv(file_path)
    df.columns = my_utils.process_cols_v2(list(df))
    return df


def run():
    today = str(dt.datetime.today()).split(" ")[0]

    config = my_utils.import_configs()
    config_dict = dict(config.items("dir_info"))
    path = config_dict["gen_path"]
    prod_path_bs = my_utils.most_recent_product_file(path)
    print("\n", prod_path_bs, "\n")
    prod_path_ls = config_dict["ls_prods"]

    files = [prod_path_bs, prod_path_ls]
    dfs = [clean_csv(file_path) for file_path in files]

    # filter by column criteria
    df = dfs[0].loc[dfs[0].product_type == "P"]
    df = df.loc[pd.isnull(df.product_code_sku) == False]
    df = df.dropna(axis=1, how="all")
    df = df.loc[df.product_visible == "Y"]

    # create dataframe of lightspeed overlapping SKUs
    overlap, _, _ = lists_calc(list(df.product_code_sku), list(dfs[1].custom_sku))
    overlap.remove("MVP-147")  # duplicate in lightspeed df
    skus = pd.DataFrame(overlap)
    skus.columns = ["sku"]
    df_ls = skus.merge(dfs[1], left_on="sku", right_on="custom_sku", how="left")
    df_ls = df_ls.dropna(axis=1, how="all")

    df = df.merge(df_ls, how="left", left_on="product_code_sku", right_on="sku")

    # clean dollar amounts
    cols = [i for i in list(df) if "price" in i] + [i for i in list(df) if "_cost" in i]
    for col in cols:
        df[col] = df[col].apply(lambda x: convert_zeros(x))

    cols_dict = {
        "price": "price_x",
        "cost": "combined_cost_col",
        "stock": "current_stock_level",
        "inv_val": "inventory_value_price",
        "inv_val_cost": "inventory_value_cost",
        "groupby": "product_category",
    }

    # aggregate cost columns
    df["cost_price_null"] = pd.isnull(df.cost_price)
    df[cols_dict["cost"]] = df.apply(
        lambda x: bool_fxn(x["cost_price"], x["default_cost"], x["cost_price_null"]), axis=1
    )

    # assign data types
    df[cols_dict["stock"]] = df[cols_dict["stock"]].astype(int)
    x = [cols_dict["price"], cols_dict["stock"], cols_dict["cost"]]
    for col in x:
        df[col] = df[col].astype(float)

    # calculate inventory value
    df[cols_dict["inv_val"]] = df[cols_dict["price"]] * df[cols_dict["stock"]]
    df[cols_dict["inv_val_cost"]] = df[cols_dict["cost"]] * df[cols_dict["stock"]]
    df[cols_dict["groupby"]] = df.category_x.apply(lambda x: x.split("/")[0].split(";")[0])

    res = {}
    res["cleaned_data"] = df

    # generate and save pivot tables
    pivot_df = df.groupby(["brand_name", cols_dict["groupby"]]).agg(
        {
            cols_dict["price"]: ["mean", "sum"],
            cols_dict["stock"]: ["mean", "sum"],
            cols_dict["cost"]: ["mean", "sum"],
            cols_dict["inv_val"]: "sum",
            cols_dict["inv_val_cost"]: "sum",
        }
    )
    pivot_df.reset_index(inplace=True, drop=False)
    res["pivot_by_brand"] = pivot_df

    pivot_df = df.groupby([cols_dict["groupby"]]).agg(
        {
            cols_dict["price"]: ["mean", "sum"],
            cols_dict["stock"]: ["mean", "sum"],
            cols_dict["cost"]: ["mean", "sum"],
            cols_dict["inv_val"]: "sum",
            cols_dict["inv_val_cost"]: "sum",
        }
    )
    pivot_df.reset_index(inplace=True, drop=False)
    res["pivot_by_category"] = pivot_df

    filename = "_inventory_value_report.xlsx"
    my_utils.save_res_dict_to_xlsx(res, filename)
    return True


if __name__ == "__main__":
    run()
