#!/usr/bin/env python3
"""This module imports the local version of the Cisco Ready Report, re-formats the contract
    number and contract end-date. Then it creates a new data frame that will be used for
    the response."""

__author__ = "Aaron Davis"
__version__ = "0.1.5"
__copyright__ = "Copyright (c) 2022 Aaron Davis"
__license__ = "MIT License"

import os
import pandas as pd
from timer import Timer


def main():
    raise Exception("This package cannot run without being called.")


def prep(ob_id, attch):
    dress = str(attch.split("/")[-1])
    file_id = str(ob_id) + "_" + dress
    file = f"./attach/{file_id}.xlsb"
    df1 = p_df(file)
    df1 = p_form_date(df1)
    df1 = p_contract(df1)
    df2 = p_filter(df1)
    df2a = p_format(df2)
    print(df2a["Install Site Name"].loc[0])
    print(df2a[["Contract Number"]])
    print(df2a.shape)
    os.remove(file)
    return df2a


def p_df(a_file):
    t = Timer(text="Pandas took {:.4f} seconds to read the file.")
    t.start()
    df = pd.read_excel(
        a_file, engine="pyxlsb", sheet_name="Powered by Cisco Ready", header=3
    )
    t.stop()
    df_1 = pd.DataFrame(df)
    return df_1


def p_form_date(df):
    df["Covered Line End Date"] = pd.to_datetime(
        df["Covered Line End Date"], unit="D", origin="1899-12-30"
    )
    df["Covered Line End Date"] = df["Covered Line End Date"].dt.strftime("%m/%d/%Y")
    return df


def p_contract(df):
    df["Contract Number"] = df["Contract Number"].fillna(0)
    df = df.astype({"Contract Number": "int"})
    return df


def p_filter(df):
    df_n = df[
        (df["Product Type"] == "CHASSIS")
        & (df["Coverage"] == "COVERED")
        & (df["Contract Number"] != 0)
    ]
    return df_n


def p_format(df):
    df = df.drop_duplicates(subset="Contract Number", keep="first")
    df.sort_values(by=["Contract Number"])
    df = df.reset_index(drop=True)
    return df


def pbear(recs):
    for rec in recs:
        recd = rec["id"]
        rid = rec["rid"]
        aid = rec["aid"]
        # resp = rec["RESP"]
        prep(recd, aid)
    return


if __name__ == "__main__":
    main()
