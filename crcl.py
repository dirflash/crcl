#!/usr/bin/env python3
"""This is used as a bot to replies to a Webex App message with an attached Cisco Ready report via
webhook and responds back with the active contracts found in the report."""

__author__ = "Aaron Davis"
__version__ = "0.1.5"
__copyright__ = "Copyright (c) 2022 Aaron Davis"
__license__ = "MIT License"

import configparser
import os
import pandas as pd
import requests

KEY = "CI"
if os.getenv(KEY):
    webex_bearer = os.environ["webex_bearer"]
else:
    config = configparser.ConfigParser()
    config.read("config.ini")
    webex_bearer = config["DEFAULT"]["bearer"]

url = "https://webexapis.com/v1/contents/Y2lzY29zcGFyazovL3VzL0NPTlRFTlQvMzlkYzZmMTAtMmJlYi0xMWVkLTg4NDgtMzdiZmI4YWZkYmJkLzA"

payload = {}
headers = {
    "Authorization": webex_bearer,
    "Content-Type": "application/json",
}

response = requests.request("GET", url, headers=headers, data=payload, timeout=5)

print(response)

output = open("test.xlsb", "wb")
output.write(response.content)
output.close()

df = pd.read_excel(
    "test.xlsb", engine="pyxlsb", sheet_name="Powered by Cisco Ready", header=3
)
df1 = pd.DataFrame(df)

labels = [title for title in df.columns]

df1["Covered Line End Date"] = pd.to_datetime(
    df1["Covered Line End Date"], unit="D", origin="1899-12-30"
)
df1["Covered Line End Date"] = df1["Covered Line End Date"].dt.strftime("%m/%d/%Y")
df1["Contract Number"] = df1["Contract Number"].fillna(0)
df1 = df1.astype({"Contract Number": "int"})

df2 = df1[
    (df1["Product Type"] == "CHASSIS")
    & (df1["Coverage"] == "COVERED")
    & (df1["Contract Number"] != 0)
]
df2 = df2.drop_duplicates(subset="Contract Number", keep="first")
df2.sort_values(by=["Contract Number"])
df2 = df2.reset_index(drop=True)

print(df2["Install Site Name"].loc[0])
print(df2[["Contract Number"]])
print(df2.shape)

os.remove("test.xlsb")
