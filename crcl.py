#!/usr/bin/env python3
"""This is used as a bot to replies to a Webex App message with an attached Cisco Ready report via
webhook and responds back with the active contracts found in the report."""

__author__ = "Aaron Davis"
__version__ = "0.1.5"
__copyright__ = "Copyright (c) 2022 Aaron Davis"
__license__ = "MIT License"

import configparser
import os
import sys
import logging
import pandas as pd
import requests
import certifi
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from timer import Timer
from mongo import rrf
from ready import ready as rdy
from pbear import bear

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        # logging.FileHandler(r".\logs\debug.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

KEY = "CI"
if os.getenv(KEY):
    webex_bearer = os.environ["webex_bearer"]
    mongouser = os.environ["mongouser"]
    mongopw = os.environ["mongopw"]
else:
    config = configparser.ConfigParser()
    config.read("config.ini")
    webex_bearer = config["DEFAULT"]["webex_bearer"]
    mongouser = config["DEFAULT"]["mon_un"]
    mongopw = config["DEFAULT"]["mon_pd"]

mongoaddr = "cluster0.jzvod.mongodb.net"
mongodb = "CRCL"
mongocollect = "crcl"

MAX_MONGODB_DELAY = 500

Mongo_Client = MongoClient(
    f"mongodb+srv://{mongouser}:{mongopw}@{mongoaddr}/{mongodb}?retryWrites=true&w=majority",
    tlsCAFile=certifi.where(),
    serverSelectionTimeoutMS=MAX_MONGODB_DELAY,
)

db = Mongo_Client[mongodb]
collection = db[mongocollect]

t = Timer()

logging.info("------------------------------------------------------")

new_record = collection.find({"DUP": "Null"})
num_records = collection.count_documents({"DUP": "Null"})

rrf.rapid_filter(new_record, collection)
unique_recs = rdy.report(collection, webex_bearer)
bear.pbear(unique_recs)
