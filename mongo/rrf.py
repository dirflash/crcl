#!/usr/bin/env python3
"""This module is used check all the new Mongo DB records to make sure there are no duplicates,
    before responding."""

__author__ = "Aaron Davis"
__version__ = "0.1.5"
__copyright__ = "Copyright (c) 2022 Aaron Davis"
__license__ = "MIT License"

""" rapid request filter """

import logging
from datetime import datetime, timedelta


def main():
    raise Exception("This package cannot run without being called.")


def new_recs(recs):
    """Create a list of document _id's

    Args:
        recs (dict): Mongo records

    Returns:
        list: document _id's
    """

    record_ids = [record.get("_id") for record in recs]
    logging.info("Database entries before rapid request filter: %s", len(record_ids))
    return record_ids


def check_prep(recs, collect):
    """Create list of dicts of Mongo records

    Args:
        recs (str): Mongo DB record IDs
        collect (str): Mongo DB connection URL

    Returns:
        list: List of dicts of Mongo records
    """
    dup_check = []
    for rec in recs:
        step = collect.find_one(rec)
        recd = step["_id"]
        ts = step["TS"]
        pid = step["PID"]
        rid = step["RID"]
        aid = step["AID"]
        dup = step["DUP"]
        if isinstance(ts, str):
            ts = conv_ts(collect, recd, ts)
        if "RESP" in step:
            resp = step["RESP"]
        else:
            resp = "Null"
        dup_check.append(
            {
                "id": recd,
                "ts": ts,
                "pid": pid,
                "rid": rid,
                "aid": aid,
                "dup": dup,
                "resp": resp,
            }
        )
    return dup_check


def conv_ts(ts_collect, ts_recd, ts_str):
    """If Mongo record timestamp is string type, convert it to datetime.

    Args:
        ts_collect (str): Mongo DB connection URL
        ts_recd (str): Mongo DB record ID
        ts_str (str): timestamp record to be converted

    Returns:
        datetime: converted timestamp
    """
    mydate = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    ts_collect.update_one({"_id": ts_recd}, {"$set": {"TS": mydate}})
    return mydate


def dup_check(r_cnt, pre_list):
    """Scan the pre_list and create a new list of "duplicate" requests.
        Duplicate requests are records that match and the timestamp is
        < 30 seconds.

    Args:
        r_cnt (int): Record count
        pre_list (list): List of records to compare for duplication

    Returns:
       list: List of duplicate record IDs
    """
    p_idx = []
    for x in reversed(range(r_cnt)):
        for z in reversed(range(x)):
            if z >= 0:
                z_id = pre_list[z]["id"]
                pid_dup = pre_list[x]["pid"] == pre_list[z]["pid"]
                rid_dup = pre_list[x]["rid"] == pre_list[z]["rid"]
                if pid_dup == rid_dup:
                    if (pre_list[x]["ts"] - pre_list[z]["ts"]) < timedelta(seconds=30):
                        logging.info("Database entry %s is a duplicate.", z)
                        p_idx.append(z_id)
    return p_idx


def dup_edits(mon, dup_ids, data_list):
    """Compare data_list with dup_ids to create a list of database records that will have
        'DUP' records updated to indicate duplicates.

    Args:
        mon (str): Mongo DB connection URL
        dup_ids (list): List of duplicate record IDs
        data_list (list): List of records that includes all record IDs
    """
    all_ids = [obj_id["id"] for obj_id in data_list]
    dup_idx = [all_ids.index(x) for x in dup_ids]
    go_obj = all_ids
    for n in dup_idx:
        go_obj.pop(n)
    for rd in go_obj:
        mon.update_one({"_id": rd}, {"$set": {"DUP": False}})
    for rd in dup_ids:
        mon.update_one({"_id": rd}, {"$set": {"DUP": True}})
        mon.update_one({"_id": rd}, {"$set": {"RESP": True}})
    logging.info("Duplicate database entries: %s", len(dup_ids))
    logging.info("Unique database entries: %s", len(go_obj))


def rapid_filter(records, r_recs, mon_col):
    rec_ids = new_recs(records)
    dup_prep = check_prep(rec_ids, mon_col)
    record_count = len(dup_prep)
    dup_idx = dup_check(record_count, dup_prep)
    dup_edits(mon_col, dup_idx, dup_prep)
    return


if __name__ == "__main__":
    main()

# end rapid request filter
