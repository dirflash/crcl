""" rapid request filter """

import logging


def new_recs(recs):
    """Create a list of document _id's

    Args:
        recs (dict): Mongo records

    Returns:
        list: document _id's
    """
    record_ids = [record.get("_id") for record in recs]
    logging.info("Before rapid request filter: \n\t%s", record_ids)
    return record_ids


def check_prep(recs, collect):
    dup_check = []
    for rec in recs:
        step = collect.find_one(rec)
        rec = step["_id"]
        ts = step["TS"]
        pid = step["PID"]
        rid = step["RID"]
        aid = step["AID"]
        dup_check.append({"id": rec, "ts": ts, "pid": pid, "rid": rid, "aid": aid})
    return dup_check


def rapid_filter(records, mon_col):
    rec_ids = new_recs(records)
    dup_prep = check_prep(rec_ids, mon_col)
    sample_size = len(dup_prep)
    print(sample_size)


"""

for x in reversed(range(LEN_DUP_CHECK)):
    logging.info("Duplicate primary check index: %s", x)
    source_dup_check = dup_check[x]["record_id"]["_id"]
    source_e_compare = dup_check[x]["email"]
    source_m_compare = dup_check[x]["msg_created"]
    if isinstance(source_m_compare, str):
        update_created(source_dup_check, source_m_compare)
    source_lookup = collection.find_one(record_id)
    source_m_converted = source_lookup["createdAt"]
    sec_dup_check = x - 1
    logging.info("Duplicate compare check index: %s", sec_dup_check)
    if sec_dup_check >= 0:
        secondary_dup_check = dup_check[sec_dup_check]["record_id"]["_id"]
        sec_e_compare = dup_check[sec_dup_check]["email"]
        sec_m_compare = dup_check[sec_dup_check]["msg_created"]
        if isinstance(sec_m_compare, str):
            update_created(secondary_dup_check, sec_m_compare)
        sec_lookup = collection.find_one(record_id)
        sec_m_converted = sec_lookup["createdAt"]
        if source_e_compare == sec_e_compare:
            logging.info("Duplicate email address found.")
            record_delta = source_m_converted - sec_m_converted
            logging.info("Compare messages record delta: %s", record_delta)
            if record_delta < timedelta(seconds=10):
                print("within threshold - not a good message")
                logging.info("Duplicate message sent less than 10 seconds.")
                tagged_msg_id = record_ids[x]
                logging.info("Tag msg id  %s as duplicate.", tagged_msg_id)
                try:
                    dup_msg_id = record_ids[x]
                    collection.update_one(
                        {"_id": dup_msg_id},
                        {"$set": {"response": "duplicate"}},
                    )
                    record_ids.pop(x)
                except ConnectionFailure as update_err:
                    logging.exception(update_err)
            else:
                print("exceeded threshold - send good message")
"""

# end rapid request filter