import logging
from requests import request


def main():
    raise Exception("This package cannot run without being called.")


def uniques(collect):
    new_record = collect.find({"DUP": False})
    num_records = collect.count_documents({"DUP": False})
    logging.info("Unique entries: %s", (num_records))
    unqs = []
    for rec in new_record:
        step = collect.find_one(rec)
        recd = step["_id"]
        pid = step["PID"]
        rid = step["RID"]
        aid = step["AID"]
        dup = step["DUP"]
        if "RESP" not in step:
            resp = "Null"
        else:
            resp = step["RESP"]
        unqs.append(
            {"id": recd, "pid": pid, "rid": rid, "aid": aid, "dup": dup, "resp": resp}
        )
    return unqs


def attach(uques, wx_bear):
    for rec in uques:
        if rec["resp"] == "Null":
            oid = rec["id"]
            aid = rec["aid"]
            dress = str(oid) + "_" + str(aid.split("/")[-1])

            payload = {}
            headers = {
                "Authorization": wx_bear,
                "Content-Type": "application/json",
            }

            response = request("GET", aid, headers=headers, data=payload, timeout=5)
            logging.info("Retrieved file: %s", (dress))
            file = f"./attach/{dress}.xlsb"
            with open(file, "wb") as output:
                output.write(response.content)
                logging.info("Created file: %s", (file))


def report(collection, wex_bear):
    unis = uniques(collection)
    attach(unis, wex_bear)
    return unis


if __name__ == "__main__":
    main()
