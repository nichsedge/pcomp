import os
import pandas as pd
from datetime import datetime
import base64
import json
import requests
from fake_useragent import UserAgent
import pandas_gbq
import re

from google.oauth2 import service_account

credentials_json = os.getenv("TF_VAR_bq_creds_file")
try:
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(credentials_json),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
except:
    credentials = service_account.Credentials.from_service_account_file(
        credentials_json,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

# session = requests.Session()
# response = session.get('https://www.enterkomputer.com/category/17/processor')

# # cookies
# cookies = session.cookies.get_dict()
# str_cookies = f"enter_session={cookies['enter_session']}; csrf_cookie_name={cookies['csrf_cookie_name']}"


# # token
# token_pattern = r'data-api-token="([^"]+)"'
# token_match = re.search(token_pattern, response.text)
# token = token_match.group(1)

# # signature
# signature_pattern = r'data-api-signature="([^"]+)"'
# signature_match = re.search(signature_pattern, response.text)
# signature = signature_match.group(1)

str_cookies = "_ga=GA1.1.1720223009.1763789067; cf_clearance=3JwInv6PN9evQdb80Ez5yQ7ZrPmx0sFWGMsq9.hNB04-1763797195-1.2.1.1-ZdA0VakTBtLvnoLmArvETkQrHhuM9qwH2o_YsyeE56swR0F9tEJOu4hhMnHS48m5Awoyg8hz5U2.W_AjeEeZ.1RYuTsaDwEwnGeRvUCWKB4IIfKAFuSe16u.gXkWa34pov2Qb5vC0srE8DcQIrtE4pNlGp5Q3QGE.rF6tFYPTX25kwwAZp5gewGTbV1xyUTUs49_ztrf5cybt.WOYfc.bBCOFp9e4rwqPOM54w2_E5o; enter_session=2e675e5178bd4792abaa1993a652572b5cc8c7d2; csrf_cookie_name=17e503d3b31823786376d3af251bb5c0; _ga_34MD92JJJB=GS2.1.s1763797196$o2$g1$t1763797299$j48$l0$h0"
token = "U2FsdGVkX1-E55sT1JEmUtTtgjHvzgK98PZU8pKsTjQf8t2cV6U0Rrrd5ijzmdtRiKOvKb944B267vLzsZdvag"
signature = "5a512f74fbd61e01a14cf94c7b90b972"


def fetch_product_list(cat_id, cat):
    """
    Fetches the product list from Enterkomputer API for a given category.
    """
    product_list = []
    page_counter = 1
    status = True

    ua = UserAgent()

    while status:
        url = "https://www.enterkomputer.com/jeanne/v2/product-list"
        headers = {
            "User-Agent": ua.random,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.5",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://www.enterkomputer.com",
            "Referer": f"https://www.enterkomputer.com/category/{cat_id}/{cat}",
            # "Cookie": str_cookies,
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

        data = {
            "KCODE": cat_id,
            "SCODE": "all",
            "BCODE": "all",
            "BNAME": "",
            "MORDR": "default",
            "MSTGE": "mapping",
            "MKYWD": "",
            "MTAGS": "",
            "MSGMN": "category",
            "MPAGE": page_counter,
            "token": token,
            "signature": signature,
        }

        json_response = json.loads(requests.post(url, headers=headers, json=data).text)
        print(cat, page_counter, json_response["status"])

        page_counter += 1
        status = json_response["status"]

        if status:
            products = json_response["result"][0]["PPRNT"][0]["PCHLD"]
            for p1 in products:
                for p2 in p1["PLIST"]:
                    product_list.append(p2)

    return product_list


def main():
    categories = [
        ["17", "processor"],
        ["24", "vga"],
        ["11", "memory-ram"],
        # ["12", "motherboard"],
        # ["3", "casing"],
        # ["9", "lcd"],
        # ["4", "cooler"],
        # ["8", "keyboard"],
        # ["19", "psu"],
        ["101", "solid-state-drive"],
        # ["6", "hard-disk"],
    ]

    all_product_list = []
    for cat_id, cat in categories:
        product_list = fetch_product_list(cat_id, cat)
        all_product_list.extend(product_list)

    df = pd.DataFrame(all_product_list)
    df["inserted_at"] = datetime.now().date()

    # df.to_csv("enterkomputer_raw.csv", index=False)

    df = df.astype(str)

    table_id = "de_zoomcamp.enterkomputer_raw"
    pandas_gbq.to_gbq(
        dataframe=df,
        credentials=credentials,
        destination_table=table_id,
        if_exists="append",
    )

    print(df.shape, "Data exported to CSV successfully.")


if __name__ == "__main__":
    main()
