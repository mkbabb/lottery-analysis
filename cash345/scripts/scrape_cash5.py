import datetime
import time
import urllib.request

import pandas as pd
from lxml import etree
import re


from typing import *


CALLS_PER_SECOND = 0.5

CASH5_START = datetime.datetime.strptime("10/27/2006", "%m/%d/%Y")


PATHS = {
    "prize_5": '//*[@id="ctl00_MainContent_ResultsCash5_rptCash5_ctl00_lblPrize5"]',
    "prize_4": '//*[@id="ctl00_MainContent_ResultsCash5_rptCash5_ctl00_lblPrize4"]',
    "winners_5": '//*[@id="ctl00_MainContent_ResultsCash5_rptCash5_ctl00_lblWin5"]',
    "winners_4": '//*[@id="ctl00_MainContent_ResultsCash5_rptCash5_ctl00_lblWin4"]',
    "winners_3": '//*[@id="ctl00_MainContent_ResultsCash5_rptCash5_ctl00_lblWin3"]',
    "winners_2": '//*[@id="ctl00_MainContent_ResultsCash5_rptCash5_ctl00_lblWin2"]',
    "prize_5_double_play": '//*[@id="ctl00_MainContent_ResultsCash5_rptCash5_ctl01_lblPrize5"]',
    "prize_4_double_play": '//*[@id="ctl00_MainContent_ResultsCash5_rptCash5_ctl01_lblPrize5"]',
    "winners_5_double_play": '//*[@id="ctl00_MainContent_ResultsCash5_rptCash5_ctl01_lblWin5"]',
    "winners_4_double_play": '//*[@id="ctl00_MainContent_ResultsCash5_rptCash5_ctl01_lblWin4"]',
    "winners_3_double_play": '//*[@id="ctl00_MainContent_ResultsCash5_rptCash5_ctl01_lblWin3"]',
    "winners_2_double_play": '//*[@id="ctl00_MainContent_ResultsCash5_rptCash5_ctl01_lblWin2"]',
}

HEADER = {"User-Agent": "Lottery Research 1.0.0"}


def dollar_to_int(s: str) -> Optional[int]:
    s = s.replace(",", "")
    if (dollars := re.match("\$(\d+)", s)) is not None:
        return int(dollars.group(1))
    else:
        return None


def query_nclotto(date_string):
    url = f"https://nclottery.com/Cash5Pay?dd={date_string}"
    req = urllib.request.Request(url, headers=HEADER)
    response = urllib.request.urlopen(req)
    data = response.read().decode("utf-8")

    return etree.HTML(data)


def scrape_cash5(out_path: str, start_date: datetime.time, end_date: datetime.time):
    data_dict = {"date": []}
    data_dict.update({key: [] for key in PATHS.keys()})

    total_days = (end_date - start_date).days
    dates = (start_date + datetime.timedelta(i) for i in range(total_days))

    for date in dates:
        date_string = date.strftime("%m/%d/%Y")
        cash5_html = query_nclotto(date_string)
        data_dict["date"].append(date_string)

        for key, path in PATHS.items():
            try:
                node = cash5_html.xpath(path)[0]
                value = node.text

                dollars = dollar_to_int(value)

                if dollars is not None:
                    data_dict[key].append(str(dollars))
                else:
                    data_dict[key].append(value)

            except Exception as e:
                data_dict[key].append("0.0")

        time.sleep(CALLS_PER_SECOND)

    df = pd.DataFrame(data_dict)
    df.to_csv(out_path, index=False)
    return df


start_date = datetime.datetime.strptime("10/27/2006", "%m/%d/%Y")
# start_date = datetime.datetime.strptime("01/01/2022", "%m/%d/%Y")
end_date = datetime.datetime.now()

out_path = f"cash345/data/cash5_scraped_2022.csv"

df = scrape_cash5(out_path, start_date, end_date)
