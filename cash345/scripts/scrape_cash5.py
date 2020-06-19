import datetime
import time
import urllib.request

import pandas as pd
from lxml import etree

from utils import dollar_to_float

from typing import *
from utils import file_components

CALLS_PER_SECOND = 0.25

CASH5_START = datetime.datetime.strptime("10/27/2006", "%m/%d/%Y")


PATHS = {"prize_5": '//*[@id="ctl00_MainContent_lblCash5Match5Prize"]',
         "prize_4": '//*[@id="ctl00_MainContent_lblCash5Match4Prize"]',

         "winners_5": '//*[@id="ctl00_MainContent_lblCash5Match5"]',
         "winners_4": '//*[@id="ctl00_MainContent_lblCash5Match4"]',
         "winners_3": '//*[@id="ctl00_MainContent_lblCash5Match3"]',
         "winners_2": '//*[@id="ctl00_MainContent_lblCash5Match2"]',

         "jackpot": '//*[@id="ctl00_MainContent_lblCash5TopPrize"]'}

HEADER = {'User-Agent': 'Lottery Research 0.9.0'}


def query_nclotto(date_string):
    url = f"https://nclottery.com/Cash5?dd={date_string}"
    req = urllib.request.Request(url, headers=HEADER)
    response = urllib.request.urlopen(req)
    data = response.read().decode('utf-8')

    return etree.HTML(data)


def scrape_cash5(out_path: str,
                 start_date: datetime.time,
                 end_date: datetime.time):
    total_days = (end_date - start_date).days
    data_dict = {key: [] for key in PATHS.keys()}

    for n, date in enumerate((start_date + datetime.timedelta(i)
                              for i in range(total_days))):
        date_string = date.strftime("%m/%d/%Y")
        cash5_html = query_nclotto(date_string)
        data_dict["date"] = date_string

        for key, path in PATHS.items():
            try:
                node = cash5_html.xpath(path)[0]
                value = node.text

                dollars = dollar_to_float(value)
                if (dollars is not None):
                    data_dict[key].append(str(dollars))
                else:
                    data_dict[key].append(value)
            except Exception as e:
                print(e)

        time.sleep(CALLS_PER_SECOND)

    df = pd.DataFrame(data_dict)
    df = df.reindex(list(sorted(df.columns)),
                    axis=1)
    df.to_csv(out_path, index=False)
    return df


start_date = datetime.datetime.strptime("11/27/2019", "%m/%d/%Y")
end_date = datetime.datetime.now()

start_date_string = start_date.strftime("%m/%d/%Y")
end_date_string = end_date.strftime("%m/%d/%Y")

out_path = f"cash345/data/cash5_scraped_{start_date_string}_{end_date_string}.csv"
# out_path = f"cash345/data/cash5_scraped.csv"

df = scrape_cash5(out_path, start_date, end_date)
