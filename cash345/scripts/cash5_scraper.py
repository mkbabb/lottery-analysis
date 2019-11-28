import urllib.request
from lxml import etree

import pandas as pd
import datetime
from utils import dollar_to_float


START_DATE = datetime.datetime.strptime("10/27/2006", "%m/%d/%Y")
END_DATE = datetime.datetime.now()

TOTAL_DAYS = (END_DATE - START_DATE).days


PATHS = {"prize_5": '//*[@id="ctl00_MainContent_lblCash5Match5Prize"]',
         "prize_4": '//*[@id="ctl00_MainContent_lblCash5Match4Prize"]',

         "winners_5": '//*[@id="ctl00_MainContent_lblCash5Match5"]',
         "winners_4": '//*[@id="ctl00_MainContent_lblCash5Match4"]',
         "winners_3": '//*[@id="ctl00_MainContent_lblCash5Match3"]',
         "winners_2": '//*[@id="ctl00_MainContent_lblCash5Match2"]',

         "jackpot": '//*[@id="ctl00_MainContent_lblCash5TopPrize"]'}


def URL_TEMPLATE(date): return f"https://nclottery.com/Cash5?dd={date}"


hdr = {'User-Agent': 'Lottery Research 0.9.0'}

data = {"prize_5": [],
        "prize_4": [],
        "winners_5": [],
        "winners_4": [],
        "winners_3": [],
        "winners_2": [],
        "jackpot": []}


for n, date in enumerate((START_DATE + datetime.timedelta(i)
                          for i in range(TOTAL_DAYS))):
    print(n, date)
    try:
        date_string = date.strftime("%m/%d/%Y")
        url = URL_TEMPLATE(date_string)

        req = urllib.request.Request(url, headers=hdr)
        response = urllib.request.urlopen(req)
        txt = response.read().decode('utf-8')
        root = etree.HTML(txt)

        for key in data.keys():
            value = ""

            try:
                node = root.xpath(PATHS[key])[0]
                try:
                    value = node.text
                except:
                    pass
            except IndexError:
                pass

            if (value.find("$") != -1):
                value = str(dollar_to_float(value))
            elif (value.find(",")):
                value = value.replace(",", "")

            data[key].append(value)
    except:
        break

out_path = "cash345/data/cash5_winnings.csv"

df = pd.DataFrame(data)
df = df.reindex(list(sorted(df.columns)), axis=1)
df.to_csv(out_path, index=False)
