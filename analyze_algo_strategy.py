import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os
import re

def get_listOf_dates(directory, ascending=True):
    files = []
    
    pattern = re.compile(r"^(\d{8})\.zip$")

    for fname in os.listdir(directory):
        match = pattern.match(fname)
        if match:
            date_str = match.group(1)
            try:
                date_obj = datetime.strptime(date_str, "%Y%m%d")
                files.append((date_obj, fname, date_str))
            except ValueError:
                continue

    # Sort by the date
    files.sort(key=lambda x: x[0], reverse=not ascending)

    # return [f[1] for f in files]
    return [f[2] for f in files]


if __name__ == "__main__":
    try:
        allDates = get_listOf_dates("sqOffPosition")
        # print(dates)
        if not allDates:
            sys.exit()
        for date_str in allDates:
            pass
        # Load Daily pnl
        # df = pd.read_csv("Daily_Pnl.csv")

        # df = df.sort_values("Date")

        # df["Cummlative_Pnl"] = df["Net_Pnl"].cumsum()
    except Exception as ex:
        print("ex: ", ex)