import pandas as pd
import numpy as np

if __name__ == "__main__":
    try:
        # Load Daily pnl
        df = pd.read_csv("Daily_Pnl.csv")

        df = df.sort_values("Date")

        df["Cummlative_Pnl"] = df["Net_Pnl"].cumsum()