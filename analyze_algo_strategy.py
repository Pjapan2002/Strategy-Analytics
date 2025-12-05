import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
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


def get_pnls_for_client(client_df, exchange):
    data = client_df.loc[client_df['exchange'] == exchange, ["t_realizedPnl", "t_m2mPnl", "t_netPnl"]]

    return data.iloc[0].to_dict() if not data.empty else {
        "t_realizedPnl": 0, "t_m2mPnl": 0, "t_netPnl": 0
    }
    

def get_pnls_for_ctcl(ctcl_df, exchange):
    data = ctcl_df[ctcl_df["exchange"] == exchange].groupby("ctcl").agg(
        realizedPnl=("realizedPnl", "sum"),
        m2mPnl=("m2mPnl", "sum"),
        netPnl=("netPnl", "sum")
    )

    return data.iloc[0].to_dict() if not data.empty else {
        "realizedPnl": 0, "m2mPnl": 0, "netPnl": 0
    }


def get_allDailyPnl_clientIdWise_data(dates):
    rows = []

    for date in dates:
        path = f"dailyPnl/dailyPnl_{date}.csv"
        df = pd.read_csv(path)
        df = df.loc[df['client_id'] != "OWN", :].reset_index(drop=True)

        client_ids = df['client_id'].unique().tolist()

        for client_id in client_ids:
            client_df = df[df['client_id'] == client_id]

            # for bse
            bsePnlData = get_pnls_for_client(client_df, "BSE")
            # if not client_df[client_df['exchange'] == "BSE"].empty:
            #     bsePnlData = (
            #         client_df.loc[client_df['exchange'] == "BSE", ["t_realizedPnl", "t_m2mPnl", "t_netPnl"]]
            #         .iloc[0]
            #         .to_dict()
            #     )
            # else:
            #     bsePnlData = {"t_realizedPnl": 0, "t_m2mPnl": 0, "t_netPnl": 0}

            # for nse
            nsePnlData = get_pnls_for_client(client_df, "NSE")
            # if not client_df[client_df['exchange'] == "NSE"].empty:
            #     nsePnlData = (
            #         client_df.loc[client_df['exchange'] == "NSE", ["t_realizedPnl", "t_m2mPnl", "t_netPnl"]]
            #         .iloc[0]
            #         .to_dict()
            #     )
            # else:
            #     nsePnlData = {"t_realizedPnl": 0, "t_m2mPnl": 0, "t_netPnl": 0}

            rows.append({
                'date': date,
                'client_id': client_id,
                # net
                'net_realizedPnl': round(bsePnlData['t_realizedPnl'] + nsePnlData['t_realizedPnl'], 3),

                'net_m2mPnl': round(bsePnlData['t_m2mPnl'] + nsePnlData['t_m2mPnl'], 3),

                'net_pnl': round(bsePnlData['t_netPnl'] + nsePnlData['t_netPnl'], 3),
                # bse
                'bse_net_realizedPnl': round(bsePnlData['t_realizedPnl'], 3),

                'bse_net_m2mPnl': round(bsePnlData['t_m2mPnl'], 3),

                'bse_net_pnl': round(bsePnlData['t_netPnl'], 3),
                # nse
                'nse_net_realizedPnl': round(nsePnlData['t_realizedPnl'], 3),

                'nse_net_m2mPnl': round(nsePnlData['t_m2mPnl'], 3),

                'nse_net_pnl': round(nsePnlData['t_netPnl'], 3)
            })

    return rows


def get_allDailyPnl_ctclWise_data(dates):
    rows = []

    for date in dates:
        path = f"dailyPnl/dailyPnl_{date}.csv"
        df = pd.read_csv(path)
        df = df.loc[df['client_id'] != "OWN", :].reset_index(drop=True)
        # df = df.loc[df['client_id'] != "OWN", :].reset_index(drop=True)

        # client_ids = df['client_id'].unique().tolist()
        ctcls = df['ctcl'].unique().tolist()

        for ctcl in ctcls:
            ctcl_df = df.loc[df['ctcl'] == ctcl, :].reset_index(drop=True)

            # for bse
            bsePnlData = get_pnls_for_ctcl(ctcl_df, "BSE")
            # if not ctcl_df[ctcl_df['exchange'] == "BSE"].empty:
            #     # bsePnlData = (
            #     #     ctcl_df.loc[ctcl_df['exchange'] == "BSE", ["t_realizedPnl", "t_m2mPnl", "t_netPnl"]]
            #     #     .iloc[0]
            #     #     .to_dict()
            #     # )
            #     # bsePnlData = (
            #     #     ctcl_df.loc[ctcl_df['exchange'] == "BSE", ["ctcl", "realizedPnl", "m2mPnl", "netPnl"]]
            #     # ).reset_index(drop=True)
            #     # bsePnlData = (
            #     #     bsePnlData.groupby('ctcl')
            #     #     .agg([
            #     #         ("realizedPnl", "sum"),
            #     #         ("m2mPnl", "sum"),
            #     #         ("netPnl", "sum")
            #     #     ])
            #     #     .reset_index(drop=True)
            #     #     .iloc[0]
            #     #     .to_dict()
            #     # )
            #     # bsePnlData = get_pnls_for_ctcl(ctcl_df, "BSE")
            # else:
            #     bsePnlData = {"realizedPnl": 0, "m2mPnl": 0, "netPnl": 0}

            # for nse
            nsePnlData = get_pnls_for_ctcl(ctcl_df, "NSE")
            # if not ctcl_df[ctcl_df['exchange'] == "NSE"].empty:
            #     # nsePnlData = (
            #     #     ctcl_df.loc[ctcl_df['exchange'] == "NSE", ["ctcl", "realizedPnl", "m2mPnl", "netPnl"]]
            #     #     .iloc[0]
            #     #     .to_dict()
            #     # )
            #     nsePnlData = get_pnls_for_ctcl(ctcl_df, "NSE")
            # else:
            #     nsePnlData = {"realizedPnl": 0, "m2mPnl": 0, "netPnl": 0}

            rows.append({
                'date': date,
                'ctcl': ctcl,
                # net
                'net_realizedPnl': round(bsePnlData['realizedPnl'] + nsePnlData['realizedPnl'], 3),

                'net_m2mPnl': round(bsePnlData['m2mPnl'] + nsePnlData['m2mPnl'], 3),

                'net_pnl': round(bsePnlData['netPnl'] + nsePnlData['netPnl'], 3),
                # bse
                'bse_net_realizedPnl': round(bsePnlData['realizedPnl'], 3),

                'bse_net_m2mPnl': round(bsePnlData['m2mPnl'], 3),

                'bse_net_pnl': round(bsePnlData['netPnl'], 3),
                # nse
                'nse_net_realizedPnl': round(nsePnlData['realizedPnl'], 3),

                'nse_net_m2mPnl': round(nsePnlData['m2mPnl'], 3),

                'nse_net_pnl': round(nsePnlData['netPnl'], 3)
            })

    return rows


def generate_dailyPnl_report(data):
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["date"]).reset_index(drop=True)
    return df

def get_dailyPnl_for_client(df, client_id="AW11"):
    df = df.loc[df["client_id"] == client_id].reset_index(drop=True)
    return df
    # print(df.head())

def get_dailyPnl_for_ctcl(df, ctcl=1111111111111):
    df = df.loc[df['ctcl'] == ctcl].reset_index(drop=True)
    return df
    # print(df.head)

def get_dailyPnl_for_multipleClients(df, client_ids=["AW11"]):
    df = df.loc[df["client_id"].isin(client_ids)].reset_index(drop=True)
    # print(df.head())
    return df

def get_dailyPnl_for_multipleCTCLs(df, ctcls=[1111111111111]):
    df = df.loc[df["ctcl"].isin(ctcls)].reset_index(drop=True)
    # print(df.head())
    return df

def get_topN_highestEarn_clientIds(df):
    _df = df.groupby(["date", "client_id"]).agg(
        realizedPnl=("net_realizedPnl", "sum"),
        m2mPnl=("net_m2mPnl", "sum"),
        netPnl=("net_pnl", "sum"),
        # nse
        nse_realizedPnl=("nse_net_realizedPnl", "sum"),
        nse_m2mPnl=("nse_net_m2mPnl", "sum"),
        nse_netPnl=("nse_net_pnl", "sum"),
        # bse
        bse_realizedPnl=("bse_net_realizedPnl", "sum"),
        bse_m2mPnl=("bse_net_m2mPnl", "sum"),
        bse_netPnl=("bse_net_pnl", "sum")
    )
    _df = df.sort_values(by="t_netPnl").reset_index(drop=True)
    print(_df.head())

def get_topN_highestEarn_ctclIds(df):
    _df = df.groupby(["date", "ctcl"]).agg(
        realizedPnl=("realizedPnl", "sum"),
        m2mPnl=("m2mPnl", "sum"),
        netPnl=("netPnl", "sum")
    )
    _df = df.sort_values(by="netPnl").reset_index(drop=True)
    print(_df.head())


# def get_descriptive_summary(df, id):
#     # df = df.loc[df["client_id"] == client_id, 2:].reset_index(drop=True)
#     df = df.loc[df["client_id"] == client_id].reset_index(drop=True)
#     print(df.head())
#     # summary = df.describe()
#     # print(summary)

def analyze_daily_pnl(data):
    """
    data: list of dictionaries (your row list)
    Returns:
        summary: aggregated metrics
        client_stats: metrics per client
        daily_stats: metrics per date
        rolling: rolling performance (7-day, 30-day)
    """

    # -----------------------------
    # Convert list of dicts to flat table
    # -----------------------------
    # records = []
    # for r in data:
    #     records.append({
    #         "date": r["date"],
    #         "client_id": r["client_id"],

    #         "net_realizedPnl": r["net_realizedPnl"],
    #         "net_m2mPnl": r["net_m2mPnl"],
    #         "net_pnl": r["net_pnl"],

    #         "bse_pnl": r["BSE"]["net_pnl"],
    #         "nse_pnl": r["NSE"]["net_pnl"],
    #     })

    # df = pd.DataFrame(data)
    # df["date"] = pd.to_datetime(df["date"])
    # df = df.sort_values(["client_id", "date"]).reset_index(drop=True)
    # df.to_csv("dailyPnl_report.csv", index=False)
    # print(df.info())
    # print(" * "*10)
    # print(df.head())
    # print(" * "*10)
    # print(df.tail())

    # -----------------------------
    # 1) Summary metrics (overall)
    # -----------------------------
    # summary = {
    #     "total_net_pnl": df["net_pnl"].sum(),
    #     "total_realized": df["net_realizedPnl"].sum(),
    #     "total_m2m": df["net_m2mPnl"].sum(),
    #     "best_day": df.loc[df["net_pnl"].idxmax()].to_dict(),
    #     "worst_day": df.loc[df["net_pnl"].idxmin()].to_dict(),
    #     "profit_days": (df["net_pnl"] > 0).sum(),
    #     "loss_days": (df["net_pnl"] < 0).sum(),
    # }

    # -----------------------------
    # 2) PnL per client
    # -----------------------------
    # client_stats = df.groupby("client_id").agg({
    #     "net_pnl": ["sum", "mean", "max", "min"],
    #     "net_realizedPnl": "sum",
    #     "net_m2mPnl": "sum"
    # })
    # client_stats.columns = ["_".join(c) for c in client_stats.columns]

    # -----------------------------
    # 3) PnL per day (aggregate)
    # -----------------------------
    # daily_stats = df.groupby("date").agg({
    #     "net_pnl": "sum",
    #     "bse_pnl": "sum",
    #     "nse_pnl": "sum"
    # })

    # -----------------------------
    # 4) Rolling analysis
    # -----------------------------
    # daily_stats["rolling_7d"] = daily_stats["net_pnl"].rolling(7).sum()
    # daily_stats["rolling_30d"] = daily_stats["net_pnl"].rolling(30).sum()

    # -----------------------------
    # 5) Identify streaks
    # -----------------------------
    # df["win"] = df["net_pnl"] > 0
    # df["loss"] = df["net_pnl"] < 0

    # df["win_streak"] = df.groupby("client_id")["win"].cumsum() - \
    #                    df.groupby("client_id")["win"].cumsum().where(~df["win"]).ffill().fillna(0)

    # df["loss_streak"] = df.groupby("client_id")["loss"].cumsum() - \
    #                     df.groupby("client_id")["loss"].cumsum().where(~df["loss"]).ffill().fillna(0)

    # return {
    #     "df": df,
    #     "summary": summary,
    #     "client_stats": client_stats,
    #     "daily_stats": daily_stats,
    #     "rolling": daily_stats[["rolling_7d", "rolling_30d"]],
    # }

def ___plot_daywise_pnl(df):
    # Ensure date is datetime
    print("plot-daywise-pnl")
    df["date"] = pd.to_datetime(df["date"])

    plt.figure(figsize=(12, 6))

    # Bar charts
    plt.bar(df["date"], df["bse_net_pnl"])
    plt.bar(df["date"], df["nse_net_pnl"], bottom=df["bse_net_pnl"])

    # --- Move X-axis to Y=0 ---
    ax = plt.gca()
    ax.spines["bottom"].set_position(("data", 0))  # X-axis at PnL = 0

    # Optional cleanup
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xlabel("Date")
    plt.ylabel("PnL")
    plt.title("Daywise PnL (Segmented: BSE + NSE)")
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.show()

def plot___daywise_pnl(df, outfile="daywise_pnl.png"):
    df["date"] = pd.to_datetime(df["date"])

    plt.figure(figsize=(12, 6))

    # Bars
    plt.bar(df["date"], df["bse_net_pnl"])
    plt.bar(df["date"], df["nse_net_pnl"], bottom=df["bse_net_pnl"])

    # X-axis at y=0
    ax = plt.gca()
    ax.spines["bottom"].set_position(("data", 0))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xlabel("Date")
    plt.ylabel("PnL")
    plt.title("Daywise PnL (Segmented: BSE + NSE)")
    plt.xticks(rotation=45)

    plt.tight_layout()

    # Save to file instead of showing
    plt.savefig(outfile, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Chart saved: {outfile}")


def plot_daywise_pnl(df, outfile="_daywise_pnl.png"):
    df["date"] = pd.to_datetime(df["date"])

    # Colors for profit/loss
    def pnl_color(v):
        return "green" if v >= 0 else "red"

    bse_colors = [pnl_color(v) for v in df["bse_net_pnl"]]
    nse_colors = [pnl_color(v) for v in df["nse_net_pnl"]]

    plt.figure(figsize=(12, 6))

    # BSE bar
    plt.bar(df["date"], df["bse_net_pnl"], 
            color=bse_colors, label="BSE")

    # NSE stacked bar
    plt.bar(df["date"], df["nse_net_pnl"],
            bottom=df["bse_net_pnl"],
            color=nse_colors, label="NSE")

    ax = plt.gca()

    # Add labels inside bars
    for x, bse, nse in zip(df["date"], df["bse_net_pnl"], df["nse_net_pnl"]):
        # BSE label inside bar
        ax.text(
            x,
            bse / 2,
            f"{bse:.0f}",
            ha="center",
            va="center"
        )
        # NSE label inside stacked segment
        ax.text(
            x,
            bse + nse / 2,
            f"{nse:.0f}",
            ha="center",
            va="center"
        )

    # Move X-axis to y = 0
    ax.spines["bottom"].set_position(("data", 0))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xlabel("Date")
    plt.ylabel("PnL")
    plt.title("Daywise PnL: AW11")
    plt.xticks(rotation=45)
    plt.legend()

    plt.tight_layout()

    # Save plot (server-safe)
    plt.savefig(outfile, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Chart saved to: {outfile}")

def plot_cumulative_pnl(df, outfile="cumulative_pnl.png"):
    """
    df must contain columns:
    - date
    - net_pnl
    """

    # Ensure datetime
    df["date"] = pd.to_datetime(df["date"])

    # Sort by date
    df = df.sort_values("date").reset_index(drop=True)

    # Compute cumulative PnL
    df["cumulative_pnl"] = df["net_pnl"].cumsum()

    # Color logic: green if last value positive
    color = "green" if df["cumulative_pnl"].iloc[-1] >= 0 else "red"

    plt.figure(figsize=(12, 6))

    # Line plot
    plt.plot(df["date"], df["cumulative_pnl"], linewidth=2, color=color, marker="o")

    # Zero reference line
    plt.axhline(0, color="black", linewidth=1)

    # Labels
    for x, y in zip(df["date"], df["cumulative_pnl"]):
        plt.text(x, y, f"{y:.0f}", ha="center", va="bottom", fontsize=9)

    plt.xlabel("Date")
    plt.ylabel("Cumulative PnL")
    plt.title("Daywise Cumulative PnL")
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(outfile, dpi=300)
    plt.close()

    print(f"Cumulative PnL chart saved: {outfile}")


if __name__ == "__main__":
    try:
        # allDates = get_listOf_dates("sqOffPosition")
        # # # # print(allDates)
        # if not allDates:
        #     sys.exit()
        # data = get_allDailyPnl_clientIdWise_data(allDates)
        # # data = get_allDailyPnl_ctclWise_data(allDates)
        # # # print(data)
        # df = generate_dailyPnl_report(data)
        # # print(df.head())
        # # print(" * "*10)
        # # get_dailyPnl_for_ctcl(df)
        # c_df = get_dailyPnl_for_client(df, "AW11") 
        # print(c_df)
        # print(" * "*10)
        # plot_daywise_pnl(c_df)
        # plot_cumulative_pnl(c_df)

        while (True):
            print("""Hello, Hi!!!\n
            Welcome to Strategy-Analytics Dashboard!!!
            1. View DayWisePnl Report for all clients.
            2. Download DayWisePnl Report for all clients.
            3. View DayWisePnl Report for all ctcls.
            4. Download DayWisePnl Report for all ctcls.
            5. View the top clients who gain highest pnl.
            6. View the top highest earn ctcls.
            7. view the top highest mean
            """)
            op = int(input())

            match op:
                case 1:
                    print("OK")
                case 2:
                    print("Not Found")
                case 3:
                    print("Server Error")
                case 7:
                    print("exit!")
                    sys.exit()

    except Exception as ex:
        print("ex: ", ex)