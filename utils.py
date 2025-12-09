import pandas as pd
from redis import Redis, ConnectionError
from dotenv import load_dotenv
from comman import Exchange, BhavCopyURLs
import json
import requests
import os
import zipfile
import shutil
import io
import sys

load_dotenv()

# load all trades from SOURCE_QUEUE of DC(bse)
def Load_trades_from_redis():
    try:
        Redis_Host = os.getenv("REDIS_HOST")
        Redis_Port = os.getenv("REDIS_PORT")
        DESTINATION_QUEUE = os.getenv("DESTINATION_QUEUE")
        # user_connection = Redis(
        #     host="localhost", 
        #     port=6379)
        user_connection = Redis(
            host=os.getenv("REDIS_HOST"),
            port=os.getenv("REDIS_PORT"))

        if(user_connection.ping() == False):
            raise ConnectionError("Redis server closed!!!")
        print("Successfully connected to Redis!!!")

        # get all trades from redis
        allTrades = user_connection.lrange(DESTINATION_QUEUE, 0, -1)
        allTrades = [json.loads(rowByte.decode('utf-8')) for rowByte in allTrades]

        flat_records = []
        for item in allTrades:
            flat_record = item['data'].copy()
            flat_record['exchange'] = item['exchange']
            flat_records.append(flat_record)

        _df = pd.DataFrame(flat_records)
        _df.to_csv('allTrades_20251112.csv', index=False)
        return
        # df = _df[["TransactTime", "SecurityID", "LastPx", "LastQty", "Side"]].copy()

        # # fixed the datatype for each columns
        # df["TransactTime"] = df["TransactTime"].astype(str)
        # df["SecurityID"] = df["SecurityID"].astype(int)
        # df["LastPx"] = df["LastPx"].astype(float)
        # df["LastQty"] = df["LastQty"].astype(int)
        # df["Side"] = df["Side"].astype(int)

        # df.loc[:, "LastPx"] = df["LastPx"] / 1e8

        # return df
    except ConnectionError as e:
        print(f"redis connection failed! Due to {e}")
        return None
    except Exception as e:
        print(f"unexpected errors occured! {e}")
        return None


# download bhavCopy
def download_bhavCopy(date_str:str, exchange:str):
    try:
        if exchange == Exchange.NSE.value:
            url = f"{BhavCopyURLs.NSE.value}/BhavCopy_{exchange}_FO_0_0_0_{date_str}_F_0000.csv.zip"
            base_url = BhavCopyURLs.NSE_BASE_URL.value
        elif exchange == Exchange.BSE.value:
            url = f"{BhavCopyURLs.BSE.value}/BhavCopy_{exchange}_FO_0_0_0_{date_str}_F_0000.CSV"
            base_url = BhavCopyURLs.BSE_BASE_URL.value
        
        # print("url: ", url, "\nbase-url: ", base_url)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": base_url,
        }

        os.makedirs("bhavCopys", exist_ok=True)

        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            file_path = f"bhavCopys/BhavCopy_{exchange}_FO_{date_str}.csv"
            if os.path.isfile(file_path):
                print(f"{exchange}-fo-bhavCopy_{date_str} allready downloaded!")
                return
            
            if exchange == Exchange.NSE.value:
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    csv_files = [name for name in z.namelist() if name.lower().endswith(".csv")]

                    if not csv_files:
                        print("No CSV file found inside ZIP")
                        return

                    csv_name = csv_files[0]
                    csv_bytes = z.read(csv_name)

                    with open(file_path, "wb") as f:
                        f.write(csv_bytes)
                    print(f"✅ NSE-fo-bhavCopy_{date_str} successfully download!")
            elif exchange == Exchange.BSE.value:
                with open(file_path, "wb") as f:
                    f.write(response.content)
                print(f"✅ BSE-fo-bhavCopy_{date_str} successfully download!")
        else:
            print(f"Failed with status code: {response.status_code} {exchange} for {date_str}")

    except requests.exceptions.RequestException as e:
        print(f"Network error while downloading {exchange}-BhavCopy for {date_str}: {e}")
    except OSError as e:
        print(f"File system error while saving {exchange}-BhavCopy for {date_str}: {e}")
    except Exception as e:
        print(f"Unexpected error occurred {exchange}-BhavCopy for {date_str}: {e}")



# load bhavCopy for that date
# def Load_bhavCopy(date_str):
#     bhavCopy = pd.read_csv(f"bhavCopys/BhavCopy_BSE_FO_{date_str}.csv")
#     # bhavCopy_df = bhavCopy[["FinInstrmId", "ClsPric"]]
#     bhavCopy_df = bhavCopy[["FinInstrmId", "SttlmPric"]]
#     # bhavCopy_df = bhavCopy_df.rename(columns = {'FinInstrmId': 'SecurityID', 'ClsPric': 'ClosePrice'})
#     bhavCopy_df = bhavCopy_df.rename(columns = {'FinInstrmId': 'SecurityID', 'SttlmPric': 'SttlmPrice'})
#     return bhavCopy_df

# def Load_bhavCopy(exchange, date_str):
#     bhavCopy = pd.read_csv(f"bhavCopys/BhavCopy_{exchange}_FO_{date_str}.csv")
#     if exchange == Exchange.NSE.value:
#         # bhavCopy = pd.read_csv(f"bhavCopys/BhavCopy_{exchange}_FO_{date_str}.csv")
#         # bhavCopy_df = bhavCopy[["FinInstrmId", "ClsPric"]]
#         bhavCopy_df = bhavCopy[["FinInstrmId", "SttlmPric"]]
#         bhavCopy_df = bhavCopy_df.rename(columns = {'FinInstrmId': 'SecurityID', 'SttlmPric': 'SttlmPrice'})
#         return bhavCopy_df
#     elif exchange == Exchange.BSE.value:
#         # bhavCopy = pd.read_csv(f"bhavCopys/BhavCopy_{exchange}_FO_{date_str}.csv")
#         # bhavCopy_df = bhavCopy[["FinInstrmId", "ClsPric"]]
#         bhavCopy_df = bhavCopy[["FinInstrmId", "SttlmPric"]]
#         bhavCopy_df = bhavCopy_df.rename(columns = {'FinInstrmId': 'SecurityID', 'SttlmPric': 'SttlmPrice'})
#         return bhavCopy_df
#     else:
#         print("Invaild exchange name!!!")

def Load_bhavCopy(exchange, date_str):
    try:
        bhavCopy = pd.read_csv(f"bhavCopys/BhavCopy_{exchange}_FO_{date_str}.csv")
        # reverse the date_str: YYYYMMDD -> DDMMYYYY, "20251125" -> "25112025"
        # new_date = date_str[6:8] + date_str[4:6] + date_str[0:4]
        # check for expiry date
        bhavCopy["isExpiryToday"] = bhavCopy.apply(
            lambda row: (row['XpryDt'].replace("-", "") == date_str) or (row['UndrlygPric'] == row['SttlmPric']), axis=1
        )
        # bhavCopy = bhavCopy.loc[
        #     bhavCopy['XpryDt'].astype(str).str.replace("-", "", regex=False) != date_str,
        #     :
        # ]
        # bhavCopy_df = bhavCopy.loc[:, ["FinInstrmId", "SttlmPric", "LastPric", "ClsPric", "isExpiryToday"]]
        # rename_dict = {
        #     'FinInstrmId': 'instrument_id',
        #     'SttlmPric': 'sttlmPrice',
        #     'LastPric': 'lastPrice',
        #     'ClsPric': 'closePrice',
        #     'isExpiryToday': 'isExpiryToday'
        # }
        bhavCopy_df = bhavCopy.loc[:, ["FinInstrmId", "SttlmPric", "OptnTp", "StrkPric", "isExpiryToday"]]
        rename_dict = {
            'FinInstrmId': 'instrument_id',
            'SttlmPric': 'sttlmPrice',
            'OptnTp': 'optionType',
            'StrkPric': 'strikePrice',
            'isExpiryToday': 'isExpiryToday'
        }
        # bhavCopy_df = bhavCopy_df.rename(columns = {'FinInstrmId': 'instrument_id', 'SttlmPric': 'sttlmPrice', 'isExpiryToday': 'isExpiryToday'})
        bhavCopy_df = bhavCopy_df.rename(columns = rename_dict)
        bhavCopy_df['optionType'] = bhavCopy_df['optionType'].fillna('FUT')
        bhavCopy_df['strikePrice'] = bhavCopy_df['strikePrice'].fillna(0)
        return bhavCopy_df
    except Exception as ex:
        print(f"bhavCopy loading failed! due to {ex}")

def get_contractInfo(bhav_df, instrument_id):
    row = bhav_df.loc[bhav_df['instrument_id'] == instrument_id,
                ["sttlmPrice", "optionType", "strikePrice", "isExpiryToday"]
                ].iloc[0].to_dict()
    return row

def calc_charges(buy_price, sell_price, qty, segment="OPTIONS", exchange="NSE"):
    turnover = (buy_price * qty) + (sell_price * qty)
    
    # 1) BROKERAGE
    if segment == "FUTURE":
        # 0.03% or Rs 20 per executed order whichever is lower
        brokerage_buy  = min(0.0003 * buy_price  * qty, 20)
        brokerage_sell = min(0.0003 * sell_price * qty, 20)
        brokerage = brokerage_buy + brokerage_sell
    elif segment == "OPTIONS":
        # 20 per executed order (buy + sell)
        brokerage = 40

    # 2) STT
    if segment == "FUTURE":
        stt = int(0.0002 * sell_price * qty)
    elif segment == "OPTIONS":
        stt = int(0.001 * sell_price * qty)

    # 3) TRANSACTION CHARGES
    if exchange == "NSE":
        if segment == "FUTURE":
            txn_rate = 0.0000173
        elif segment == "OPTIONS":
            txn_rate = 0.0003503
    else:  # BSE
        if segment == "FUTURE":
            txn_rate = 0
        elif segment == "OPTIONS":
            txn_rate = 0.000325
    transaction_charges = txn_rate * turnover

    # 4) SEBI FEES
    # ₹10 per crore
    sebi_charges = (10 / 1e7) * turnover

    # 5) STAMP DUTY (only BUY side)
    if segment == "FUTURE":
        stamp = 0.00002 * buy_price * qty
    elif segment == "OPTIONS":
        stamp = 0.00003 * buy_price * qty

    # 6) GST (18% of brokerage + seb + txn)
    gst = 0.18 * (brokerage + sebi_charges + transaction_charges)

    # TOTAL CHARGES
    total_charges = brokerage + stt + transaction_charges + sebi_charges + stamp + gst

    gross_pnl = (sell_price - buy_price) * qty
    net_pnl = gross_pnl - total_charges

    return {
        "gross_pnl": round(gross_pnl, 2),
        "total_charges": round(total_charges, 2),
        "net_pnl": round(net_pnl, 2),
        "charges": {
            "brokerage": round(brokerage, 2),
            "stt": stt,
            "transaction": round(transaction_charges, 2),
            "sebi": round(sebi_charges, 2),
            "stamp": round(stamp, 2),
            "gst": round(gst, 2),
        }
    }


def get_charge(df, exchange):

    if df.empty:
        return df.assign(txn=[], net_pnl=[])

    def compute_row(row):
        segment = "FUTURE" if row.optionType == "FUT" else "OPTIONS"
        txn = calc_charges(row.buyPrice, row.sellPrice, row.matchedQty, segment, exchange)

        total_charge = txn["total_charges"]
        realized = row.get("realizedPnL", 0)

        return pd.Series([total_charge, realized - total_charge])

    df[["txn", "net_pnl"]] = df.apply(compute_row, axis=1).values
    return df

# calculate net daily-pnl
def calculate_net_pnl(trades_df, bhav_df, prevOpenPosition_df, exchange):
    # sys.exit()
    # sort trades by transaction-time
    # trades_df['transactTime'] = pd.to_datetime(trades_df['transactTime'])
    # trades_df = trades_df.sort_values(by="transactTime")

    # update the overnight position price with prev.day close-price
    # print(prevOpenPosition_df)
    # sys.exit()
    if not prevOpenPosition_df.empty:
        for _, prevPos in prevOpenPosition_df.iterrows():
            matchPos = trades_df[(trades_df['instrument_id'] == prevPos['instrument_id']) & (trades_df['qty'] == prevPos['matchedQty'])]
            if not matchPos.empty:
                posIndex = matchPos.index[0]
                trades_df.at[posIndex, 'price'] = prevPos['sellPrice']
    
    # FIFO queue: {secID: [[qty, price], ...]}
    open_positions = {}
    realized_trades = []
    unrealized_trades = []

    # 1: calculate realized PnL by matching qty
    for _, trade in trades_df.iterrows():
        sec = trade['instrument_id']
        qty = trade['qty']
        px = trade['price']
        side = trade['side']
        
        if sec not in open_positions:
            open_positions[sec] = []

        if side == 1: # BUY
            if len(open_positions[sec]) == 0:
                open_positions[sec].append([qty, px, side])
                continue
            
            open_qty, open_px, open_side = open_positions[sec][0]

            if open_side == 1: # buy (add more)
                open_positions[sec].append([qty, px, side])

            if open_side == 2: # open-pos:sell (sq.off short position)
                buy_qty = qty
                pnl = 0
                while buy_qty > 0:
                    if len(open_positions[sec]) == 0:
                        open_positions[sec].append([buy_qty, px, side])
                        buy_qty = 0
                        break
                    match_qty = min(open_qty, buy_qty)

                    pnl += (open_px - px) * match_qty

                    open_qty -= match_qty
                    buy_qty -= match_qty

                    if open_qty == 0:
                        open_positions[sec].pop(0)
                        if buy_qty != 0 and len(open_positions[sec]) > 0:
                            open_qty, open_px, open_side = open_positions[sec][0]
                    else:
                        open_positions[sec][0][0] = open_qty
                # append realized pnl
                row = get_contractInfo(bhav_df, sec)
                # realized_trades.append([sec, match_qty, open_px, px, pnl, "short"])
                # realized_trades.append([sec, match_qty, open_px, px, pnl, "short", row['strikePrice'], row['optionType']])
                realized_trades.append([sec, match_qty, px, open_px, pnl, "short", row['strikePrice'], row['optionType']])

        if side == 2: # SELL
            if len(open_positions[sec]) == 0:
                open_positions[sec].append([qty, px, side])
                continue
            
            open_qty, open_px, open_side = open_positions[sec][0]

            if open_side == 2: # sell (add more)
                open_positions[sec].append([qty, px, side])

            if open_side == 1: # open-pos:buy (sq.off long position)
                sell_qty = qty
                pnl = 0
                while sell_qty > 0:
                    if len(open_positions[sec]) == 0:
                        open_positions[sec].append([sell_qty, px, side])
                        sell_qty = 0
                        break
                    match_qty = min(open_qty, sell_qty)

                    pnl += (px - open_px) * match_qty

                    open_qty -= match_qty
                    sell_qty -= match_qty

                    if open_qty == 0:
                        open_positions[sec].pop(0)
                        if sell_qty != 0 and len(open_positions[sec]) > 0:
                            open_qty, open_px, open_side = open_positions[sec][0]
                    else:
                        open_positions[sec][0][0] = open_qty
                # append realized pnl
                row = get_contractInfo(bhav_df, sec)
                realized_trades.append([sec, match_qty, open_px, px, pnl, "long", row['strikePrice'], row['optionType']])

    realized_df = pd.DataFrame(
        realized_trades,
        # columns=["instrument_id", "matchedQty", "buyPrice", "sellPrice", "realizedPnL", "position"]
        columns=["instrument_id", "matchedQty", "buyPrice", "sellPrice", "realizedPnL", "position", "strike", "optionType"]
    )

    # 2: calculate M2M-PnL using BhavCopy Close Price
    for sec, pos_list in open_positions.items():
        if pos_list:
            # print(pos_list)
            total_qty = sum(q for q, p, s in pos_list)
            avg_price = sum(q * p for q, p, s in pos_list) / total_qty
            open_side = pos_list[0][-1]
            # sq.off position dayend position with sttlmPrice

            # close_price = bhav_df.loc[bhav_df['instrument_id'] == sec, 'sttlmPrice']
            # close_price_df = bhav_df.loc[bhav_df['instrument_id'] == sec, ["sttlmPrice", "lastPrice", "closePrice", "isExpiryToday"]]
            # close_price_df = bhav_df.loc[
            #     bhav_df['instrument_id'] == sec,
            #     ["sttlmPrice", "lastPrice", "closePrice", "isExpiryToday"]
            #     ].iloc[0].to_dict()
            close_price_df = bhav_df.loc[
                bhav_df['instrument_id'] == sec,
                ["sttlmPrice", "optionType", "strikePrice", "isExpiryToday"]
                ].iloc[0].to_dict()
            if close_price_df["isExpiryToday"]:
                # close_price = close_price_df["closePrice"]
                if close_price_df['optionType'] == 'CE':
                    close_price = max(close_price_df["sttlmPrice"] - close_price_df["strikePrice"], 0)
                elif close_price_df['optionType'] == 'PE':
                    close_price = max(close_price_df["strikePrice"] - close_price_df["sttlmPrice"], 0)
                elif close_price_df['optionType'] == 'FUT':
                    close_price = close_price_df["sttlmPrice"]
            else:
                close_price = close_price_df["sttlmPrice"]

            if open_side == 1: # closed long position
                m2m_pnl = (close_price - avg_price) * total_qty
                unrealized_trades.append([sec, total_qty, avg_price, close_price, m2m_pnl, "long", close_price_df['strikePrice'], close_price_df['optionType']])
            if open_side == 2: # closed short position
                m2m_pnl = (avg_price - close_price ) * total_qty
                unrealized_trades.append([sec, total_qty, close_price, avg_price, m2m_pnl, "short", close_price_df['strikePrice'], close_price_df['optionType']])

    unrealized_df = pd.DataFrame(
        unrealized_trades,
        # columns=["instrument_id", "matchedQty", "buyPrice", "sellPrice", "m2mPnl", "position"]
        columns=["instrument_id", "matchedQty", "buyPrice", "sellPrice", "m2mPnl", "position", "strike", "optionType"]
    )

    # add charges
    realized_df = get_charge(realized_df, exchange)
    unrealized_df = get_charge(unrealized_df, exchange)

    # get realized pnl for each instrument_ids
    realized_df = realized_df.loc[realized_df["matchedQty"] != 0, :].reset_index(drop=True)
    # realizedPnl
    __realized_pnl = realized_df.groupby("instrument_id")["realizedPnL"].sum().reset_index()
    net_realized_pnl = __realized_pnl["realizedPnL"].sum()
    # print("net_realized_pnl: ", net_realized_pnl)
    # net_realizedPnl
    __realized_pnl = realized_df.groupby("instrument_id")["net_pnl"].sum().reset_index()
    with_txn_net_realized_pnl = __realized_pnl["net_pnl"].sum()
    # print("with_txn_net_realized_pnl: ", with_txn_net_realized_pnl)
    # m2mpnl & netM2mpnl
    net_m2m_pnl = unrealized_df["m2mPnl"].sum()
    with_txn_net_m2m_pnl = unrealized_df["net_pnl"].sum()
    # print("with_txn_net_m2m_pnl: ", with_txn_net_m2m_pnl)
    # net pnl with txn
    net_pnl = net_realized_pnl + net_m2m_pnl
    with_txn_net_pnl = with_txn_net_realized_pnl + with_txn_net_m2m_pnl
    # print("with_txn_net_pnl: ", with_txn_net_pnl)
    return {
        "Realized_df": realized_df,
        "UnRealized_df": unrealized_df,
        "data": {
            "realizedPnl": net_realized_pnl,
            "withTxnRealizedPnl": with_txn_net_realized_pnl,
            "m2mPnl": net_m2m_pnl,
            "withTxnM2MPnl": with_txn_net_m2m_pnl,
            "netPnl": net_pnl,
            "withTxnNetPnl": with_txn_net_pnl
        }
    }


# def get_clientWise_pnl(ctclWisePnl:dict):
#     realizedPnl, m2mPnl, netPnl = 0, 0, 0
#     for ctcl in ctclWisePnl.values():
#         # print(ctcl, "\n")
#         realizedPnl += float(ctcl['realizedPnl'])
#         m2mPnl += float(ctcl['m2mPnl'])
#         netPnl += float(ctcl['netPnl'])
#     # print("realizedPnl: ", realizedPnl)
#     return realizedPnl, m2mPnl, netPnl

def get_clientWise_pnl(ctclWisePnl:dict):
    realizedPnl, withTxnRealizedPnl, m2mPnl, withTxnM2MPnl, netPnl, withTxnNetPnl = 0, 0, 0, 0, 0, 0
    for ctcl in ctclWisePnl.values():
        # print(ctcl, "\n")
        realizedPnl += float(ctcl['realizedPnl'])
        withTxnRealizedPnl += float(ctcl['withTxnRealizedPnl'])
        m2mPnl += float(ctcl['m2mPnl'])
        withTxnM2MPnl += float(ctcl['withTxnM2MPnl'])
        netPnl += float(ctcl['netPnl'])
        withTxnNetPnl += float(ctcl['withTxnNetPnl'])
    # print("realizedPnl: ", realizedPnl)
    return realizedPnl, withTxnRealizedPnl, m2mPnl, withTxnM2MPnl, netPnl, withTxnNetPnl

# zip & remove directory
def zip_and_remove(directory_path, output_zip_name):
    # 1. Create ZIP file
    zip_file = shutil.make_archive(output_zip_name, 'zip', directory_path)
    print(f"Created ZIP: {zip_file}")

    # 2. Remove the original directory
    shutil.rmtree(directory_path)
    print(f"Removed directory: {directory_path}")

# read csv file data from zip file
def read_zip_data(zip_path, file_name):
    with zipfile.ZipFile(zip_path) as z:
        with z.open(file_name) as f:
            df = pd.read_csv(f)
    return df

# # calculate net daily-pnl
# def calculate_net_pnl(trades_df, bhav_df):
#     # sort trades by transaction-time
#     trades_df['TransactTime'] = pd.to_datetime(trades_df['TransactTime'])
#     trades_df = trades_df.sort_values(by="TransactTime")

#     # update the overnight position price with prev.day close-price
#     # if not unrealized_df.empty:
#     #     for _, prevPos in unrealized_df.iterrows():
#     #         matchPos = trades_df[(trades_df['SecurityID'] == prevPos['SecurityID']) & (trades_df['LastQty'] == prevPos['OpenQty'])]
#     #         if not matchPos.empty:
#     #             posIndex = matchPos.index[0]
#     #             trades_df.at[posIndex, 'LastPx'] = prevPos['ClosePx']


#     # FIFO queue: {secID: [[qty, price], ...]}
#     open_positions = {}
#     realized_trades = []
#     unrealized_trades = []

#     # 1: calculate realized PnL by matching qty
#     for _, trade in trades_df.iterrows():
#         sec = trade['SecurityID']
#         qty = trade['LastQty']
#         px = trade['LastPx']
#         side = trade['Side']

#         if sec not in open_positions:
#             open_positions[sec] = []

#         if side == 1: # BUY
#             if len(open_positions[sec]) == 0:
#                 open_positions[sec].append([qty, px, side])
            
#             open_qty, open_px, open_side = open_positions[sec][0]

#             if open_side == 1: # buy (add more)
#                 open_positions[sec].append([qty, px, side])

#             if open_side == 2: # open-pos:sell (sq.off short position)
#                 buy_qty = qty
#                 pnl = 0
#                 while buy_qty > 0:
#                     if len(open_positions[sec]) == 0:
#                         open_positions[sec].append([buy_qty, px, side])
#                         buy_qty = 0
#                         break
#                     # open_qty, open_px, open_side = open_positions[sec][0]
#                     match_qty = min(open_qty, buy_qty)

#                     pnl += (open_px - px) * match_qty
#                     # realized_trades.append([sec, match_qty, open_px, px, pnl])
#                     realized_trades.append([sec, match_qty, open_px, px, pnl, "short"])

#                     open_qty -= match_qty
#                     buy_qty -= match_qty

#                     if open_qty == 0: 
#                         open_positions[sec].pop(0)
#                     else:
#                         open_positions[sec][0][0] = open_qty

#         if side == 2: # SELL
#             if len(open_positions[sec]) == 0:
#                 open_positions[sec].append([qty, px, side])
            
#             open_qty, open_px, open_side = open_positions[sec][0]

#             if open_side == 2: # sell (add more)
#                 open_positions[sec].append([qty, px, side])

#             if open_side == 1: # open-pos:buy (sq.off long position)
#                 sell_qty = qty
#                 pnl = 0
#                 while sell_qty > 0:
#                     if len(open_positions[sec]) == 0:
#                         open_positions[sec].append([sell_qty, px, side])
#                         sell_qty = 0
#                         break
#                     match_qty = min(open_qty, sell_qty)

#                     pnl += (px - open_px) * match_qty
#                     # realized_trades.append([sec, match_qty, open_px, px, pnl])
#                     realized_trades.append([sec, match_qty, open_px, px, pnl, "long"])

#                     open_qty -= match_qty
#                     sell_qty -= match_qty

#                     if open_qty == 0:
#                         open_positions[sec].pop(0)
#                     else:
#                         open_positions[sec][0][0] = open_qty

#     realized_df = pd.DataFrame(
#         realized_trades,
#         # columns=["SecurityID", "MatchedQty", "BuyPx", "SellPx", "RealizedPnL"]
#         columns=["SecurityID", "MatchedQty", "BuyPx", "SellPx", "RealizedPnL", "Position"]
#     )

#     # 2: calculate M2M-PnL using BhavCopy Close Price
#     for sec, pos_list in open_positions.items():
#         if pos_list:
#             # print(pos_list)
#             total_qty = sum(q for q, p, s in pos_list)
#             avg_price = sum(q * p for q, p, s in pos_list) / total_qty
#             open_side = pos_list[0][-1]

#             # close_price = bhav_df.loc[bhav_df['SecurityID'] == sec, 'ClosePrice']
#             close_price = bhav_df.loc[bhav_df['SecurityID'] == sec, 'SttlmPrice']
#             if len(close_price) == 0:
#                 continue
#             close_price = close_price.values[0]

#             if open_side == 1: # closed long position
#                 m2m_pnl = (close_price - avg_price) * total_qty
#                 unrealized_trades.append([sec, total_qty, avg_price, close_price, m2m_pnl, "long"])
#             if open_side == 2: # closed short position
#                 m2m_pnl = (avg_price - close_price ) * total_qty
#                 unrealized_trades.append([sec, total_qty, close_price, avg_price, m2m_pnl, "short"])

#             # unrealized_trades.append([sec, total_qty, avg_price, close_price, m2m_pnl])

#     unrealized_df = pd.DataFrame(
#         unrealized_trades,
#         # columns=["SecurityID", "OpenQty", "AvgOpenPx", "ClosePx", "UnrealizedPnL"]
#         # columns=["SecurityID", "OpenQty", "AvgOpenPx", "ClosePx", "m2mPnl"]
#         columns=["SecurityID", "MatchedQty", "BuyPx", "SellPx", "m2mPnl", "Position"]
#     )

#     # get realized pnl for each SecurityIds
#     __realized_pnl = realized_df.groupby("SecurityID")["RealizedPnL"].sum().reset_index()
#     net_realized_pnl = __realized_pnl["RealizedPnL"].sum()
#     net_m2m_pnl = unrealized_df["m2mPnl"].sum()
#     net_pnl = net_realized_pnl + net_m2m_pnl

#     return {
#         "Realized_df": realized_df,
#         "UnRealized_df": unrealized_df,
#         "data": {
#             "Net_Realized_Pnl": net_realized_pnl,
#             "Net_M2M_Pnl": net_m2m_pnl,
#             "Net_Pnl": net_pnl
#         }
#     }


# save daily pnl summary
def save_daily_pnl_summary(unrealized_df, realized_df):
    _df = unrealized_df.groupby(['SecurityID']).agg(
        MatchedQty=('MatchedQty', 'sum'),
        BuyPx=('BuyPx', 'mean'),
        SellPx=('SellPx', 'mean'),
        m2mPnl=('m2mPnl', 'sum'),
        Position=('Position', 'first')
    )
    