# from redis import Redis, ConnectionError
# import pandas as pd
# import json
# import os
from utils import calculate_net_pnl, Load_trades_from_redis, download_bhavCopy, Load_bhavCopy
from datetime import datetime

now = datetime.now().strftime("%Y%m%d")

# REDIS_HOST = "localhost"
# REDIS_PORT = 6379
# DESTINATION_QUEUE = "SOURCE_QUEUE"
# CURRENT_DATE = str(now)
# PNL_FILENAME = "Daily_Pnl.csv"

if __name__ == "__main__":
    try:
        date_str = "20251117"

        # Download bhavCopy
        download_bhavCopy(date_str, "NSE")
        download_bhavCopy(date_str, "BSE")

        # Load trades from redis-server
        # trades_df = Load_trades_from_redis()

        # Load bhavCopy data
        # bhavCopy_df = Load_bhavCopy(date_str)

        # calculate daily net-pnl
        # pnl = calculate_net_pnl(trades_df = trades_df, bhav_df = bhavCopy_df)

        # print("*"*6, " Net Daily PNL ", "*"*6)
        # # print(pnl.Realized_df)
        # print(f"Realized pnl: {pnl["data"]["Net_Realized_Pnl"]}")
        # print(f"Unrealized pnl: {pnl["data"]["Net_M2M_Pnl"]}")
        # print(f"Net Daily Pnl: {pnl["data"]["Net_Pnl"]}")

        # print("( * )"*10)
        # # print(type(pnl["UnRealized_df"]))
        # print(pnl["Realized_df"].head())
        # print(" -*--*- "*10)
        # print(pnl["Realized_df"].tail())
    except Exception as e:
        print(f"exit due to {e}")


# if __name__ == "__main__":
#     try:
#         user_connection = Redis(host="localhost", port=6379)

#         if(user_connection.ping() == False):
#             raise ConnectionError("Redis server closed!!!")
#         print("Successfully connected to Redis!!!")

#         # get all trades from redis
#         allTrades = user_connection.lrange(DESTINATION_QUEUE, 0, -1)
#         # # print(allTrades[-1])
#         allTrades = [json.loads(rowByte.decode('utf-8')) for rowByte in allTrades]

#         # print(type(allTrades))
#         # print(type(allTrades[0]))
#         # print(len(allTrades))
#         # print(allTrades[-1])
#         # print(allTrades)

#         flat_records = []
#         for item in allTrades:
#             flat_record = item['data'].copy()
#             flat_record['exchange'] = item['exchange']
#             flat_records.append(flat_record)

#         _df = pd.DataFrame(flat_records)

#         # print(_df.iloc[0])
#         # _df.to_csv('allTrades.csv', index=False)

#         # print("unique values: ", len(_df["unique_id"].unique()))
#         # print("Rootpartyid: ", _df["RootPartyIDSessionID"].unique())
        
#         # df = _df[["SenderLocationID", "unique_id", "TransactTime", "SecurityID", "LastPx", "LastQty", "Side"]].copy()
#         df = _df[["TransactTime", "SecurityID", "LastPx", "LastQty", "Side"]].copy()

#         # Last price
#         # df["LastPx"] = pd.to_numeric(df["LastPx"], errors='coerce')
#         # df["LastPx"] = df["LastPx"].astype(float) / 1e8
#         # df.loc[:, "LastPx"] = pd.to_numeric(df["LastPx"], errors="coerce") / 1e8

#         df["TransactTime"] = df["TransactTime"].astype(str)
#         df["SecurityID"] = df["SecurityID"].astype(int)
#         df["LastPx"] = df["LastPx"].astype(float)
#         df["LastQty"] = df["LastQty"].astype(int)
#         df["Side"] = df["Side"].astype(int)

#         df.loc[:, "LastPx"] = df["LastPx"] / 1e8

#         print("*"*6, " DC-Trade-BOOK ", "*"*6)
#         print(f"Dataframe Shape: {df.shape}")

#         print("*"*6, " DC-Trade-Info ", "*"*6)
#         print(df.info())
#         print("*"*6, " DC-Trade-Book: top rows ", "*"*6)
#         print(df.head())

#         # print(df.columns)
#         # print(df.head())
#         # df.to_csv('allTrades.csv', index=False)

#         # Load bhav copy
#         # bhavCopy = pd.read_csv(f"BhavCopy_BSE_FO_0_0_0_{CURRENT_DATE}_F_0000.CSV")
#         # bhavCopy = pd.read_csv(f"BhavCopy_BSE_FO_0_0_0_20251111_F_0000.CSV")
#         bhavCopy = pd.read_csv(f"BhavCopy_20251111.csv")
#         # bhavCopy_df = bhavCopy[["FinInstrmId", "LastPric"]]
#         # bhavCopy_df = bhavCopy[["FinInstrmId", "ClsPric", "StrkPric", "OptnTp", "FinInstrmNm", ""]]
#         # bhavCopy_df = bhavCopy[["FinInstrmId", "ClsPric"]]
#         bhavCopy_df = bhavCopy[["FinInstrmId", "SttlmPric"]]
#         # bhavCopy_df = bhavCopy_df.rename(columns = {'FinInstrmId': 'SecurityID', 'ClsPric': 'ClosePrice', 'StrkPric': 'StrikePrice', 'OptnTp': 'OptionType', 'FinInstrmNm': 'TradingSymbol'})
#         bhavCopy_df = bhavCopy_df.rename(columns = {'FinInstrmId': 'SecurityID', 'SttlmPric': 'SttlmPrice'})

#         print("*"*6, " BhavCopy ", "*"*6)
#         print(bhavCopy_df.info())
#         print("*"*6, " BhavCopy-Info ", "*"*6)
#         print(bhavCopy_df.head())

#         pnl = calculate_final_pnl(trades_df=df, bhav_df=bhavCopy_df)

#         # pnldf = pd.DataFrame(pnl["data"])
#         # # save daily pnl
#         # if not os.path.isfile(PNL_FILENAME):
#         #     pnldf.to_csv(PNL_FILENAME, index=False)
#         # else:
#         #     pnldf.to_csv(PNL_FILENAME, mode='a', header=False, index=False)


#         print("*"*6, " Net Daily PNL ", "*"*6)
#         # print(pnl.Realized_df)
#         print(f"Realized pnl: {pnl["data"]["Net_Realized_Pnl"]}")
#         print(f"Unrealized pnl: {pnl["data"]["Net_M2M_Pnl"]}")
#         print(f"Net Daily Pnl: {pnl["data"]["Net_Pnl"]}")

#         print("( * )"*10)
#         # print(type(pnl["UnRealized_df"]))
#         print(pnl["Realized_df"])

#     except ConnectionError as e:
#         print(f"redis connection failed! Due to {e}")
#     except Exception as e:
#         print(f"unexpected errors occured! {e}")
