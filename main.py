import pandas as pd
from temp import get_mysql_connection, get_dc_trades
from utils import download_bhavCopy, Load_bhavCopy, calculate_net_pnl, get_clientWise_pnl, zip_and_remove
import os
from dotenv import load_dotenv

load_dotenv()

# pnl_dict = { 
#     'AW011_NSE': { 
#         'exchange': 'NSE', 
#         'client_id': 'AW011', 
#         'realizedPnl': 100, 
#         'm2mPnl': 150, 
#         'netPnl': 200, 
#         'ctclWisePnl': { 
#             'ctcl': { 
#                 'realizedPnl': 100, 
#                 'm2mPnl': 150, 
#                 'netPnl': 200 
#                 } 
#             } 
#         },
#     'Aw011_BSE': { 
#         'exchange': 'BSE', 
#         'client_id': 'AW011', 
#         'realizedPnl': 100, 
#         'm2mPnl': 150, 
#         'netPnl': 200, 
#         'ctclWisePnl': { 
#             'ctcl': { 
#                 'realizedPnl': 100, 
#                 'm2mPnl': 150, 
#                 'netPnl': 200 
#                 } 
#             } 
#         } 
#     }

def save_dayEnd_positions(date_str, exchange, realizedPnl_df_list, m2mPnl_df_list):
    os.makedirs(f"sqOffPosition/{date_str}", exist_ok=True)
    # os.makedirs(date_str, exist_ok=True)
    # for realizedPnl_dfs
    realizedPnl_df_list = [df for df in realizedPnl_df_list if not df.empty]
    realizedPnl_df_final = (
        pd.concat(realizedPnl_df_list, ignore_index=True) 
        if realizedPnl_df_list else pd.DataFrame()
    )
    realizedPnl_df_final.to_csv(f"sqOffPosition/{date_str}/realizedPnl_{exchange}_{date_str}.csv", index=False)
    # for m2mPnl_dfs
    m2mPnl_df_list = [df for df in m2mPnl_df_list if not df.empty]
    m2mPnl_df_final = (
        pd.concat(m2mPnl_df_list, ignore_index=True) 
        if m2mPnl_df_list else pd.DataFrame()
    )
    m2mPnl_df_final.to_csv(f"sqOffPosition/{date_str}/m2mPnl_{exchange}_{date_str}.csv", index=False)
    # # save zip files
    # zip_path = os.path.join("sqOffPosition", f"{date_str}_{exchange}")
    # shutil.make_archive(zip_path, "tar", "sqOffPosition")
    # shutil.make_archive(f"sqOffPosition/{date_str}", "zip", "sqOffPosition")


def run(engine, exchange, date_str, pnl_dict):
    os.makedirs('dcTradeBook', exist_ok=True)
    path = f'dcTradeBook/DC_{exchange}_{date_str}.csv'
    # get dc trades
    if os.path.isfile(path):
        df = pd.read_csv(path)
        print(f"DC TradeBook for {exchange} on {date_str} already exists. Loaded from file.")
    else:
        df = get_dc_trades(engine, exchange)
        df.to_csv(path, index=False)
    
    # download bhavCopy
    download_bhavCopy(date_str, exchange)

    # load bhavCopy
    bhavCopy_df = Load_bhavCopy(exchange, date_str)
    
    # sort trade by timestamp
    df['transactTime'] = pd.to_datetime(df['transactTime'])
    df = df.sort_values(by="transactTime")
    # if exchange == "NSE":
    #     df['transactTime'] = pd.to_datetime(df['transactTime'])
    #     df = df.sort_values(by="transactTime")
    # else:
    #     df['transactTime'] = pd.to_datetime(df['transactTime'])
    #     df = df.sort_values(by="transactTime", ascending=False)

    # fix ctcl values
    if exchange == "NSE" and len(str(df['ctcl'][0])) > 14:
        df['ctcl'] = df['ctcl'].astype(str).str[:-2]
    elif exchange == "BSE" and len(str(df['ctcl'][0])) > 14:
        df['ctcl'] = df['ctcl'].astype(str).str[:-3]
    # print(df.head(5))
    # print("*"*10)
    # print(df.tail(5))
    realizedPnl_df_list = []
    m2mPnl_df_list = []
    # calculate pnl per client_id
    client_Ids = df['client_id'].unique().tolist()
    # print("client_Ids: ", client_Ids)
    for client_id in client_Ids:
        client_df = df.loc[df['client_id'] == client_id, :].reset_index(drop=True)
        # get ctcls for the client_id
        ctcls = client_df['ctcl'].unique().tolist()
        print(f"ctcls: for client_id {client_id}: ", ctcls)
        ctclWisePnl = {}
        for ctcl in ctcls:
            ctcl_df = client_df.loc[client_df['ctcl'] == ctcl, :].reset_index(drop=True)
            # ctcl_df = client_df.loc[client_df['ctcl'] == ctcl, ['transactTime', 'instrument_id', 'side', 'qty', 'price']].reset_index(drop=True)
            # get pnl
            pnl = calculate_net_pnl(trades_df=ctcl_df, bhav_df=bhavCopy_df)
            ctclWisePnl[ctcl] = pnl["data"]
            # append realized and m2m pnl dataframes
            # realized pnl
            realizedPnl_df = pnl["Realized_df"]
            realizedPnl_df['client_id'] = client_id
            realizedPnl_df['ctcl'] = ctcl
            # unrealized pnl
            m2mPnl_df = pnl["UnRealized_df"]
            m2mPnl_df['client_id'] = client_id
            m2mPnl_df['ctcl'] = ctcl
            realizedPnl_df_list.append(realizedPnl_df)
            m2mPnl_df_list.append(m2mPnl_df)
        # get total pnl for client_id
        realizedPnl, m2mPnl, netPnl = get_clientWise_pnl(ctclWisePnl)
        unique_key = f"{client_id}_{exchange}"
        pnl_dict[unique_key] = {
            'exchange': exchange,
            'client_id': client_id,
            'realizedPnl': realizedPnl,
            'm2mPnl': m2mPnl,
            'netPnl': netPnl,
            'ctclWisePnl': ctclWisePnl.copy()
        }
        ctclWisePnl.clear()
    # now save
    save_dayEnd_positions(date_str, exchange, realizedPnl_df_list, m2mPnl_df_list)
    # realizedPnl_df_final = pd.concat(realizedPnl_df_list, ignore_index=True)
    # realizedPnl_df_list = [df for df in realizedPnl_df_list if not df.empty]
    # realizedPnl_df_final.to_csv(f"sqOffPosition/realizedPnl_{exchange}_{date_str}.csv", index=False)
    # m2mPnl_df_final = pd.concat(m2mPnl_df_list, ignore_index=True)
    # m2mPnl_df_final.to_csv(f"sqOffPosition/m2mPnl_{exchange}_{date_str}.csv", index=False)
    

def get_pnl_df(pnl_dict:dict):
    rows = []

    for key, d in pnl_dict.items():
        base = {
            "client_id": d["client_id"],
            "exchange": d["exchange"],
            "t_realizedPnl": d["realizedPnl"],
            "t_m2mPnl": d["m2mPnl"],
            "t_netPnl": d["netPnl"]
        }

        for ctcl, pnl in d["ctclWisePnl"].items():
            row = {**base, "ctcl": ctcl, **pnl}
            rows.append(row)
    print("rows: ", len(rows))
    df = pd.DataFrame(rows)
    return df
    

if __name__ == "__main__":
    try:
        date_str = "20251127"
        # exchange = "BSE"
        pnl_dict = {}
        # make connection with dc-database
        db_host = os.getenv("DB_HOST")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")
        db_port = os.getenv("DB_PORT")
        # get dc-database connection
        engine = get_mysql_connection(host=db_host, user=db_user, password=db_password, database=db_name, port=db_port)
    
        run(engine, "BSE", date_str, pnl_dict)
        run(engine, "NSE", date_str, pnl_dict)

        # save zip files
        zip_and_remove(f"sqOffPosition/{date_str}", f"sqOffPosition/{date_str}")
        # zip_path = os.path.join("sqOffPosition", f"{date_str}")
        # shutil.make_archive(zip_path, "tar", "sqOffPosition")

        # print(pnl_dict)
        pnl_df = get_pnl_df(pnl_dict)
        
        pnl_df.to_csv(f"dailyPnl/_dailyPnl_{date_str}.csv", index=False)
        # print(pnl_df.tail(20))
        # _df = pnl_df[pnl_df["client_id"] == "AW011"].copy()
        # print(_df.head(10))
    except Exception as ex:
        print("main loop closed! due to exception: ", str(ex))
