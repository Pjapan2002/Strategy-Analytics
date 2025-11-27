import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime

HOST = "192.168.100.62"
USER = "sa"
PASSWORD = "sa.1"
DATABASE = "dropcopy"
PORT = "3306"


def get_mysql_connection(
    host: str,
    user: str,
    password: str,
    database: str,
    port: int = 3306
):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            autocommit=True
        )

        if connection.is_connected():
            print("✅ Connected to MySQL database")
            return connection
        else:
            print("❌ Failed to connect to MySQL")

    except Error as e:
        print(f"❌ MySQL connection error: {e}")
        return None

def fetch_data(connection, query, params=None):
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params or ())
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return None

def parse_nse_trade(data):
    # [Timestamp, Client_ID, nnfField, Trader_ID, Qty, TradePrice, Trans_Type]
    df = pd.DataFrame(data)
    # print(df.info())
    nseColumns = ["Timestamp", "Client_ID", "nnfField", "Scrip", "Trans_Type", "Qty", "TradePrice"]
    df = df.loc[:, nseColumns].reset_index(drop=True)
    # rename columns name
    df = df.rename(columns = {
        "Timestamp": "transactTime",
        "Client_ID": "client_id",
        "nnfField": "ctcl",
        "Scrip": "instrument_id",
        "Trans_Type": "side",
        "Qty": "qty",
        "TradePrice": "price"
    })
    df['client_id'] = df['client_id'].str.strip()
    df["price"] = df["price"] / 1e2
    df["exchange"] = "NSE"
    return df

def parse_bse_trade(data):
    # [TransactTime, FreeText1(ClientID), SenderLocationID, SecurityID, LastQty, LastPx, Side]
    df = pd.DataFrame(data)
    bseColumns = ["TransactTime", "FreeText1", "SenderLocationID", "SecurityID", "Side", "LastQty", "LastPx"]
    df = df.loc[:, bseColumns].reset_index(drop=True)
    df = df.rename(columns = {
        "TransactTime": "transactTime",
        "FreeText1": "client_id",
        "SenderLocationID": "ctcl",
        "SecurityID": "instrument_id",
        "Side": "side",
        "LastQty": "qty",
        "LastPx": "price"
    })
    df['client_id'] = df['client_id'].str.strip()
    df["transactTime"] = (
        pd.to_datetime(df["transactTime"].astype("int64"), unit="ns", utc=True)
        .dt.tz_convert("Asia/Kolkata")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )
    df["price"] = df["price"] / 1e8
    df["exchange"] = "BSE"
    return df

def get_dc_trades(connection, exchange):
    if exchange == "NSE":
        # fetch data
        data = fetch_data(connection, "SELECT * FROM tbl_trades_fo")
        # parse row data
        df = parse_nse_trade(data)
        return df
    elif exchange == "BSE":
        # fetch data
        data = fetch_data(connection, "SELECT * FROM tbl_trades_bsefo")
        # parse row data
        df = parse_bse_trade(data)
        return df
    else:
        print("Invaild exchange name!")
        return pd.DataFrame()

if __name__ == "__main__":
    try:
        date_str = "20251127"
        exchange = "NSE"
        engine = get_mysql_connection(host=HOST, user=USER, password=PASSWORD, database=DATABASE, port=PORT)
        # # BSE
        # df = get_dc_trades(engine, "BSE")
        # print(df.info())
        # print(df.head())

        # NSE
        df = get_dc_trades(engine, exchange)
        df.to_csv(f'dcTradeBook/DC_{exchange}_{date_str}.csv', index=False)
        # print(df.info())
        # print(df.head())
 
        # cursor = engine.cursor()
        # nse
        # data = fetch_data(engine, "SELECT * FROM tbl_trades_fo")
        # df = parse_nse_trade(data)
        # # bse
        # # data = fetch_data(engine, "SELECT * FROM tbl_trades_bsefo")
        # # df = parse_bse_trade(data)
        # print(df.info())
        # print("\n", "*"*10, "\n")
        # print(df.head())
        # # print(len(rows))
        # # for row in rows:
        # #     print(row)
    except Exception as ex:
        print(f"errors: ", ex)