from enum import Enum

class BhavCopyURLs(Enum):
    BSE = "https://www.bseindia.com/download/Bhavcopy/Derivative"
    BSE_BASE_URL = "https://www.bseindia.com"
    NSE = "https://nsearchives.nseindia.com/content/fo"
    NSE_BASE_URL = "https://www.nseindia.com"

class Exchange(Enum):
    BSE = "BSE"
    NSE = "NSE"

class TradeColumnName(Enum):
    transactTime = "transactTime" 
    client_id = "client_id" 
    ctcl = "ctcl"
    instrument_id = "instrument_id"
    side = "side" 
    qty = "qty" 
    price = "price"

if __name__ == "__main__":
    # urls = BhavCopyURLs()
    print(type(BhavCopyURLs.BSE.value))