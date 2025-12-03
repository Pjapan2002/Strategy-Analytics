import requests
import datetime
import zipfile
import io
import pandas as pd

# date = datetime.date.today()
# date_str = date.strftime("%Y%m%d")
date_str = "20251117"

# url = f"https://www.bseindia.com/download/Bhavcopy/Derivative/BhavCopy_BSE_FO_0_0_0_{date_str}_F_0000.CSV"
url = f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"

# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
#     "Referer": "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx",
# }

# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
#     "Referer": "https://www.bseindia.com",
# }


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.nseindia.com"
}

response = requests.get(url, headers=headers)

# if response.status_code == 200:
#     with open(f"bhavCopys/BhavCopy_BSE_FO_{date_str}.csv", "wb") as f:
#         f.write(response.content)
#     print("✅ Download successful")
# else:
#     print(f"❌ Failed with status code: {response.status_code}")


if response.status_code == 200:
    # Open ZIP file from memory
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # Find CSV file inside ZIP
        csv_files = [name for name in z.namelist() if name.lower().endswith(".csv")]

        if not csv_files:
            print("❌ No CSV file found inside ZIP")
            # return

        csv_name = csv_files[0]  # Mostly only one CSV in BhavCopy ZIP

        # Extract CSV contents
        csv_bytes = z.read(csv_name)

        # Save it with your required name
        output_path = f"bhavCopys/BhavCopy_NSE_FO_{date_str}.csv"

        with open(output_path, "wb") as f:
            f.write(csv_bytes)

        print(f"✅ Extracted & saved: {output_path}")
else:
    print(f"❌ Failed with status code: {response.status_code}")


# import datetime
# import requests

# date = datetime.date.today()  # or datetime.date(2025, 11, 4)
# date_str = date.strftime("%Y%m%d")

# url = f"https://www.bseindia.com/download/Bhavcopy/Derivative/BhavCopy_BSE_FO_0_0_0_{date_str}_F_0000.CSV"

# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
#     "Referer": "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx",
# }

# r = requests.get(url, headers=headers)
# print(r.status_code)

if __name__ == "__main__":
    date_str = "20251202"

    script_df = pd.read_csv("scriptStatus.csv")
    # print("row-1: ", script_df.at['row', 'isDataDownloaded'])
    # value = script_df.at[0, 'isDataDownloaded']
    # print("row-2: ", value)
    script_df.at[0, 'isDataDownloaded'] = True

    # script_df = pd.DataFrame([{"todayDate": str(date_str), "isDataDownloaded": False}])
    script_df.to_csv("scriptStatus.csv", index=False)
    
    # df = pd.read_csv("scriptStatus.csv")
    # print(df.info())

