import pandas as pd
from utils import download_bhavCopy, Load_bhavCopy, read_zip_data
import tarfile
import zipfile
import shutil
# exchange = "NSE"
# date_str = "20251125"
# bhavCopy_df = Load_bhavCopy(exchange, date_str)
# print(exchange)
# print(bhavCopy_df.head())
# print("*"*10)
# print(bhavCopy_df.tail())

# def zip_and_remove(directory_path, output_zip_name):
#     """
#     Compress a directory into a ZIP file and remove the directory.

#     :param directory_path: Path of the directory to be zipped
#     :param output_zip_name: Output zip file name without extension
#     """

#     # 1. Create ZIP file
#     zip_file = shutil.make_archive(output_zip_name, 'zip', directory_path)
#     print(f"Created ZIP: {zip_file}")

#     # 2. Remove the original directory
#     shutil.rmtree(directory_path)
#     print(f"Removed directory: {directory_path}")

# zip_and_remove("sqOffPosition/20251127", "sqOffPosition/20251127")

zip_path = "sqOffPosition/20251127.zip"
# file_name = "m2mPnl_BSE_20251127.csv"
file_name = "realizedPnl_BSE_20251127.csv"

# with zipfile.ZipFile(zip_path) as z:
#     with z.open(file_name) as f:
#         df = pd.read_csv(f)
df = read_zip_data(zip_path, file_name)
print(df)
# def read_zip_data(zip_path, file_name):
#     with zipfile.ZipFile(zip_path) as z:
#         with z.open(file_name) as f:
#             df = pd.read_csv(f)
#     return df

# with tarfile.open("sqOffPosition/20251127.tar", "r:*") as tar:
#     for member in tar.getmembers():
#         print(member.name)

# def read_csv_from_tar(tar_path, csv_filename):
#     with tarfile.open(tar_path, "r:*") as tar:
#         member = tar.getmember(csv_filename)
#         f = tar.extractfile(member)
#         return pd.read_csv(f)

# # Example usage
# # df = read_csv_from_tar("backup.tar.gz", "sqOffPosition/m2mPnl_20251127.csv")
# df = read_csv_from_tar(f"sqOffPosition/20251127.tar", "m2mPnl_NSE_20251127.csv")
# print(df.head())


# exchange = "NSE"
# date_str = "20251124"
# path = f'dcTradeBook/DC_{exchange}_{date_str}.csv'
# df = pd.read_csv(path)
# # 1111111111111
# df = df[df['client_id'] == 'AW11'].reset_index(drop=True)
# print(df[df['client_id'] == 'AW11'].unique())
# def to_match_ctcl():
#     for exchange in ["NSE", "BSE"]:
#         path = f'dcTradeBook/DC_{exchange}_{date_str}.csv'
#         df = pd.read_csv(path)


# if exchange == "NSE":
#     df['ctcl'] = df['ctcl'].astype(str).str[:-3]
    # print(df['ctcl'].unique().shape)
    # print(df['ctcl'].unique())

# print(df['client_id'].unique().shape)
# print(df['client_id'].unique())
# print(df['ctcl'].unique().shape)
# print(df['ctcl'].unique())
# if exchange == "NSE":
#     pass
#     # print(df['ctcl'][0], " ", len(df['ctcl'][0]))
#     df['ctcl'] = df['ctcl'].astype(str).str[:-3]
#     # print(df['ctcl'][0], " ", len(df['ctcl'][0]))

# print(df['ctcl'].unique())
# print(len(df['client_id'][0]))
# print(df['ctcl'].unique().shape)
# result = (
#     df.groupby("client_id")
#       .agg(
#            distinct_ctcl=("ctcl", "nunique"),
#            total_rows=("ctcl", "count")
#       )
#       .reset_index()
# )

# result = (
#     df.groupby("ctcl")
#       .agg(
#            distinct_client_id=("client_id", "nunique"),
#            total_rows=("client_id", "count")
#       )
#       .reset_index()
# )

# print(result)
# result.to_csv(f'mapping_ctcl_clientId_{exchange}_{date_str}.csv', index=False)

# print(result[result['distinct_ctcl'] > 1].shape)
# print(df['client_id'].unique().shape)
# print(df['client_id'].unique())
# print(df['ctcl'].unique().shape)
# df["ctcl"] = df["ctcl"].astype(str).str[:-3]
# df.to_csv(f'dcTradeBook/DC_{exchange}_{date_str}.csv', index=False)