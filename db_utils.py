import pandas as pd
from utils import download_bhavCopy, Load_bhavCopy

exchange = "NSE"
date_str = "20251125"
bhavCopy_df = Load_bhavCopy(exchange, date_str)
print(exchange)
print(bhavCopy_df.head())
print("*"*10)
print(bhavCopy_df.tail())


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