from utils import Load_bhavCopy, calculate_net_pnl
import pandas as pd


def __calculate_net_pnl(trades_df, bhav_df):
    # sort trades by transaction-time
    # trades_df['transactTime'] = pd.to_datetime(trades_df['transactTime'])
    # trades_df = trades_df.sort_values(by="transactTime")

    # print(trades_df.head())

    # FIFO queue: {secID: [[qty, price], ...]}
    open_positions = {}
    realized_trades = []
    unrealized_trades = []

    # 1: calculate realized PnL by matching qty
    for sec, group in trades_df.groupby("instrument_id"):
        print("Processing SecID: ", sec)
        for _, trade in group.iterrows():
            # sec = trade['instrument_id']
            qty = trade['qty']
            px = trade['price']
            side = trade['side']
            print(f"trade details: sec={sec}, qty={qty}, price={px}, side={side}")
            # if side ==1 => qty positive else negative
            # qty = qty if qty == 1 else qty*(-1)
        
            if sec not in open_positions:
                open_positions[sec] = []

            if side == 1: # BUY
                if len(open_positions[sec]) == 0:
                    open_positions[sec].append([qty, px, side])
                    print(f"init open long position for sec {sec}: ", open_positions[sec])
                    continue

                open_qty, open_px, open_side = open_positions[sec][0]

                if open_side == 1: # buy (add more)
                    open_positions[sec].append([qty, px, side])
                    print(f"added open long position for sec {sec}: ", open_positions[sec])

                if open_side == 2: # open-pos:sell (sq.off short position)
                    buy_qty = qty
                    pnl = 0
                    while buy_qty > 0:
                        if len(open_positions[sec]) == 0:
                            open_positions[sec].append([buy_qty, px, side])
                            print(f"added remain open long position for sec {sec}: ", open_positions[sec])
                            buy_qty = 0
                            break
                        # open_qty, open_px, open_side = open_positions[sec][0]
                        match_qty = min(open_qty, buy_qty)

                        pnl += (open_px - px) * match_qty
                        print(f"matched trade for sec {sec}: match_qty={match_qty}, open_px={open_px}, px={px}, pnl={pnl}")
                        # pnl += (open_px - px)
                        # realized_trades.append([sec, match_qty, open_px, px, pnl])
                        realized_trades.append([sec, match_qty, open_px, px, pnl, "short"])

                        open_qty -= match_qty
                        buy_qty -= match_qty
                        # open_qty += match_qty
                        # buy_qty += match_qty

                        if open_qty == 0:
                            open_positions[sec].pop(0)
                        else:
                            open_positions[sec][0][0] = open_qty
                        print(f"remain open positions for sec {sec}: ", open_positions[sec])

            if side == 2: # SELL
                if len(open_positions[sec]) == 0:
                    open_positions[sec].append([qty, px, side])
                    print(f"init open short position for sec {sec}: ", open_positions[sec])
                    continue

                open_qty, open_px, open_side = open_positions[sec][0]

                if open_side == 2: # sell (add more)
                    open_positions[sec].append([qty, px, side])
                    print(f"added open short position for sec {sec}: ", open_positions[sec])

                if open_side == 1: # open-pos:buy (sq.off long position)
                    sell_qty = qty
                    pnl = 0
                    print("Entering while loop to match sell qty...", sell_qty)
                    while sell_qty > 0:
                        print("remaining sell qty: ", sell_qty)
                        if len(open_positions[sec]) == 0:
                            open_positions[sec].append([sell_qty, px, side])
                            print(f"added remain open short position for sec {sec}: ", open_positions[sec])
                            sell_qty = 0
                            break
                        match_qty = min(open_qty, sell_qty)

                        pnl += (px - open_px) * match_qty
                        print(f"matched trade for sec {sec}: match_qty={match_qty}, open_px={open_px}, px={px}, pnl={pnl}")
                        # pnl += (px - open_px)
                        # realized_trades.append([sec, match_qty, open_px, px, pnl])
                        realized_trades.append([sec, match_qty, open_px, px, pnl, "long"])

                        open_qty -= match_qty
                        sell_qty -= match_qty
                        # open_qty += match_qty
                        # sell_qty += match_qty

                        if open_qty == 0:
                            print("removing open position! and ramining sell qty: ", sell_qty)
                            open_positions[sec].pop(0)
                            if sell_qty != 0:
                                open_qty, open_px, open_side = open_positions[sec][0]
                        else:
                            print(f"updating open position for sec {sec}: ", open_positions[sec])
                            first_open_position = open_positions[sec][0]
                            first_open_position[0] = open_qty
                            # open_positions[sec][0][0] -= match_qty
                        print(f"remain open positions for sec {sec}: ", open_positions[sec])

    realized_df = pd.DataFrame(
        realized_trades,
        columns=["instrument_id", "matchedQty", "buyPrice", "sellPrice", "realizedPnL", "position"]
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
            close_price_df = bhav_df.loc[
                bhav_df['instrument_id'] == sec,
                ["sttlmPrice", "lastPrice", "closePrice", "isExpiryToday"]
                ].iloc[0].to_dict()
            if close_price_df["isExpiryToday"]:
                close_price = close_price_df["closePrice"]
            else:
                # close_price = close_price_df["sttlmPrice"]
                close_price = close_price_df["closePrice"]
            # if len(close_price) == 0:
            #     continue
            # close_price = close_price.values[0]

            if open_side == 1: # closed long position
                m2m_pnl = (close_price - avg_price) * total_qty
                # m2m_pnl = (close_price*total_qty - avg_price)
                unrealized_trades.append([sec, total_qty, avg_price, close_price, m2m_pnl, "long"])
            if open_side == 2: # closed short position
                m2m_pnl = (avg_price - close_price ) * total_qty
                # m2m_pnl = (avg_price - close_price*total_qty)
                unrealized_trades.append([sec, total_qty, close_price, avg_price, m2m_pnl, "short"])

            # unrealized_trades.append([sec, total_qty, avg_price, close_price, m2m_pnl])

    unrealized_df = pd.DataFrame(
        unrealized_trades,
        columns=["instrument_id", "matchedQty", "buyPrice", "sellPrice", "m2mPnl", "position"]
    )

    # get realized pnl for each SecurityIds
    # __realized_pnl = realized_df.groupby("instrument_id")["realizedPnL"].sum().reset_index()
    realized_df = realized_df.loc[realized_df["matchedQty"] != 0, :].reset_index(drop=True)
    __realized_pnl = realized_df.groupby("instrument_id")["realizedPnL"].sum().reset_index()
    net_realized_pnl = __realized_pnl["realizedPnL"].sum()
    net_m2m_pnl = unrealized_df["m2mPnl"].sum()
    net_pnl = net_realized_pnl + net_m2m_pnl

    return {
        "Realized_df": realized_df,
        "UnRealized_df": unrealized_df,
        "data": {
            "realizedPnl": net_realized_pnl,
            "m2mPnl": net_m2m_pnl,
            "netPnl": net_pnl
        }
    }


if __name__ == "__main__":
    exchange = "NSE"
    date_str = "20251125"

    df = pd.read_csv("aw11_pnl_test.csv")
    bhavCopy_df = Load_bhavCopy(exchange, date_str)
    
    # df = df.loc[df['instrument_id'] == 49908, ['transactTime', 'instrument_id', 'side', 'qty', 'price']].reset_index(drop=True)
    df = df.loc[:, ['transactTime', 'instrument_id', 'side', 'qty', 'price']].reset_index(drop=True)
    
    pnl = calculate_net_pnl(trades_df=df, bhav_df=bhavCopy_df)
    realized_pnl = pnl['Realized_df']
    realized_pnl = realized_pnl.groupby('instrument_id').agg(
        {
            'matchedQty': 'sum',
            'buyPrice': 'mean',
            'sellPrice': 'mean',
            'realizedPnL': 'sum'
        }
    )
    print(realized_pnl)
    unrealized_pnl = pnl['UnRealized_df']
    # print(pnl['Realized_df'])
    print("\n", "*"*20, "\n")
    unrealized_pnl = unrealized_pnl.groupby('instrument_id').agg(
        {
            'matchedQty': 'sum',
            'buyPrice': 'mean',
            'sellPrice': 'mean',
            'm2mPnl': 'sum'
        }
    )
    print(unrealized_pnl)
    # print("\n", "*"*20, "\n")
    # print(pnl['UnRealized_df'])


