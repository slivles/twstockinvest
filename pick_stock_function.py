import finplot as fplt
import tkinter as tk
import pandas as pd

import database_function

# 畫出K線圖
import file_process_function


def draw(df,pciked_reason):
    # ts_code = '2330'  # 股票代码
    symbol = df.StockCode.iloc[-1] + "-" + df.name.iloc[-1] + pciked_reason
    # set df
    # conn = connectDB()
    # sql = "SELECT * FROM `technical_data` WHERE StockCode = '2330'"
    # df = read_history_db_as_dataframe(sql, conn)
    # df = df.rename(columns={'open': 'Open', 'close': 'Close', 'low': 'Low', 'high': 'High'}, inplace=False)
    df['Date'] = pd.to_datetime(df['Date']).astype('int64')
    df = df.set_index("Date")
    # df = yfinance.download('AAPL')

    # change color
    fplt.background = '#dddddd'
    fplt.odd_plot_background = '#dddddd'

    # create two axes
    ax, ax2, ax3 = fplt.create_plot(symbol, rows=3)

    # plot candle sticks
    candles = df[['open', 'close', 'high', 'low']]
    candlestick_plot = fplt.candlestick_ochl(candles, ax=ax)
    # ax = fplt.create_plot(symbol)
    # overlay volume on the top plot
    volumes = df[['open', 'close', 'volume']]
    volume_plot = fplt.volume_ocv(volumes, ax=ax2)

    # put an MA on the close price
    fplt.plot(df['close'].rolling(120).mean(), ax=ax, legend='ma-120')
    fplt.plot(df['close'].rolling(60).mean(), ax=ax, legend='ma-60')
    fplt.plot(df['close'].rolling(20).mean(), ax=ax, legend='ma-20')
    fplt.plot(df['close'].rolling(5).mean(), ax=ax, legend='ma-5')

    # plot macd with standard colors first
    # macd = df.macdhist
    macd_plot = fplt.volume_ocv(df[['open', 'close', 'macdhist']], ax=ax3, colorfunc=fplt.strength_colorfilter)
    # fplt.plot(macd, ax=ax2, legend='MACD')

    # b-bands
    fplt.plot(df.upperband, ax=ax, color='#4e4ef1')
    fplt.plot(df.lowerband, ax=ax, color='#4e4ef1')

    candlestick_plot.colors.update(dict(
        bull_shadow='#d56161',
        bull_frame='#5c1a10',
        bull_body='#e8704f',
        bear_shadow='#388d53',
        bear_frame='#205536',
        bear_body='#52b370',
        text_color='white',
        label_color='white',
        grid_color='white'
    ))

    volume_plot.colors.update(dict(
        bull_shadow='#d56161',
        bull_frame='#5c1a10',
        bull_body='#e8704f',
        bear_shadow='#388d53',
        bear_frame='#205536',
        bear_body='#52b370',
        text_color='white',
        label_color='white',
        grid_color='white'
    ))

    macd_plot.colors.update(dict(
        bull_shadow='#d56161',
        bull_frame='#5c1a10',
        bull_body='#e8704f',
        bear_shadow='#388d53',
        bear_frame='#205536',
        bear_body='#52b370',
    ))

    # restore view (X-position and zoom) if we ever run this example again
    # fplt.autoviewrestore()

    fplt.show()

# 第一階段電腦篩選的規則
def pick_stock(df, picked_list, picked_reason_list):
    picked = False
    reason = ""
    df = df.rename(columns={'5ma': 'ma5'}, inplace=False)
    df = df.rename(columns={'10ma': 'ma10'}, inplace=False)
    # 過濾量太小的
    if (df.TradeValue.iloc[-1] < 20000000):
        return picked_list, picked_reason_list


    # 爆大量 (100% more)  (上漲，成交量>500)
    if ((df.volume.iloc[-1] > df.volume.iloc[-2] * 2) and (df.close.iloc[-1] > df.close.iloc[-2]) and df.volume.iloc[
        -1] > 500000):
        picked = True
        reason = "、爆大量"
    # 過濾月線下彎
    elif ((df.middleband.iloc[-2] > df.middleband.iloc[-1]) and (df.middleband.iloc[-3] > df.middleband.iloc[-2])):
        return picked_list, picked_reason_list
    # 當日支撐
    # 下布林通道
    if ((df.close.iloc[-1] < df.lowerband.iloc[-1]) and (df.close.iloc[-1] > df.lowerband.iloc[-1])):
        picked = True
        reason = reason + "、下布林支撐"
    # 上布林通道
    if ((df.close.iloc[-1] < df.upperband.iloc[-1]) and (df.close.iloc[-1] > df.upperband.iloc[-1])):
        picked = True
        reason = reason + "、上布林支撐"
    # 5日
    if ((df.close.iloc[-1] < df.ma5.iloc[-1]) and (df.close.iloc[-1] > df.ma5.iloc[-1])):
        picked = True
        reason = reason + "、5日支撐"
    # 10日
    if ((df.close.iloc[-1] < df.ma10.iloc[-1]) and (df.close.iloc[-1] > df.ma10.iloc[-1])):
        picked = True
        reason = reason + "、10日支撐"
    # 月線
    if ((df.close.iloc[-1] < df.middleband.iloc[-1]) and (df.close.iloc[-1] > df.middleband.iloc[-1])):
        picked = True
        reason = reason + "、月線支撐"
    # 隔日支撐
    # 下布林通道
    if ((df.close.iloc[-2] < df.lowerband.iloc[-2]) and (df.close.iloc[-1] > df.lowerband.iloc[-1])):
        picked = True
        reason = reason + "、下布林隔日支撐"
    # 上布林通道
    if ((df.close.iloc[-2] < df.upperband.iloc[-2]) and (df.close.iloc[-1] > df.upperband.iloc[-1])):
        picked = True
        reason = reason + "、上布林隔日支撐"
    # 5日
    if ((df.close.iloc[-2] < df.ma5.iloc[-2]) and (df.close.iloc[-1] > df.ma5.iloc[-1])):
        picked = True
        reason = reason + "、5日隔日支撐"
    # 5日
    if ((df.close.iloc[-2] < df.ma10.iloc[-2]) and (df.close.iloc[-1] > df.ma10.iloc[-1])):
        picked = True
        reason = reason + "、10日隔日支撐"
    # 月線
    if ((df.low.iloc[-2] < df.middleband.iloc[-2]) and (df.close.iloc[-1] > df.middleband.iloc[-1])):
        picked = True
        reason = reason + "、月線隔日支撐"
    # 離線支撐
    #  how  ?

    #
    # # 突破
    # # 布林通道
    print(str(df.close.iloc[-1]) + "," + str(df.upperband.iloc[-1]))
    if ((df.close.iloc[-1] > df.upperband.iloc[-1])):
        picked = True
        reason = reason + "、突破布林"
    # macd
    # if ((df.macdhist.iloc[-1] > df.macdhist.iloc[-2]) and (df.macdhist.iloc[-1] < 0)):
    #     picked = True
    #     reason = reason + "M"
    if (picked == True):
        picked_list.append(df)
        picked_reason_list.append(reason)
    return picked_list, picked_reason_list

    # 離開下軌道

def make_pick_or_drop():
    global prdct
    root = tk.Tk()
    root.title('pick_or_drop')
    root.geometry('300x200+1500+0')

    predict = ""

    def button_pick():
        global prdct
        prdct = "pick"
        root.destroy()

    def button_drop():
        global prdct
        prdct = "drop"
        root.destroy()

    tk.Button(root, text='Pick', command=button_pick, bg='red', width='20', height='3').pack()
    tk.Button(root, text='Drop', command=button_drop, bg='grey', width='20', height='3').pack()

    root.mainloop()



def human_pick(picked_list, pciked_reason_list):
    global prdct
    global today
    print("picked_list length : " + str(len(picked_list)))
    conn = database_function.connectDB()
    man_picked_stock_code_list = []
    man_picked_reason_list = []
    n = 0
    length = len(picked_list)
    picked_count = 0
    for i in picked_list:
        print("( " + str(n + 1) + " / " + str(length) + " ) "+'{:.1%}'.format((n + 1)/length)+" ,Picked : "+str(picked_count))

        draw(i,pciked_reason_list[n])
        make_pick_or_drop()
        stock_code = i.StockCode.iloc[-1]
        name = i.name.iloc[-1]
        # today = str(datetime.date.today()).replace("-", "")
        if (prdct == "pick"):
            picked_count += 1
            man_picked_stock_code_list.append(i)
            man_picked_reason_list.append(pciked_reason_list[n])
            # today = str(datetime.date.today()).replace("-", "")
            sql = "INSERT INTO `predict_log`(`Date`, `StockCode`, `name`, `picked_reason`, `predict`, `after_1_day`, `after_5_day`, `after_10_day`, `after_20_day`, `after_30_day`, `after_60_day`, `after_120_day`) " \
                  "VALUES (" + str(
                today) + ",'" + stock_code + "','" + name + "','" + pciked_reason_list[
                      n] + "','" + prdct + "',0,0,0,0,0,0,0)"
            conn.execute(sql)
        else:
            # print(stock_code_and_name)
            sql = "INSERT INTO `predict_log`(`Date`, `StockCode`, `name`, `picked_reason`, `predict`, `after_1_day`, `after_5_day`, `after_10_day`, `after_20_day`, `after_30_day`, `after_60_day`, `after_120_day`) " \
                  "VALUES (" + str(
                today) + ",'" + stock_code + "','" + name + "','" + pciked_reason_list[
                      n] + "','" + prdct + "',0,0,0,0,0,0,0)"
            conn.execute(sql)
        n += 1
    return man_picked_stock_code_list, man_picked_reason_list

def make_decision():
    global prdct
    root = tk.Tk()
    root.title('make_decision')
    root.geometry('300x500+1500+200')

    def button_1():
        global prdct
        prdct = "low risk must buy"
        root.destroy()

    def button_2():
        global prdct
        prdct = "predict up A"
        root.destroy()

    def button_3():
        global prdct
        prdct = "predict up B"
        root.destroy()

    def button_4():
        global prdct
        prdct = "curious"
        root.destroy()

    def button_5():
        global prdct
        prdct = "conflict"
        root.destroy()

    def button_6():
        global prdct
        prdct = "drop"
        root.destroy()

    tk.Button(root, text='low risk must buy', command=button_1, bg='red', width='20', height='3').pack()
    tk.Button(root, text='predict up A', command=button_2, bg='orange', width='20', height='3').pack()
    tk.Button(root, text='predict up B', command=button_3, bg='green', width='20', height='3').pack()
    tk.Button(root, text='curious', command=button_4, bg='blue', width='20', height='3').pack()
    tk.Button(root, text='conflict', command=button_5, bg='grey', width='20', height='3').pack()
    tk.Button(root, text='drop', command=button_6, bg='dark grey', width='20', height='3').pack()

    root.mainloop()



def human_pick_2nd_round(man_picked_stock_code_list, man_picked_reason_list):
    global today
    print("2nd round picked_list length : " + str(len(man_picked_stock_code_list)))
    conn = database_function.connectDB()
    n = 0
    length = len(man_picked_stock_code_list)
    final_predict_list = []
    for i in man_picked_stock_code_list:
        print("( " + str(n + 1) + " / " + str(length) + " )")
        draw(i,man_picked_reason_list[n])
        make_decision()
        stock_code = i.StockCode.iloc[-1]
        name = i.name.iloc[-1]
        # today = str(datetime.date.today()).replace("-", "")
        sql = "INSERT INTO `predict_log`(`Date`, `StockCode`, `name`, `picked_reason`, `predict`, `after_1_day`, `after_5_day`, `after_10_day`, `after_20_day`, `after_30_day`, `after_60_day`, `after_120_day`) " \
              "VALUES (" + str(
            today) + ",'" + stock_code + "','" + name + "','" + man_picked_reason_list[
                  n] + "','" + prdct + "',0,0,0,0,0,0,0)"
        conn.execute(sql)
        final_predict_list.append(prdct)
        n += 1
    file_name = "records/man_pick_records_"+database_function.get_last_trading_date_with_dash_from_db()+".txt"
    file_process_function.write_pick_records(file_name, man_picked_stock_code_list, final_predict_list)
