import datetime
import msvcrt
import investpy
import time
from sqlalchemy import create_engine
import threading
import pandas as pd
from talib import abstract
from urllib import request
import numpy as np
import yfinance
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import finplot as fplt
import requests
import sys
import tkinter as tk

np.seterr(divide='ignore', invalid='ignore')  # 忽略warning
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
fig = plt.figure(figsize=(20, 12), dpi=100, facecolor="white")  # 创建fig对象

lock = threading.Lock()
lockForSql = threading.Lock()
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
failList = []


def fetchData(stock_code):
    df = investpy.get_stock_historical_data(stock=stock_code,
                                            country='Taiwan',
                                            from_date='01/01/2010',
                                            to_date='01/10/2021')
    df['StockCode'] = stock_code
    df['Date'] = df.index
    print(df.head())
    # print(df.index)
    # print(df)
    writeSuccRecord(stock_code)

    return df


def insertToDB(df, conn):
    lockForSql.acquire()
    df.to_sql('historical_data', conn, if_exists='append', index=False)
    lockForSql.release()


def insert_to_tech_db(df, conn):
    lockForSql.acquire()
    df.to_sql('technical_data', conn, if_exists='append', index=False)
    lockForSql.release()


def connectDB():
    engine = create_engine(
        "mysql+pymysql://{}:{}@{}/{}?charset={}".format('fetchprogram', 'fetchprogram', '127.0.0.1:3306', 'twinvest',
                                                        'utf8'))
    conn = engine.connect()
    if conn:
        print("Connection Successful :)")
    else:
        print("Connection Failed :(")
        quit()
    return conn


def readList():
    codeList = []
    with open('ListedStockList.txt', 'r', encoding="utf8") as f:
        for line in f.readlines():
            spLine = line.split()
            if (spLine[0].isnumeric()):
                codeList.append(spLine[0])
    return codeList


def getfinishPoint():
    with open('finishRecord.txt', 'r', encoding="utf8") as f:
        for line in f.readlines():
            finished = line.split()[0]
    return finished


def fetchAndInsertForMT(codeList, finished, conn, fetchLength):
    try:
        for i in range(finished, finished + fetchLength):
            print(str(i))
            if (i > len(codeList)):
                print("fetchAndInsertForMT : i > len(codeList) , End")
                return
            try:
                df = fetchBySearch(codeList[i])
            except Exception as e:
                print(e)
                failList.append(codeList[i])
                print("Fetch " + codeList[i] + " Failed")
            try:
                df = df.rename(columns={'Open': 'open', 'Close': 'close', 'Low': 'low', 'High': 'high'}, inplace=False)
                df = add_talib_info(df)
                insertToDB(df, conn)
            except Exception as e:
                print(e)
                failList.append(codeList[i])
                print("Insert " + codeList[i] + " Failed")
    except Exception as e:
        print(e)


def fetchAndInsert(codeList, finished, conn):
    if (finished == ""):
        indexNo = 0
        start_point = 0
    else:
        indexNo = codeList.index(finished)
        start_point = indexNo + 1
    for i in range(start_point, len(codeList)):
        print(str(i))
        try:
            # df = fetchData(codeList[i])
            df = fetchBySearch(codeList[i])
            insertToDB(df, conn)
        except:
            writeFailedRecord(codeList[i])
            print("Fetch " + codeList[i] + " Failed")


def writeFailedRecord(failList):
    with open('failedList.txt', 'a', encoding="utf8") as f:
        for i in failList:
            f.write(str(i) + '\n')


def writeSuccRecord(stockCode):
    with open('finishRecord.txt', 'w', encoding="utf8") as f:
        f.write(stockCode + '\n')


def fetchBySearch(stockno):
    search_result = investpy.search_quotes(text=stockno, products=['stocks'],
                                           countries=['taiwan'], n_results=1)
    print(search_result)
    today = datetime.date.today().strftime("%d/%m/%Y")
    historical_data = search_result.retrieve_historical_data(from_date='01/01/2010', to_date='27/10/2021')
    historical_data['StockCode'] = stockno
    historical_data['Date'] = historical_data.index
    historical_data = historical_data.rename(columns={'Change Pct': 'ChangePct'}, inplace=False)
    print(historical_data)
    return historical_data


def readFailedList():
    failList = []
    with open('failedList.txt.txt', 'r', encoding="utf8") as f:
        for line in f.readlines():
            spLine = line.split()
            if (spLine[0].isnumeric()):
                failList.append(spLine[0])
    return failList


def multiThreadGetAll():
    conn = connectDB()
    codeList = readList()
    print(len(codeList))
    finished = 0
    t_list = []
    threadNo = 1
    if (threadNo == 1):
        t = threading.Thread(target=fetchAndInsertForMT,
                             args=(codeList, finished, conn, int(len(codeList))))
        t.start()
        t.join()
    else:
        for i in range(threadNo):
            t = threading.Thread(target=fetchAndInsertForMT,
                                 args=(codeList, finished, conn, int(len(codeList) / (threadNo - 1))))
            finished += int(len(codeList) / (threadNo - 1))
            t_list.append(t)
        for i in range(threadNo):
            t_list[i].start()
            print("thread " + str(i) + " Start")
        x = 0
        for i in t_list:
            i.join()
            print("thread " + str(x) + " Join")
    writeFailedRecord(failList)


def getTechnicalIndicator(stockno):
    search_result = investpy.search_quotes(text=stockno, products=['stocks'],
                                           countries=['taiwan'], n_results=1)
    data = search_result.retrieve_technical_indicators(interval='daily')
    print(data)


def add_talib_info(df):
    macd = abstract.MACD(df)
    ma5 = abstract.SMA(df, 5)
    ma5.name = "5ma"
    ma5frame = ma5.to_frame()
    ma10 = abstract.SMA(df, 10)
    ma10.name = "10ma"
    ma10frame = ma10.to_frame()
    ma20 = abstract.SMA(df, 20)
    ma20.name = "20ma"
    ma20frame = ma20.to_frame()
    ma60 = abstract.SMA(df, 60)
    ma60.name = "60ma"
    ma60frame = ma60.to_frame()
    ma120 = abstract.SMA(df, 120)
    ma120.name = "120ma"
    ma120frame = ma120.to_frame()

    bbands = abstract.BBANDS(df, timeperiod=20, nbdevup=float(2), nbdevdn=float(2))
    pdList = [df, macd, ma5frame, ma10frame, ma20frame, ma60frame, ma120frame, bbands]
    result = pd.concat(pdList, axis=1).fillna(0)
    # print(result.to_string)
    return result


def getDayTradeData():
    # download today stock data
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data"
    today = datetime.date.today()
    request.urlretrieve(url, "STOCK_DAY_ALL_" + str(today) + ".csv")
    url2 = "http://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_download.php?l=zh-tw&se=EW"
    request.urlretrieve(url2, "SQUOTE_EW_" + str(today) + ".csv")

def counter_data_preprocess(filename):
    # read input file
    fin = open(filename, "rt", encoding="utf-8")
    # read file contents to string
    data = fin.read()
    # replace all occurrences of the required string
    data = data.replace('","', '" "')
    data = data.replace(',', '')
    # close the input file
    fin.close()
    # open the input file in write mode
    fin = open(filename, "wt", encoding="utf-8")
    # overrite the input file with the resulting data
    fin.write(data)
    # close the file
    fin.close()

def read_conter_cvs_file(filename, date):
    counter_data_preprocess(filename)
    df = pd.read_csv(filename, encoding="utf-8", sep=' ', header=None)
    df = df.drop([10, 11, 12, 13, 14, 15, 16], 1)
    df = df.rename(
        columns={0: 'StockCode', 1: 'name', 2: 'close', 3: 'changeAct', 4: 'open', 5: 'high', 6: 'low', 7: 'volume',
                 8: 'TradeValue', 9: 'transaction'}, inplace=False)
    df = df.replace({"---": 0, "----": 0})
    df["Date"] = str(date).replace("-", "")
    # print(df.to_string())
    return df


def read_listed_cvs_file(filename, date):
    df = pd.read_csv(filename, encoding="utf-8", sep=',', header=None)
    df = df[1:]
    df = df.rename(
        columns={0: 'StockCode', 1: 'name', 2: 'volume', 3: 'TradeValue', 4: 'open', 5: 'high', 6: 'low', 7: 'close',
                 8: 'changeAct', 9: 'transaction'}, inplace=False)
    df = df.fillna(0)
    df["Date"] = str(date).replace("-", "")
    return df


def remove_excess_line(filename):
    #     delete 1~4 line and last line
    lines = []
    with open(filename, 'r', encoding="utf8") as fp:
        # read an store all lines into list
        lines = fp.readlines()
    lines = lines[4:len(lines) - 1]
    with open(filename, 'w', encoding="utf8") as fp:
        for line in lines:
            fp.write(line)


def change_encoding(filenmae):
    with open(filenmae, 'rb') as source_file:
        with open("reEncode_" + filenmae, 'w+b') as dest_file:
            contents = source_file.read()
            dest_file.write(contents.decode('cp950').encode('utf-8'))


def process_counter_data(filenmae, date):
    # change encoding from big5 to utf8
    change_encoding(filenmae)
    # remove lines that are unnecessary and have problem to transfer to dataframe
    filenmae = "reEncode_" + filenmae
    remove_excess_line(filenmae)
    # file to dataframe
    df = read_conter_cvs_file(filenmae, date)
    return df


def process_listed_data(filename, date):
    df = read_listed_cvs_file(filename, date)
    return df


def everyday_stock_data_update():
    getDayTradeData()
    date = datetime.date.today()
    conn = connectDB()
    df = process_counter_data("SQUOTE_EW_" + str(date) + ".csv", date)
    insertToDB(df, conn)
    df2 = process_listed_data("STOCK_DAY_ALL_" + str(date) + ".csv", date)
    insertToDB(df2, conn)


def insert_cvs_file_by_date(date):
    conn = connectDB()
    df = process_counter_data("SQUOTE_EW_" + str(date) + ".csv", date)
    insertToDB(df, conn)
    df2 = process_listed_data("STOCK_DAY_ALL_" + str(date) + ".csv", date)
    insertToDB(df2, conn)


def read_history_db_as_dataframe(SQL, conn):
    # conn = connectDB()
    df = pd.read_sql(SQL, con=conn)
    # print(df.to_string)
    return df


def fetch_history_insert_tech(stock_code, conn):
    # fetch from history
    SQL = "SELECT Date,StockCode,open,high,close,low,volume,TradeValue,transaction,name FROM `historical_data` WHERE StockCode = '" + stock_code + "'"
    df = read_history_db_as_dataframe(SQL, conn)

    # df.drop(
    #     (["changeAct", "ChangePct", "Currency", "5ma", "10ma", "20ma", "60ma", "120ma", "upperband", "middleband",
    #       "lowerband", "macd", "macdsignal", "macdhist", "TimeStamp"], 1))
    # add extra info
    df['ChangePct'] = df.close.pct_change() * 100
    df['changeAct'] = df.close.diff()
    df = add_talib_info(df)
    # inset in to technical data
    df = df.replace([np.inf, -np.inf], np.nan)
    insert_to_tech_db(df, conn)
    print(df)


def historical_to_technical(codeList, conn):
    # conn = connectDB()
    # codeList = readList()
    for stock_code in codeList:
        print("start proccessing " + stock_code)
        fetch_history_insert_tech(stock_code, conn)
        print("end proccessing " + stock_code)


def delete_old_table():
    conn = connectDB()
    SQL = "DELETE FROM `technical_data`"
    conn.execute(SQL)


def historical_to_technical_one_pack():
    delete_old_table()
    # fetch all
    conn = connectDB()
    sql = "SELECT Date,StockCode,open,high,close,low,volume,TradeValue,transaction,name FROM `historical_data`"
    df = read_history_db_as_dataframe(sql, conn)
    # print(df.to_string)
    # Process all
    grouped = df.groupby(df.StockCode)
    code_list = readList()
    picked_list = []
    for i in code_list:
        tmp = grouped.get_group(i)
        tmp['ChangePct'] = tmp.close.pct_change() * 100
        tmp['changeAct'] = tmp.close.diff()
        tmp = add_talib_info(tmp)
        # inset in to technical data
        tmp = tmp.replace([np.inf, -np.inf], np.nan)

        # pick or not
        # picked_list = pick_stock(tmp, picked_list)

        # insert all
        insert_to_tech_db(tmp, conn)
        print(tmp)
    # return picked_list


def fetch_technical_for_pick():
    conn = connectDB()
    sql = "SELECT * FROM `technical_data`"
    df = read_history_db_as_dataframe(sql, conn)
    grouped = df.groupby(df.StockCode)
    code_list = readList()
    picked_list = []
    picked_reason_list = []
    for i in code_list:
        tmp = grouped.get_group(i)
        # pick or not
        picked_list, picked_reason_list = pick_stock(tmp, picked_list, picked_reason_list)
        print(tmp)
    return picked_list, picked_reason_list


def draw(df):
    # ts_code = '2330'  # 股票代码
    symbol = df.StockCode.iloc[-1] + "-" + df.name.iloc[-1]
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

    #


def pick_stock(df, picked_list, picked_reason_list):
    picked = False
    reason = ""
    df = df.rename(columns={'5ma': 'ma5'}, inplace=False)
    # 過濾量太小的
    if (df.TradeValue.iloc[-1] < 20000000):
        return picked_list, picked_reason_list
    # # 過濾月線下彎
    elif ((df.middleband.iloc[-2] > df.middleband.iloc[-1]) and (df.middleband.iloc[-3] > df.middleband.iloc[-2])):
        return picked_list, picked_reason_list

    # 爆大量 (100% more)  (上漲，成交量>500)
    if ((df.volume.iloc[-1] > df.volume.iloc[-2] * 2) and (df.close.iloc[-1] > df.close.iloc[-2]) and df.volume.iloc[
        -1] > 500000):
        picked = True
        reason = reason + "V"
    # 當日支撐
    # 下布林通道
    elif ((df.close.iloc[-1] < df.lowerband.iloc[-1]) and (df.close.iloc[-1] > df.lowerband.iloc[-1])):
        picked = True
        reason = reason + "S"
    # 上布林通道
    elif ((df.close.iloc[-1] < df.upperband.iloc[-1]) and (df.close.iloc[-1] > df.upperband.iloc[-1])):
        picked = True
        reason = reason + "S"
    # 5日
    elif ((df.close.iloc[-1] < df.ma5.iloc[-1]) and (df.close.iloc[-1] > df.ma5.iloc[-1])):
        picked = True
        reason = reason + "S"
    # 月線
    elif ((df.close.iloc[-1] < df.middleband.iloc[-1]) and (df.close.iloc[-1] > df.middleband.iloc[-1])):
        picked = True
        reason = reason + "S"
    # 隔日支撐
    # 下布林通道
    elif ((df.close.iloc[-2] < df.lowerband.iloc[-2]) and (df.close.iloc[-1] > df.lowerband.iloc[-1])):
        picked = True
        reason = reason + "S"
    # 上布林通道
    elif ((df.close.iloc[-2] < df.upperband.iloc[-2]) and (df.close.iloc[-1] > df.upperband.iloc[-1])):
        picked = True
        reason = reason + "S"
    # 5日
    elif ((df.close.iloc[-2] < df.ma5.iloc[-2]) and (df.close.iloc[-1] > df.ma5.iloc[-1])):
        picked = True
        reason = reason + "S"
    # 月線
    elif ((df.low.iloc[-2] < df.middleband.iloc[-2]) and (df.close.iloc[-1] > df.middleband.iloc[-1])):
        picked = True
        reason = reason + "S"
    # 離線支撐
    #  how  ?

    #
    # # 突破
    # # 布林通道
    print(str(df.close.iloc[-1]) + "," + str(df.upperband.iloc[-1]))
    if ((df.close.iloc[-1] > df.upperband.iloc[-1])):
        picked = True
        reason = reason + "B"
    # macd
    if ((df.macdhist.iloc[-1] > df.macdhist.iloc[-2]) and (df.macdhist.iloc[-1] < 0)):
        picked = True
        reason = reason + "M"
    if (picked == True):
        picked_list.append(df)
        picked_reason_list.append(reason)
    return picked_list, picked_reason_list

    # 離開下軌道


def write_picked_list(stock_code_and_name):
    today = datetime.date.today()
    with open('Picked_List' + str(today) + '.txt', 'a', encoding="utf8") as f:
        for i in stock_code_and_name:
            f.write(str(i) + '\n')


def human_pick(picked_list, pciked_reason_list):
    global prdct
    print("picked_list length : " + str(len(picked_list)))
    conn = connectDB()
    man_picked_stock_code_list = []
    man_picked_reason_list = []
    n = 0
    length = len(picked_list)
    for i in picked_list:
        print("( " + str(n + 1) + " / " + str(length) + " )")

        draw(i)
        make_pick_or_drop()
        stock_code = i.StockCode.iloc[-1]
        name = i.name.iloc[-1]
        today = str(datetime.date.today()).replace("-", "")
        if (prdct == "pick"):
            man_picked_stock_code_list.append(i)
            man_picked_reason_list.append(pciked_reason_list[n])
            today = str(datetime.date.today()).replace("-", "")
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


def human_pick_2nd_round(man_picked_stock_code_list, man_picked_reason_list):
    print("2nd round picked_list length : " + str(len(man_picked_stock_code_list)))
    conn = connectDB()
    n = 0
    length = len(man_picked_stock_code_list)
    for i in man_picked_stock_code_list:
        print("( " + str(n + 1) + " / " + str(length) + " )")

        draw(i)
        make_decision()
        stock_code = i.StockCode.iloc[-1]
        name = i.name.iloc[-1]
        today = str(datetime.date.today()).replace("-", "")
        sql = "INSERT INTO `predict_log`(`Date`, `StockCode`, `name`, `picked_reason`, `predict`, `after_1_day`, `after_5_day`, `after_10_day`, `after_20_day`, `after_30_day`, `after_60_day`, `after_120_day`) " \
              "VALUES (" + str(
            today) + ",'" + stock_code + "','" + name + "','" + man_picked_reason_list[
                  n] + "','" + prdct + "',0,0,0,0,0,0,0)"
        conn.execute(sql)
        n += 1


def getchar_test():
    a = sys.stdin.read(1)
    return a


def winrate_test():  # not finished , wish it become a mini game
    # inupt latest data
    stock_df_list = fetch_technical_for_pick()
    # pick stock (use yesterday data)
    picked_list = []
    for i in stock_df_list:
        df = i.drop(i.tail(1).index, inplace=True)  # drop last n rows
        picked_list = pick_stock(df, picked_list)
    # draw (use yesterday data)

    # guess
    # check winrate
    return 0


prdct = ""


def make_decision():
    global prdct
    root = tk.Tk()
    root.title('make_decision')
    root.geometry('300x400+1500+0')

    def button_buy():
        global prdct
        prdct = "buy"
        root.destroy()

    def button_pay_attention():
        global prdct
        prdct = "pay attention"
        root.destroy()

    def button_keep_tracking():
        global prdct
        prdct = "keep tracking"
        root.destroy()

    def button_no_idea():
        global prdct
        prdct = "no idea"
        root.destroy()

    def button_drop():
        global prdct
        prdct = "drop"
        root.destroy()

    tk.Button(root, text='Buy', command=button_buy, bg='red', width='20', height='3').pack()
    tk.Button(root, text='Pay Attention', command=button_pay_attention, bg='orange', width='20', height='3').pack()
    tk.Button(root, text='Keep Tracking', command=button_keep_tracking, bg='green', width='20', height='3').pack()
    tk.Button(root, text='No Idea', command=button_no_idea, bg='grey', width='20', height='3').pack()
    tk.Button(root, text='Drop', command=button_drop, bg='dark grey', width='20', height='3').pack()

    root.mainloop()


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


def make_predict():
    global prdct
    root = tk.Tk()
    root.title('make_predict')
    root.geometry('300x600+1000+0')

    predict = ""

    def predict_up():
        global prdct
        prdct = "up"
        root.destroy()

    def predict_dowm():
        global prdct
        prdct = "down"
        root.destroy()

    def preditc_no_idea():
        global prdct
        prdct = "noidea"
        root.destroy()

    tk.Button(root, text='UP', command=predict_up, bg='red', width='20', height='3').pack()
    tk.Button(root, text='no_idea', command=preditc_no_idea, bg='grey', width='20', height='3').pack()
    tk.Button(root, text='down', command=predict_dowm, bg='green', width='20', height='3').pack()

    root.mainloop()


def test_tmp():
    conn = connectDB()
    sql = "SELECT * FROM `technical_data` WHERE StockCode ='1616'"
    df = read_history_db_as_dataframe(sql, conn)
    df = df[0:-1]
    print(str(df.close.iloc[-1]) + "," + str(df.upperband.iloc[-1]))
    if ((df.close.iloc[-1] > df.upperband.iloc[-1])):
        print("12131213123132")

    list = []
    list.append(df)
    # sql = "SELECT * FROM `technical_data` WHERE StockCode ='4976'"
    # df = read_history_db_as_dataframe(sql, conn)
    # list.append(df)
    pciked_reason_list = ["A"]
    picked_list, pciked_reason_list = fetch_technical_for_pick()
    human_pick(list, pciked_reason_list)


def predict_update():
    # get date order and data from technical_data
    conn = connectDB()
    sql = "SELECT Date FROM technical_data WHERE technical_data.StockCode ='2330' ORDER BY `technical_data`.`Date`"
    date_df = read_history_db_as_dataframe(sql, conn)
    today = str(date_df.Date.iloc[-1]).replace("-", "")
    yesterday = str(date_df.Date.iloc[-2]).replace("-", "")
    bf_5_day = str(date_df.Date.iloc[-6]).replace("-", "")
    bf_10_day = str(date_df.Date.iloc[-11]).replace("-", "")
    bf_20_day = str(date_df.Date.iloc[-21]).replace("-", "")
    bf_60_day = str(date_df.Date.iloc[-61]).replace("-", "")
    bf_120_day = str(date_df.Date.iloc[-121]).replace("-", "")
    date_list = [bf_5_day, bf_10_day, bf_20_day, bf_60_day, bf_120_day]

    # update yesterday predict
    print("update yesterday predict")
    sql = "SELECT * FROM predict_log WHERE Date = " + yesterday + " AND after_1_day = 0"
    df = read_history_db_as_dataframe(sql, conn)
    n = 0
    length = len(df)
    print(df.tail())
    for i in df.StockCode:
        sql = "SELECT * FROM technical_data WHERE StockCode = '" + i + "' AND Date = " + today
        print(sql)
        tech_df = read_history_db_as_dataframe(sql, conn)
        #
        try:
            sql = "UPDATE `predict_log` SET after_1_day = " + str(
                tech_df.ChangePct.iloc[0]) + " WHERE Date = " + yesterday + " AND StockCode = '" + i + "'"
            print(sql)
            conn.execute(sql)
        except:
            print("今日可能沒有交易")
        print("( " + str(n + 1) + " / " + str(length) + " )")
        n = n + 1
    ##
    ##
    print("update more days predict")
    db_column_list = ["after_5_day", "after_10_day", "after_20_day", "after_30_day", "after_60_day", "after_120_day"]
    n = 0
    for day in date_list:
        print("update " + db_column_list[n] + " predict")
        # 找要更新的股票代碼
        sql = "SELECT * FROM predict_log WHERE Date = " + day + " AND " + db_column_list[n] + " = 0"
        df = read_history_db_as_dataframe(sql, conn)
        length = len(df)
        print(df.tail())
        for i in df.StockCode:
            try:
                sql = "SELECT ((t2.close-t1.close)/t1.close) *100 AS percentage FROM technical_data AS t1, technical_data AS t2 " \
                      "WHERE t1.Date=" + day + " AND t2.Date = " + today + " AND t1.StockCode='" + i + "' AND t2.StockCode='" + i + "'"
                print(sql)
                percentage_df = read_history_db_as_dataframe(sql, conn)
                percentage = percentage_df.percentage.iloc[0]
                #
                sql = "UPDATE `predict_log` SET " + db_column_list[n] + " = " + str(
                    percentage) + " WHERE Date = " + day + " AND StockCode = '" + i + "'"
                print(sql)
                conn.execute(sql)
            except:
                print("今日可能沒有交易")
            print("( " + str(n + 1) + " / " + str(length) + " )")
            n = n + 1
    # update
    return 0


def all_stock_petcentage_check():
    return 0


def date_test():
    conn = connectDB()
    sql = "SELECT Date FROM technical_data WHERE technical_data.StockCode ='2330' ORDER BY `technical_data`.`Date`"
    date_df = read_history_db_as_dataframe(sql, conn)
    yesterday = str(date_df.Date.iloc[-2]).replace("-", "")
    print(yesterday)


def user_interface():
    print("start : user interface")
    print("1. update data")
    print("2. pick stock")
    print("3. end")
    c = input("choose : ")
    if (c == "1"):
        print("1. update every day stock data")
        print("2. update technical data")
        print("3. update predict data")
        print("4. update all")
        d = input("choose : ")
        if (d == "1"):
            print("start : everyday_stock_data_update")
            everyday_stock_data_update()
            print("end : everyday_stock_data_update")
        elif (d == "2"):
            print("start : historical_to_technical_one_pack")
            historical_to_technical_one_pack()
            print("end : historical_to_technical_one_pack")
        elif (d == "3"):
            print("start : predict_update")
            predict_update()
            print("end : predict_update")
        elif (d == "4"):
            print("start : everyday_stock_data_update")
            everyday_stock_data_update()
            print("end : everyday_stock_data_update")
            print("start : historical_to_technical_one_pack")
            historical_to_technical_one_pack()
            print("end : historical_to_technical_one_pack")
            print("start : predict_update")
            predict_update()
            print("end : predict_update")

    elif (c == "2"):
        print("start : pick_stock_one_pack")
        pick_stock_one_pack()
        print("end : pick_stock_one_pack")
        return 0
    elif(c == "3"):
        print("program end.")
        quit()
    return 0


def pick_stock_one_pack():
    # pick stock
    picked_list, pciked_reason_list = fetch_technical_for_pick()
    man_picked_stock_code_list, man_picked_reason_list = human_pick(picked_list, pciked_reason_list)
    human_pick_2nd_round(man_picked_stock_code_list, man_picked_reason_list)


def main():
    global prdct
    # pd.set_option('mode.chained_assignment', None)

    # insert_cvs_file_by_date("2021-11-09")
    # insert_cvs_file_by_date("2021-10-29")
    # read_db_as_dataframe('SELECT * FROM historical_data WHERE historical_data.StockCode="2330"')

    # conn = connectDB()
    # fetch_history_insert_tech("4163",conn)

    # update data
    # everyday_stock_data_update()
    # historical_to_technical_one_pack()
    # predict_update()

    # pick stock
    # picked_list, pciked_reason_list = fetch_technical_for_pick()
    # man_picked_stock_code_list, man_picked_reason_list = human_pick(picked_list, pciked_reason_list)
    # human_pick_2nd_round(man_picked_stock_code_list, man_picked_reason_list)
    # write_picked_list(picked_stock_code_list,pciked_reason_list)

    # test_tmp()
    # predict testing
    # make_decision()

    while True:
        user_interface()


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
