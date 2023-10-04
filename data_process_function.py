# 存放處理資料的function，通常輸出為資料
import threading

from talib import abstract
import pandas as pd
import numpy as np
import datetime
from os import path
import hashlib
import os

import database_function
import file_process_function
from investing_related import fetchBySearch

failList = []

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

# 將historical資料增加技術指標後寫到technical
def historical_to_technical_one_pack():
    database_function.delete_old_table()
    # fetch all
    conn = database_function.connectDB()
    sql = "SELECT Date,StockCode,open,high,close,low,volume,TradeValue,transaction,name FROM `historical_data`"
    df = database_function.read_history_db_as_dataframe(sql, conn)
    # print(df.to_string)
    # Process all
    grouped = df.groupby(df.StockCode)
    code_list = file_process_function.readList()
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
        database_function.insert_to_tech_db(tmp, conn)
        print(tmp)
    # return picked_list

def everyday_stock_data_update():
    date = datetime.date.today()
    yesterday = date - datetime.timedelta(days=1)

    # download
    file_process_function.getDayTradeData()
    # process
    df = file_process_function.process_counter_data("origin/上櫃盤後資訊_" + str(date) + ".csv", date)
    df2 = file_process_function.process_listed_data("origin/上市盤後資訊_" + str(date) + ".csv", date)
    #
    today_counter_file = "origin/上櫃盤後資訊_" + str(date) + ".csv"
    today_listed_file = "origin/上市盤後資訊_" + str(date) + ".csv"
    today_reencode_conter_file = "processed/reEncode_上市盤後資訊_" + str(date) + ".csv"
    #
    yesterday_counter_file = "processed/reEncode_上市盤後資訊_" + str(yesterday) + ".csv"
    n = 1
    # while (not(path.exists(yesterday_counter_file))):
    #     yesterday = date - datetime.timedelta(days=n)
    #     yesterday_counter_file = "origin/上櫃盤後資訊_" + str(yesterday) + ".csv"
    #     n += 1
    # yesterday_counter_file = "origin/上櫃盤後資訊_" + str(yesterday) + ".csv"
    # yesterday_listed_file = "origin/上市盤後資訊_" + str(yesterday) + ".csv"
    # # check today's data is different from yesterday's ( start )
    # updateflag = True
    # if(file_checksum(today_counter_file) == file_checksum(yesterday_counter_file)):
    #     os.remove(today_counter_file)
    #     os.remove(today_reencode_conter_file)
    #     print("[Error] today_counter_file is the same file sa yesterday, deleted")
    #     updateflag = False
    # if(file_checksum(today_listed_file) == file_checksum(yesterday_listed_file)):
    #     os.remove(today_listed_file)
    #     print("[Error] today_listed_file is the same file sa yesterday, deleted")
    #     updateflag = False
    # # check today's data is different from yesterday's ( end )
    # if(updateflag == True):
    #     conn = database_function.connectDB()
    #     database_function.insertToDB(df, conn)
    #     database_function.insertToDB(df2, conn)

def file_checksum(filename):
    return hashlib.md5(open(filename, 'rb').read()).hexdigest()

# 從選定的日期開始讀取並insert資料到資料庫當中，直到今天為止
def insert_csv_data_from_date():
    start_date = input("Please input start date ,(formate : 2022-01-01): ")
    date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    today = datetime.datetime.today()
    conn = database_function.connectDB()
    print("date : "+str(date) )
    print("todat : "+str(today))
    print(str(date < today))
    while (date < today):
        print("讀取" + str(date.date()) + ":")
        counter_filename = "reEncode_上櫃盤後資訊_"+str(date.date())+".csv"
        append_counter_filename = "reEncode_上櫃盤後資訊_BIG5_"+str(date.date())+".csv"
        listed_filename = "上市盤後資訊_"+str(date.date())+".csv"
        append_listed_filename = "reEncode_上市盤後資訊_BIG5_"+str(date.date())+".csv"
        #
        if(path.exists(os.path.join("processed", counter_filename))):
            df = file_process_function.read_conter_cvs_file(os.path.join("processed", counter_filename), date.date())
            database_function.insertToDB(df, conn)
            print(counter_filename)
        #
        if(path.exists(os.path.join("processed", append_counter_filename))):
            df = file_process_function.read_appended_conter_cvs_file(os.path.join("processed", append_counter_filename), date.date())
            database_function.insertToDB(df, conn)
            print(append_counter_filename)
        #
        if (path.exists(os.path.join("origin", listed_filename))):
            df = file_process_function.read_listed_cvs_file(os.path.join("origin", listed_filename), date.date())
            database_function.insertToDB(df, conn)
            print(listed_filename)
        #
        if (path.exists(os.path.join("processed", append_listed_filename))):
            df = file_process_function.read_appended_listed_cvs_file(os.path.join("processed", append_listed_filename),date.date())
            database_function.insertToDB(df, conn)
            print(append_listed_filename)

        date = date + datetime.timedelta(days=1)
        #
        # if(path.exists("SQUOTE_EW_" + str(date.date()) + ".csv")):
        #     print("insert("+str(date.date())+")")
        #     #insert begin
        #     df = file_process_function.process_counter_data("SQUOTE_EW_" + str(date.date()) + ".csv", date.date())
        #     database_function.insertToDB(df, conn)
        #     df2 = file_process_function.process_listed_data("STOCK_DAY_ALL_" + str(date.date()) + ".csv", date.date())
        #     database_function.insertToDB(df2, conn)
        #     #insert end
        #     date = date + datetime.timedelta(days=1)
        # else:
        #     date = date +  datetime.timedelta(days=1)

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
                database_function.insertToDB(df, conn)
            except Exception as e:
                print(e)
                failList.append(codeList[i])
                print("Insert " + codeList[i] + " Failed")
    except Exception as e:
        print(e)

def multiThreadGetAll():
    conn = database_function.connectDB()
    codeList = file_process_function.readList()
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
    file_process_function.writeFailedRecord(failList)

