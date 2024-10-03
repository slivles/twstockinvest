import datetime
from datetime import date, timedelta
import msvcrt
import investpy
import time
from sqlalchemy import create_engine
import threading
from threading import Timer
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
import os
import hashlib
import os.path
from os import path
import schedule
import calendar
from apscheduler.schedulers.background import BackgroundScheduler
from dateutil.relativedelta import relativedelta


# 程式
import database_function
import investing_related
import file_process_function
import data_process_function
import pick_stock_function
import global_variable
from pick_stock_function import pick_stock, pick_stock_2, human_pick_2nd_round, human_pick,backtest_get_monthly_high_low_price

np.seterr(divide='ignore', invalid='ignore')  # 忽略warning
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
fig = plt.figure(figsize=(20, 12), dpi=100, facecolor="white")  # 创建fig对象

lock = threading.Lock()
lockForSql = threading.Lock()

failList = []

#global variable


# 三大法人功能整合
def three_major_leagal_person_intergrate():
    file_process_function.download_daily_three_major_leagal_person_data()
    today = str(datetime.date.today())
    # today = str(datetime.datetime.strptime("2022-10-24","%Y-%m-%d").date())
    # file_process_function.process_three_major_leagal_person_data("LegalPerson/上市三大法人_" + today + ".csv")
    # file_process_function.process_three_major_leagal_person_data("LegalPerson/上櫃三大法人_" + today + ".csv")
    # file_process_function.remove_warrant_from_csv("LegalPerson/上市三大法人_" + today + ".csv")

def insert_cvs_file_by_date(date):
    conn = database_function.connectDB()
    df = file_process_function.process_counter_data("SQUOTE_EW_" + str(date) + ".csv", date)
    database_function.insertToDB(df, conn)
    df2 = file_process_function.process_listed_data("STOCK_DAY_ALL_" + str(date) + ".csv", date)
    database_function.insertToDB(df2, conn)

def historical_to_technical(codeList, conn):
    # conn = connectDB()
    # codeList = readList()
    for stock_code in codeList:
        print("start proccessing " + stock_code)
        database_function.fetch_history_insert_tech(stock_code, conn)
        print("end proccessing " + stock_code)

# 抓technical_data 出來，用來餵給電腦篩選
def fetch_technical_for_pick():
    conn = database_function.connectDB()
    sql = "SELECT * FROM `technical_data`"
    df = database_function.read_history_db_as_dataframe(sql, conn)
    grouped = df.groupby(df.StockCode)
    code_list = file_process_function.readList()
    picked_list = []
    picked_reason_list = []
    for i in code_list:
        tmp = grouped.get_group(i)
        # pick or not
        picked_list, picked_reason_list = pick_stock(tmp, picked_list, picked_reason_list)
        print(tmp)
    return picked_list, picked_reason_list

def fetch_technical_for_back_test(date):
    conn = database_function.connectDB()
    sql = "SELECT * FROM `technical_data` WHERE `Date` <= " + date + " and StockCode in(select StockCode from historical_data hd2 where `Date` = "+ date +")"
    df = database_function.read_history_db_as_dataframe(sql, conn)
    grouped = df.groupby(df.StockCode)
    end_date_int = int(date) + 10000
    end_date = str(end_date_int)
    # sql = "select StockCode FROM `technical_data` where Date >= "+date+" and Date <= "+end_date+" group by StockCode having count(distinct MONTH(Date)) = 12 INTERSECT select StockCode FROM `technical_data` where Date = "+date
    sql = "select StockCode FROM `technical_data` where Date >= " + date + " and Date <= " + end_date + " group by StockCode INTERSECT select StockCode FROM `technical_data` where Date = " + date
    code_list = database_function.read_history_db_as_dataframe(sql, conn)
    picked_list = []
    picked_reason_list = []
    price_list = []
    for index, row in code_list.iterrows():
        # print(" i = " + row['StockCode'])
        tmp = grouped.get_group(row['StockCode'])
        # pick or not
        picked_list, picked_reason_list, price_list = pick_stock_2(tmp, picked_list, picked_reason_list, row['StockCode'], price_list)
    your_index = range(len(picked_list))
    data = {'stock_code': picked_list, 'buyin_reason': picked_reason_list, 'price':price_list}
    # print(len(picked_list), len(picked_reason_list), len(price_list))
    df2 = pd.DataFrame(data, index=your_index)
    file_name = "computer_picked/computer_picked_stock_" + date + ".xlsx"
    df2.to_excel(file_name, index=False)
    # 現在df2當中有 股票代號、日期、價格、原因，要再加上往後12個月的上漲、下跌
    df3 = backtest_get_monthly_high_low_price(df2, date)

    return df3

today = database_function.get_last_trading_date_from_db()

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

def user_interface():
    print("start : user interface")
    print("1. update data")
    print("2. pick stock")
    print("3. fetch data from investing.com")
    print("4. back test")
    print("5. end")
    c = input("choose : ")
    if (c == "1"):
        print("1. update everyday stock data")
        print("2. update technical data")
        print("3. update predict data")
        print("4. update all")
        print("5. Start a thread to auto update daily data")
        print("6. insert csv data start from certain day")
        print("7. update everyday legal person data")
        print("8. update Certain Day Stock Data")
        print("9. update Database technical table")
        print("10. insert csv data start from db last +1 day")
        d = input("choose : ")
        if (d == "1"):
            print("=======================================================")
            print("start : everyday_stock_data_update")
            data_process_function.everyday_stock_data_update()
            print("end : everyday_stock_data_update")
            print("=======================================================")
        elif (d == "2"):
            print("=======================================================")
            print("start : historical_to_technical_one_pack")
            data_process_function.historical_to_technical_one_pack()
            print("end : historical_to_technical_one_pack")
            print("=======================================================")
        elif (d == "3"):
            print("=======================================================")
            print("start : predict_update")
            database_function.predict_update()
            print("end : predict_update")
            print("=======================================================")
        elif (d == "4"):
            print("=======================================================")
            print("start : everyday_stock_data_update")
            data_process_function.everyday_stock_data_update()
            print("end : everyday_stock_data_update")
            print("start : historical_to_technical_one_pack")
            data_process_function.historical_to_technical_one_pack()
            print("end : historical_to_technical_one_pack")
            print("start : predict_update")
            database_function.predict_update()
            print("end : predict_update")
            print("start : three_major_leagal_person_intergrate")
            three_major_leagal_person_intergrate()
            print("end : three_major_leagal_person_intergrate")
            print("=======================================================")
        elif (d == "5"):
            print("=======================================================")
            print("start thread : schedule_auto_update_everyday_data")
            scheduler = BackgroundScheduler(timezone="Asia/Taipei")
            scheduler.add_job(data_process_function.everyday_stock_data_update, 'cron', day_of_week='0-4', hour=16, minute=00, misfire_grace_time=120)
            scheduler.start()
            print("=======================================================")
            # t = threading.Thread(target=schedule_auto_update_everyday_data)  #
            # t.start()  # 開始
        elif (d == "6"):
            print("=======================================================")
            print("start : insert_csv_data_from_date()")
            data_process_function.insert_csv_data_from_date()
            print("end : insert_csv_data_from_date()")
            print("=======================================================")
        elif (d == "7"):
            print("=======================================================")
            print("start : three_major_leagal_person_intergrate()")
            three_major_leagal_person_intergrate()
            print("end : three_major_leagal_person_intergrate()")
            print("=======================================================")
        elif (d == "8"):
            print("=======================================================")
            print("start : getCertainDayTradeData()")
            file_process_function.getCertainDayTradeData()
            print("end : getCertainDayTradeData()")
            print("=======================================================")
        elif (d == "9"):
            print("=======================================================")
            print("start : historical_to_technical_one_pack()")
            data_process_function.historical_to_technical_one_pack()
            print("end : historical_to_technical_one_pack()")
            print("=======================================================")
        elif( d == "10"):
            print("=======================================================")
            print("start : insert_csv_data_from_db_date()")
            data_process_function.insert_csv_data_from_db_date()
            print("end : insert_csv_data_from_db_date()")
            print("=======================================================")
    elif (c == "2"):
        print("start : pick_stock_one_pack")
        pick_stock_one_pack()
        print("end : pick_stock_one_pack")
        return 0
    elif (c == "3"):
        print("start : multiThreadGetAll")
        data_process_function.multiThreadGetAll()
        print("end : multiThreadGetAll")
    elif (c == "4"):
        print("1. back_test()")
        print("2. manually_backtest_one_pack")
        print("3. sell_strategy_backtest")
        d = input("choose : ")
        if (d == "1"):
            print("start : back_test")
            back_test()
            print("end : back_test")
        elif (d == "2"):
            print("start : manually_backtest_one_pack")
            manually_backtest_one_pack()
            print("end : manually_backtest_one_pack")
        elif (d == "3"):
            print("start : sell_strategy_backtest")
            sell_strategy_backtest()
            print("end : sell_strategy_backtest")
    elif (c == "5"):
        print("program end.")
        quit()
    return 0

def back_test(): #程式篩選
    conn = database_function.connectDB()
    start_date = input("請輸入開始日期(yyyyMMdd) : ")
    end_date = input("請輸入結束日期(yyyyMMdd) : ")
    # 西元年轉民國年
    start_date_year = start_date[:4]
    start_date_month = start_date[4:6]
    start_date_day = start_date[6:]
    end_date_year = end_date[:4]
    end_date_month = end_date[4:6]
    end_date_day = end_date[6:]
    #
    start_date = date(int(start_date_year), int(start_date_month), int(start_date_day))
    end_date = date(int(end_date_year), int(end_date_month), int(end_date_day))
    delta = timedelta(days=1)
    while start_date <= end_date:
        df = fetch_technical_for_back_test(start_date.strftime('%Y%m%d'))
        file_name = "records/back_test_" + str(start_date) + ".xlsx"
        df['Date'] = start_date.strftime('%Y%m%d')
        df.to_excel(file_name, index=False)
        database_function.insert_to_backtest_db(df, conn)
        start_date += delta
    #
    # man_picked_stock_code_list, man_picked_reason_list = human_pick(picked_list, picked_reason_list)
    # human_pick_2nd_round(man_picked_stock_code_list, man_picked_reason_list)

    return 0

def pick_stock_one_pack():
    # pick stock
    picked_list, picked_reason_list = fetch_technical_for_pick()
    file_name = "records/program_pick_records_"+database_function.get_last_trading_date_with_dash_from_db()+""
    file_process_function.write_pick_records(file_name, picked_list, picked_reason_list)

    man_picked_stock_code_list, man_picked_reason_list = human_pick(picked_list, picked_reason_list)
    human_pick_2nd_round(man_picked_stock_code_list, man_picked_reason_list)

# 抓bacl_test 出來，用來餵給人工篩選
def fetch_back_test_talbe_for_pick(start_date, end_date):
    conn = database_function.connectDB()
    sql = "select Date, stock_code, buyin_reason, price, 1_month_profit, 1_month_loss from back_test where buyin_reason like '%布林%' and buyin_reason like '%爆大量%' and stock_code not like '28%' and price <= 23 and price >= 0 and day(`Date`) in (24,25,9,1,16,14,11,3,10) and `Date` >= " + start_date +" and `Date` <= " + end_date + "order by `Date` "
    df = database_function.read_history_db_as_dataframe(sql, conn)
    grouped = df.groupby(df.stock_code)
    code_list = file_process_function.readList()
    picked_list = []
    picked_reason_list = []
    for i in code_list:
        tmp = grouped.get_group(i)
        # pick or not
        picked_list, picked_reason_list = pick_stock(tmp, picked_list, picked_reason_list)
        print(tmp)
    return picked_list, picked_reason_list

#手動回測
def manually_backtest_one_pack():
    global prdct
    # input start year month and end year month
    start_date = input("請輸入開始年月(yyyyMM) : ")
    end_date = input("請輸入結束年月(yyyyMM) : ")
    start_date_year = start_date[:4]
    start_date_month = start_date[4:]
    end_date_year = end_date[:4]
    end_date_month = end_date[4:]
    pick_strategy = input("請輸入選股策略 : ")
    auto_flag = input("是否自動(Y/N) : ")

    start_date = start_date + "01"
    end_date = end_date + "31"

    # find data within the time range
    conn = database_function.connectDB()

    sql = "select Date, stock_code, buyin_reason, price, 1_month_profit, 1_month_loss from back_test where buyin_reason like '%%布林%%' and buyin_reason like '%%爆大量%%' and stock_code not like '28%%' and price <= 23 and price >= 0 and day(`Date`) in (24,25,9,1,16,14,11,3,10) and `Date` >= " + start_date + " and `Date` <= " + end_date + " order by `Date` "
    df = database_function.read_history_db_as_dataframe(sql, conn)

    data_length = len(df.index)
    #
    n=1
    dict_buy_log = {}
    for index, row in df.iterrows():
        print("( " + str(n) + " / " + str(data_length) + " )")
        n += 1
        stock_code = row['stock_code']
        date = row['Date']
        buyin_reason = row['buyin_reason']
        one_month_profit = row['1_month_profit']
        one_month_loss = row['1_month_loss']
        price = row['price']
        # 這個月已經買過股票的話，跳過
        if(dict_buy_log.get(stock_code) == (str(date).replace("-",""))[:6]):
            print("already buy this stock, continue")
            continue
        if(auto_flag == "Y"):
            buy_or_drop = 'Buy'
        else:
            sql = "select * from technical_data where Date <= " + str(date).replace("-","") + " and StockCode = '" + stock_code + "' order by `Date` "
            print(sql)
            technical_df = database_function.read_history_db_as_dataframe(sql, conn)
            # print(technical_df)
            df_len = len(technical_df.index)
            if(df_len == 0):
                print("df_len == 0")
                continue
            pick_stock_function.draw(technical_df, buyin_reason)
            pick_stock_function.buy_or_drop()
            buy_or_drop = global_variable.button_result
            print(buy_or_drop)
        if(buy_or_drop == 'Buy'):
            sql = "insert into hand_pick_back_test(Date, stock_code, buyin_reason , price, `1_month_profit`, `1_month_loss`, strategy) Values("+str(date).replace("-","")+", '"+stock_code+"', '"+buyin_reason+"',"+str(price)+", "+str(one_month_profit)+", "+str(one_month_loss)+",'"+pick_strategy+"')"
            print(sql)
            conn.execute(sql)
            dict_buy_log[stock_code] = (str(date).replace("-",""))[:6]

# 賣出策略回測
def sell_strategy_backtest():
    conn = database_function.connectDB()
    sql = "select distinct strategy  from hand_pick_back_test"
    df = database_function.read_history_db_as_dataframe(sql, conn)
    print(df)

    data_set = input("請輸入data_set : ")


    sql = "select * from hand_pick_back_test where strategy = '"+data_set+"'"
    df = database_function.read_history_db_as_dataframe(sql, conn)
    df_len = len(df.index)
    count = 1
    #
    for index, row in df.iterrows():
        stock_code = row['stock_code']
        date = row['Date']
        price = row['price']
        bestcase_profit = row['1_month_profit']
        buyin_reason = row["buyin_reason"]
        pick_strategy = "購買原因包含「布林」或「爆大量」，股票代碼不以「28」開頭，股價介於0到23之間，日期為24、25、9、1、16、14、11、3或10日"
        end_date = next_month_end_date(date)
        sql = "select * from technical_data where StockCode = '"+stock_code+"' and `Date` > "+str(date).replace("-","")+" and `Date` <= " + str(end_date).replace("-","")
        print(sql)
        # print(str(date))
        print("("+str(count)+"/ "+str(df_len)+")")
        # break
        count += 1
        technical_df = database_function.read_history_db_as_dataframe(sql, conn)
        i = 0
        buyin_price = 0
        highest_price = 0
        lowest_price = 0
        buy_date = date
        price_dict = {}
        price_dict["mark_01"] = "false"
        for index2, row2 in technical_df.iterrows():
            # price_dict["open_price"] = row2['open']
            # high price也改成 close
            high_price = row2['close']
            low_price = row2['close']
            # current_low_price 可能要修改成 close
            price_dict["current_low_price"] = row2['close']
            price_dict["close_price"] = row2['close']
            price_dict["date"] = row2['Date']
            price_dict["price"] = price #前一天的close price
            close_price = row2['close']

            if(i == 0):
                buyin_price = row2['close']
                lowest_price = buyin_price
                buy_date = row2['Date']
                price_dict["buy_date"] = buy_date
                i += 1
            if(high_price > highest_price):
                highest_price = high_price
            price_dict["buyin_price"] = buyin_price
            price_dict["highest_price"] = highest_price
            if(low_price < lowest_price):
                lowest_price = low_price
            threshold = 999
            take_proit = 0
            stop_loss = 100
            hold_days = 30
            drawdown = 10
            # strategy_dict = sell_strategy_maximum_drawdown(price_dict,threshold,take_proit,stop_loss,hold_days)
            # strategy_dict = simple_maximum_drawdown(price_dict,drawdown,hold_days)
            strategy_dict = ladder_strategy(price_dict)
            # strategy_dict = test_strategy_01(price_dict)
            # if(strategy_dict["mark_01"] == "true"):
            #     price_dict["mark_01"]= "true"

            # strategy_dict = test_strategy_sell_at_certain_hold_days(price_dict)
            # strategy_dict = test_strategy_sell_if_lower_than_the_price(price_dict)
            # sell_strategy = "6%%_stoploss_other_last_day_v1"
            # sell_strategy = "sell_if_lower_than_the_price"
            # sell_strategy = "maximum_drawdown_"+str(stop_loss)+"%%stoploss_"+str(threshold)+"%%threshold_"+str(take_proit)+"%%takeprofit_"+str(hold_days)+"max_days"
            sell_strategy = "ladder_strategy_v3_"+str(hold_days)+"max_days"

            if(strategy_dict["sell"] == "true"):
                sell_date = row2['Date']
                sell_price = close_price
                hold_days = (sell_date - buy_date).days
                profit = float((sell_price - buyin_price) / buyin_price)
                bestcase_profit = float((highest_price - buyin_price) / buyin_price)
                worstcase_loss = float((lowest_price-buyin_price)/buyin_price)
                #bestcase_profit
                sell_reason = strategy_dict["sell_reason"]
                sql = "INSERT INTO sell_strategy_backtest_results (Date, buy_date, sell_date, hold_days, stock_code, buyin_price, sell_price, profit, bestcase_profit, worstcase_loss, buyin_reason, sell_reason, pick_strategy, sell_strategy, data_set) "+\
                "VALUES ("+str(date).replace("-","")+", "+ str(buy_date).replace("-","")+", "+str(sell_date).replace("-","")+", "+str(hold_days)+", '"+stock_code+"', "+str(buyin_price)+","+str(sell_price)+", "+str(profit)+", "+str(bestcase_profit)+", "+str(worstcase_loss)+", '"+buyin_reason+"', '"+\
                sell_reason+"', '"+pick_strategy+"', '"+sell_strategy+"', '"+data_set+"')"
                conn.execute(sql)
                print(str(date) +" "+stock_code + " "+ sell_reason)
                break
            elif(index2 == technical_df.shape[0] - 1):
                sell_reason = "last_day"
                sell_date = row2['Date']
                sell_price = close_price
                hold_days = (sell_date - buy_date).days
                profit = float((sell_price - buyin_price) / buyin_price)
                bestcase_profit = float((highest_price - buyin_price) / buyin_price)
                worstcase_loss = float((lowest_price - buyin_price) / buyin_price)
                sql = "INSERT INTO sell_strategy_backtest_results (Date, buy_date, sell_date, hold_days, stock_code, buyin_price, sell_price, profit, bestcase_profit, worstcase_loss, buyin_reason, sell_reason, pick_strategy, sell_strategy, data_set) "+ \
                 "VALUES (" + str(date).replace("-", "") + ", " + str(buy_date).replace("-", "") + ", " + str(
                    sell_date).replace("-", "") + ", " + str(hold_days) + ", '" + stock_code + "', " + str(
                    buyin_price) + ", " + str(sell_price) + ", " + str(profit) + ", " + str(
                    bestcase_profit) +", "+ str(worstcase_loss) + ", '" + buyin_reason + "', '" + \
                sell_reason + "', '" + pick_strategy + "', '" + sell_strategy + "', '"+ data_set+"')"
                conn.execute(sql)
                print(str(date) +" "+stock_code + " "+ sell_reason)
                break


#

# 最大回撤策略
# 買入價格-4%止損
# 怎麼止盈呢？
# 上漲超過 x%時，利潤減少超過y%時止盈
# 若未下跌，在最後一個交易日賣出
# buyin_price, highest_price, current_low_price, sell
# 總是以收盤價做交易
def sell_strategy_maximum_drawdown(price_dict,threshold, take_proit, stop_loss, max_hold_days):
    x = threshold
    y = take_proit
    z = stop_loss
    buyin_price = price_dict["buyin_price"]
    highest_price = price_dict["highest_price"]
    current_low_price = price_dict["current_low_price"]
    price_dict["sell"] = "false"

    if(current_low_price < buyin_price*float((100-z)/100)):
        price_dict["sell"] = "true"
        price_dict["sell_reason"] = "stop_loss"
        return price_dict
    if(highest_price >= buyin_price * float((100 + x)/100) ):
        if(current_low_price <= buyin_price + (highest_price - buyin_price) * float((100-y)/100) ):
            price_dict["sell"] = "true"
            price_dict["sell_reason"] = "take_proit"
    #
    buy_date = price_dict["buy_date"]
    current_date = price_dict["date"]
    hold_days = (current_date - buy_date).days
    if (hold_days >= max_hold_days):
        price_dict["sell"] = "true"
        price_dict["sell_reason"] = "hold_"+str(max_hold_days)+"_days"

    return price_dict

def simple_maximum_drawdown(price_dict,max_drawdown, max_hold_days):
    buyin_price = price_dict["buyin_price"]
    highest_price = price_dict["highest_price"]
    current_low_price = price_dict["current_low_price"]
    close_price = price_dict["close_price"]
    buy_date = price_dict["buy_date"]
    current_date = price_dict["date"]
    hold_days = (current_date - buy_date).days
    price_dict["sell"] = "false"
    if(current_low_price < highest_price* float((100 - max_drawdown)/100)):
        price_dict["sell"] = "true"
        price_dict["sell_reason"] = "maximum_drawdown"
    if (hold_days >= max_hold_days):
        price_dict["sell"] = "true"
        price_dict["sell_reason"] = "hold_"+str(max_hold_days)+"_days"
    return price_dict

def exponential_take_gain(highest_gain_percent):
    if (highest_gain_percent > 50):
        return 10
    if (highest_gain_percent > 25):
        return 20
    if (highest_gain_percent > 16):
        return 30
    if (highest_gain_percent > 12):
        return 40
    # if (highest_gain_percent > 10):
    #     return 50
    # if (highest_gain_percent > 8):
    #     return 60
    # if (highest_gain_percent > 7):
    #     return 70
    # if (highest_gain_percent > 6):
    #     return 80
    # if (highest_gain_percent > 5):
    #     return 90
    return 1000

def ladder_strategy(price_dict):
    buyin_price = price_dict["buyin_price"]
    current_price = price_dict["current_low_price"]
    highest_price = price_dict["highest_price"]
    price_dict["sell"] = "false"
    hightest_percent = float((highest_price - buyin_price)/buyin_price)
    print("hightest_percent = " + str(hightest_percent))
    # if ladder keeps going down, there's noting we can do, other than wait for it climb
    # target top-cut
    trend = ""
    if(current_price > buyin_price):
        trend = "up"
        take_gain_percent = float(exponential_take_gain(float(hightest_percent)*100) / 100)
        if (current_price < highest_price):
            drawback_percent = (highest_price - current_price) / (highest_price - buyin_price)
            if(drawback_percent > take_gain_percent):
                price_dict["sell"] = "true"
                price_dict["sell_reason"] = str(take_gain_percent)+"take_gain"
                return price_dict
    elif(current_price < buyin_price):
        trend = "down"
    #
    mark_01 = price_dict["mark_01"]

    if (current_price < buyin_price * float((100 - 4) / 100)):
        # 先漲後跌處理
        if (highest_price >= buyin_price * 1.05):
            price_dict["sell"] = "true"
            price_dict["sell_reason"] = "rise_then_fall_stop_loss"
            return price_dict
        else:
            price_dict["mark_01"] = "true"
    #


    buy_date = price_dict["buy_date"]
    current_date = price_dict["date"]
    hold_days = (current_date - buy_date).days
    if (hold_days >= 30):
        price_dict["sell"] = "true"
        price_dict["sell_reason"] = "hold_" + str(30) + "_days"
    # if ladder goning down and rebound
    return price_dict
    # if ladder keeps climbing, leave when it's not
    # if ladder is not doing alot, just let it be.

# 測試策略，未命名01
# 撞到-4%之後(mark_01)，如果漲到 5%的話，停利
# 其餘先放生
def test_strategy_01(price_dict):
    buyin_price = price_dict["buyin_price"]
    highest_price = price_dict["highest_price"]
    current_low_price = price_dict["current_low_price"]
    close_price = price_dict["close_price"]
    price_dict["sell"] = "false"
    mark_01 = price_dict["mark_01"]

    if (current_low_price < buyin_price * float((100 - 4) / 100)):
        # 先漲後跌處理
        if(highest_price >= buyin_price * 1.05):
            price_dict["sell"] = "true"
            price_dict["sell_reason"] = "rise_then_fall_stop_loss"
            return price_dict
        else:
            price_dict["mark_01"] = "true"
    if((close_price >= buyin_price * float(1.05)) & (price_dict["mark_01"] == "true")):
        price_dict["sell"] = "true"
        price_dict["sell_reason"] = "rebound_take_gain"
    if(current_low_price < buyin_price * float(0.94)):
        price_dict["sell"] = "true"
        price_dict["sell_reason"] = "stop_loss"
    return price_dict

def test_strategy_sell_at_certain_hold_days(price_dict):
    buy_date = price_dict["buy_date"]
    current_date = price_dict["date"]
    price_dict["sell"] = "false"
    hold_days = (current_date - buy_date).days
    if(hold_days >=18):
        price_dict["sell"] = "true"
        price_dict["sell_reason"] = "hold_30_days"
    return price_dict

def test_strategy_sell_if_lower_than_the_price(price_dict):
    close_price = price_dict["close_price"]
    price = price_dict["price"]
    price_dict["sell"] = "false"
    if(close_price < price):
        price_dict["sell"] = "true"
        price_dict["sell_reason"] = "stop_loss"
    return price_dict

def next_month_end_date(current_date):
    # 下個月的第一天
    next_month = current_date + relativedelta(months=1)
    # 下個月的第一天減去一天，即為下個月月底
    first_day_of_next_month = next_month.replace(day=1)
    year = first_day_of_next_month.year
    month = first_day_of_next_month.month
    number_of_days = calendar.monthrange(year, month)[1]
    last_day_of_next_month = first_day_of_next_month.replace(day=number_of_days)
    # last_day_of_next_month = first_day_of_next_month - relativedelta(days=1)

    return last_day_of_next_month

def is_weekend():
    i = datetime.datetime.today().weekday()
    print("Today is "+ str(i))
    if((i == 5) or (i==6)):
        print("is_weekend return True")
        return True
    else:
        print("is_weekend return False")
        return False

def schedule_auto_update_everyday_data():
    now = datetime.datetime.now().time()
    then = datetime.datetime.now().time().replace(hour=16, minute=00, second=00)
    delta = datetime.datetime.combine(datetime.datetime.min,then) - datetime.datetime.combine(datetime.datetime.min,now)
    print("sleep " + str(delta.seconds) + " seconds")
    time.sleep(int(delta.seconds))
    while(True):
        # update task
        if(True):
            print("start : everyday_stock_data_update")
            data_process_function.everyday_stock_data_update()
            print("end : everyday_stock_data_update")
            print("start : historical_to_technical_one_pack")
            #historical_to_technical_one_pack()
            print("end : historical_to_technical_one_pack")
            print("start : predict_update")
            #predict_update()
            print("end : predict_update")
            print("start : three_major_leagal_person_intergrate")
            three_major_leagal_person_intergrate()
            print("end : three_major_leagal_person_intergrate")
        # get sleep time
        now = datetime.datetime.now().time()
        then = datetime.datetime.now().time().replace(hour=16, minute=00, second=00)
        delta = datetime.datetime.combine(datetime.datetime.min, then) - datetime.datetime.combine(datetime.datetime.min, now)
        #
        time.sleep(int(delta.seconds))



def main():
    global prdct

    while True:
        user_interface()


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
