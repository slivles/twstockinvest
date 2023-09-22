import datetime
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
from apscheduler.schedulers.background import BackgroundScheduler



# 程式
import database_function
import investing_related
import file_process_function
import data_process_function
from pick_stock_function import pick_stock, human_pick_2nd_round, human_pick

np.seterr(divide='ignore', invalid='ignore')  # 忽略warning
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
fig = plt.figure(figsize=(20, 12), dpi=100, facecolor="white")  # 创建fig对象

lock = threading.Lock()
lockForSql = threading.Lock()

failList = []

#global variable
prdct = ""

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
    print("4. end")
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
        d = input("choose : ")
        if (d == "1"):
            print("start : everyday_stock_data_update")
            data_process_function.everyday_stock_data_update()
            print("end : everyday_stock_data_update")
        elif (d == "2"):
            print("start : historical_to_technical_one_pack")
            data_process_function.historical_to_technical_one_pack()
            print("end : historical_to_technical_one_pack")
        elif (d == "3"):
            print("start : predict_update")
            database_function.predict_update()
            print("end : predict_update")
        elif (d == "4"):
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
        elif (d == "5"):
            print("start thread : schedule_auto_update_everyday_data")
            scheduler = BackgroundScheduler(timezone="Asia/Taipei")
            scheduler.add_job(data_process_function.everyday_stock_data_update, 'cron', day_of_week='0-4', hour=16, minute=00)
            scheduler.start()
            # t = threading.Thread(target=schedule_auto_update_everyday_data)  #
            # t.start()  # 開始
        elif (d == "6"):
            print("start : insert_csv_data_from_date()")
            data_process_function.insert_csv_data_from_date()
            print("end : insert_csv_data_from_date()")
        elif (d == "7"):
            print("start : three_major_leagal_person_intergrate()")
            three_major_leagal_person_intergrate()
            print("end : three_major_leagal_person_intergrate()")
        elif (d == "8"):
            print("start : getCertainDayTradeData()")
            file_process_function.getCertainDayTradeData()
            print("end : getCertainDayTradeData()")
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
        print("program end.")
        quit()
    return 0

def pick_stock_one_pack():
    # pick stock
    picked_list, pciked_reason_list = fetch_technical_for_pick()
    man_picked_stock_code_list, man_picked_reason_list = human_pick(picked_list, pciked_reason_list)
    human_pick_2nd_round(man_picked_stock_code_list, man_picked_reason_list)
    three_major_leagal_person_intergrate()

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
