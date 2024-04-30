import threading
from threading import Timer
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

import data_process_function

lock = threading.Lock()
lockForSql = threading.Lock()

def insertToDB(df, conn):
    lockForSql.acquire()
    df.to_sql('historical_data', conn, if_exists='append', index=False)
    lockForSql.release()


def insert_to_tech_db(df, conn):
    lockForSql.acquire()
    df.to_sql('technical_data', conn, if_exists='append', index=False)
    lockForSql.release()

def insert_to_backtest_db(df, conn):
    lockForSql.acquire()
    df.to_sql('back_test', conn, if_exists='append', index=False)
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

def delete_old_table():
    conn = connectDB()
    SQL = "DELETE FROM `technical_data`"
    conn.execute(SQL)

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
    df = data_process_function.add_talib_info(df)
    # inset in to technical data
    df = df.replace([np.inf, -np.inf], np.nan)
    insert_to_tech_db(df, conn)
    print(df)

def read_history_db_as_dataframe(SQL, conn):
    # conn = connectDB()
    df = pd.read_sql(SQL, con=conn)
    # print(df.to_string)
    return df

def get_last_trading_date_from_db():
    conn = connectDB()
    sql = "SELECT max(Date) as Date FROM `historical_data` ORDER BY `historical_data`.`Date` DESC"
    df = read_history_db_as_dataframe(sql, conn)
    return(str(df.Date[0]).replace("-", ""))

def get_last_trading_date_with_dash_from_db():
    conn = connectDB()
    sql = "SELECT max(Date) as Date FROM `historical_data` ORDER BY `historical_data`.`Date` DESC"
    df = read_history_db_as_dataframe(sql, conn)
    return(str(df.Date[0]))

def predict_update():
    # get date order and data from technical_data
    conn = connectDB()
    sql = "SELECT DISTINCT Date FROM `historical_data` ORDER BY `historical_data`.`date` DESC"
    date_df = read_history_db_as_dataframe(sql, conn)
    today = str(date_df.Date.iloc[0]).replace("-", "")
    yesterday = str(date_df.Date.iloc[1]).replace("-", "")
    bf_5_day = str(date_df.Date.iloc[4]).replace("-", "")
    bf_10_day = str(date_df.Date.iloc[9]).replace("-", "")
    bf_20_day = str(date_df.Date.iloc[19]).replace("-", "")
    bf_60_day = str(date_df.Date.iloc[59]).replace("-", "")
    bf_120_day = str(date_df.Date.iloc[119]).replace("-", "")
    date_list = [bf_5_day, bf_10_day, bf_20_day, bf_60_day, bf_120_day]

    # update yesterday predict
    print("update yesterday predict")
    sql = "SELECT * FROM predict_log WHERE Date =" + yesterday + " AND after_1_day = 0"
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
    j = 0
    for day in date_list:
        print("update " + db_column_list[j] + " predict")
        # 找要更新的股票代碼
        sql = "SELECT * FROM predict_log WHERE Date = " + day + " AND " + db_column_list[j] + " = 0"
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
                print(str(percentage))
                #
                sql = "UPDATE `predict_log` SET " + db_column_list[j] + " = " + str(
                    percentage) + " WHERE Date = " + day + " AND StockCode = '" + i + "'"
                print(sql)
                conn.execute(sql)
            except Exception as e:
                print(e)
                print("今日可能沒有交易")
            print("( " + str(n + 1) + " / " + str(length) + " )")
            n = n + 1
        j += 1

    # update
    return 0