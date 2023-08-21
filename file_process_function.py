# 放置處理檔案的function
import os

import pandas as pd
import datetime
from urllib import request
import time

# 櫃買資料處理
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
    # open the input file in write mo   de
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

# 去除多餘的行數
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

def change_encoding(filename):
    path, filename = os.path.split(filename)
    with open(os.path.join(path, filename), 'rb') as source_file:
        with open(os.path.join("processed", "reEncode_" + filename), 'w+b') as dest_file:
            contents = source_file.read()
            dest_file.write(contents.decode('cp950').encode('utf-8'))

def process_counter_data(filename, date):
    # change encoding from big5 to utf8
    change_encoding(filename)
    # remove lines that are unnecessary and have problem to transfer to dataframe
    path, filename = os.path.split(filename)
    filename = "reEncode_" + filename
    remove_excess_line(os.path.join("processed", filename))
    # file to dataframe
    df = read_conter_cvs_file(os.path.join("processed", filename), date)
    return df

def process_listed_data(filename, date):
    df = read_listed_cvs_file(filename, date)
    return df

# 處理三大法人資料
def process_three_major_leagal_person_data(filename):
    # read input file
    fin = open(filename, "rt", encoding="cp950")
    # read file contents to string
    data = fin.read()
    # replace all occurrences of the required string
    data = data.replace('","', '" "')
    data = data.replace(',', '')
    # close the input file
    fin.close()
    # open the input file in write mode
    fin = open(filename, "wt", encoding="cp950")
    # overrite the input file with the resulting data
    fin.write(data)
    # close the file
    fin.close()
    return 0

# 刪除上市權證
def remove_warrant_from_csv(filename):
    with open(filename, "r") as f:
        lines = f.readlines()
    with open(filename, "w") as f:
        for line in lines:
            if (len(line.strip("\n"))>1 and  line.strip("\n")[0] != "="):
                f.write(line)
    return 0

# 刪除多餘行數 ，目前沒用
# def delete_excess_line_from_three_major_leagal_person_data(mode):
#     # 上市
#     if(mode == 1):
#         #刪除前一行，跟最後9行
#         lines = []
#         with open(filename, 'r', encoding="utf8") as fp:
#             # read an store all lines into list
#             lines = fp.readlines()
#         lines = lines[1:len(lines) - 9]
#         with open(filename, 'w', encoding="utf8") as fp:
#             for line in lines:
#                 fp.write(line)
#     # 上櫃
#     elif(mode == 2):
#         # 刪除前一行
#         lines = []
#         with open(filename, 'r', encoding="utf8") as fp:
#             # read an store all lines into list
#             lines = fp.readlines()
#         lines = lines[1:len(lines)]
#         with open(filename, 'w', encoding="utf8") as fp:
#             for line in lines:
#                 fp.write(line)
#     return 0

def readList():
    codeList = []
    with open('ListedStockList.txt', 'r', encoding="utf8") as f:
        for line in f.readlines():
            spLine = line.split()
            if (spLine[0].isnumeric()):
                codeList.append(spLine[0])
    return codeList

# 下載每日盤後資訊
def getDayTradeData():
    # download today stock data
    # 上市
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data"
    today = datetime.date.today()
    success_flag = False
    while(success_flag==False):
        try:
            request.urlretrieve(url, "origin/上市盤後資訊_" + str(today) + ".csv")
            success_flag = True
        except:
            time.sleep(10)
    time.sleep(20)
    # 上櫃
    url2 = "http://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_download.php?l=zh-tw&se=EW"
    try:
        request.urlretrieve(url2, "origin/上櫃盤後資訊_" + str(today) + ".csv")
    except:
        time.sleep(10)
        request.urlretrieve(url2, "origin/上櫃盤後資訊_" + str(today) + ".csv")

# 下載特定日期盤後資訊
def getCertainDayTradeData():
    # download Certain today stock data
    CertainDate = input("請輸入日期(yyyyMMdd) : ")
    # 西元年轉民國年
    ADyear = CertainDate[:4]
    ROCyear = str(int(ADyear) - 1911)
    month = CertainDate[4:6]
    day = CertainDate[6:]
    FormatedDate = ADyear + "-" + month + "-" + day
    #
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=" + CertainDate + "&type=ALL"
    # today = datetime.date.today()
    success_flag = False
    while(success_flag==False):
        try:
            request.urlretrieve(url, "origin/上市盤後資訊_" +FormatedDate + ".csv")
            success_flag = True
        except:
            time.sleep(10)
    time.sleep(20)

    url2 = "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&o=csv&d="+ ROCyear +"/"+ month +"/"+day +"&s=0,asc,0"
    try:
        request.urlretrieve(url2, "origin/上櫃盤後資訊_" + FormatedDate + ".csv")
    except:
        time.sleep(10)
        request.urlretrieve(url2, "origin/上櫃盤後資訊_" + FormatedDate + ".csv")

    # process
    process_counter_data("origin/上櫃盤後資訊_" + FormatedDate + ".csv", FormatedDate)
    process_listed_data("origin/上市盤後資訊_" + FormatedDate + ".csv", FormatedDate)

#下載每日三大法人資料
def download_daily_three_major_leagal_person_data():
    # 上市資料(證交所)
    # today = datetime.datetime.strptime("2022-10-24","%Y-%m-%d").date()
    today = datetime.date.today()
    print(str(today))
    today_just_number = str(today).replace("-","")
    url = "https://www.twse.com.tw/fund/T86?response=csv&date="+today_just_number+"&selectType=ALL"
    print("start download daily legal person data")
    print(url)
    success_flag = False
    while(success_flag == False):
        try:
            request.urlretrieve(url, "LegalPerson/上市三大法人_" + str(today) + ".csv")
            success_flag = True
        except:
            time.sleep(5)
    time.sleep(30)
    # 上櫃資料(櫃買中心)
    # https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d=111/04/15&s=0,asc
    url2 = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d="
    print(url2)
    try:
        request.urlretrieve(url2, "LegalPerson/上櫃三大法人_" + str(today) + ".csv")
    except:
        time.sleep(10)
        request.urlretrieve(url2, "LegalPerson/上櫃三大法人_" + str(today) + ".csv")
    time.sleep(30)
    print("download daily legal person data success")
    return 0

def writeFailedRecord(failList):
    with open('failedList.txt', 'a', encoding="utf8") as f:
        for i in failList:
            f.write(str(i) + '\n')

def writeSuccRecord(stockCode):
    with open('finishRecord.txt', 'w', encoding="utf8") as f:
        f.write(stockCode + '\n')