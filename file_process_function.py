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

def read_appended_conter_cvs_file(filename, date):
    df = pd.read_csv(filename, encoding="utf-8", sep=' ', header=None)
    df = df.drop([7, 11, 12, 13, 14, 15, 16, 17, 18], 1)
    df = df.rename(
        columns={0: 'StockCode', 1: 'name', 2: 'close', 3: 'changeAct', 4: 'open', 5: 'high', 6: 'low', 7: 'volume',
                 8: 'TradeValue', 9: 'transaction'}, inplace=False)
    df["Date"] = str(date).replace("-", "")
    # print(df.to_string())
    return df

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

def read_appended_listed_cvs_file(filename, date):
    # 追加上市資料讀取
    df = pd.read_csv(filename, encoding="utf-8", sep=',', header=None)
    df = df.drop([ 9, 11, 12, 13, 14, 15, 16], 1)
    df = df.rename(
        columns={0: 'StockCode', 1: 'name', 2: 'volume',3:'transaction', 4: 'TradeValue' , 5: 'open', 6: 'high', 7: 'low', 8: 'close',
                 9: 'changeAct', 10: 'changeAct'}, inplace=False)
    df = df.fillna(0)
    df["Date"] = str(date).replace("-", "")
    print(df.to_string())
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

def remove_first_3_line(filename):
    #     delete 1~3 line and last line
    lines = []
    with open(filename, 'r', encoding="utf8") as fp:
        # read an store all lines into list
        lines = fp.readlines()
    lines = lines[3:len(lines) - 1]
    with open(filename, 'w', encoding="utf8") as fp:
        for line in lines:
            fp.write(line)

def change_encoding(filename):
    path, filename = os.path.split(filename)
    with open(os.path.join(path, filename), 'rb') as source_file:
        with open(os.path.join("processed", "reEncode_" + filename), 'w+b') as dest_file:
            contents = source_file.read()
            dest_file.write(contents.decode('cp950').encode('utf-8'))

def replace_text_in_file(filename,old_string,new_string):
    # 打开文件以供读取
    with open(filename, 'r', encoding="utf8" ) as file:
        file_contents = file.read()

    # 从文件内容中替换所有出现的特定字符串
    new_contents = file_contents.replace(old_string, new_string)

    # 将新内容写回文件
    with open(filename, 'w', encoding="utf8") as file:
        file.write(new_contents)

def remove_content_after_certain_line(filename,string):
    # 打开文件以供读取
    with open(filename, 'r', encoding="utf8") as file:
        lines = file.readlines()

    # 指定要删除的特定句子
    sentence_to_delete = string

    # 查找特定句子的索引
    index_to_delete = -1
    for i, line in enumerate(lines):
        if sentence_to_delete in line:
            index_to_delete = i
            break

    # 如果找到特定句子，删除特定句子及其之后的内容
    if index_to_delete != -1:
        del lines[index_to_delete:]

    # 将新内容写回文件
    with open(filename, 'w', encoding="utf8") as file:
        file.writelines(lines)

def remove_line_with_certain_start(filename,string):
    # 打开文件以供读取
    with open(filename, 'r', encoding="utf8") as file:
        lines = file.readlines()

    # 指定要删除的特定开头
    starting_text_to_delete = string

    # 从文件中删除以特定开头的句子
    lines = [line for line in lines if not line.startswith(starting_text_to_delete)]

    # 将新内容写回文件
    with open(filename, 'w', encoding="utf8") as file:
        file.writelines(lines)


def process_counter_data(filename, date):
    # change encoding from big5 to utf8
    change_encoding(filename)
    # remove lines that are unnecessary and have problem to transfer to dataframe
    path, filename = os.path.split(filename)
    filename.replace("BIG5","UTF-8")
    filename = "reEncode_" + filename
    remove_excess_line(os.path.join("processed", filename))

    # file to dataframe
    df = read_conter_cvs_file(os.path.join("processed", filename), date)
    return df

def process_appended_counter_data(filename, date):
    # change encoding from big5 to utf8
    change_encoding(filename)
    filename.replace("BIG5", "UTF-8")
    path, filename = os.path.split(filename)
    filename = "reEncode_" + filename
    # remove lines that are unnecessary and have problem to transfer to dataframe
    remove_first_3_line(os.path.join("processed", filename))
    #去除數字中間的逗號，分隔符號改成空白
    counter_data_preprocess(os.path.join("processed", filename))
    #將沒有數字的資料改成數字
    replace_text_in_file(os.path.join("processed", filename), " ---", "0")
    replace_text_in_file(os.path.join("processed", filename), "--- ", "0")
    replace_text_in_file(os.path.join("processed", filename), "---", "0")
    #去除檔案最後多餘的資料
    remove_content_after_certain_line(os.path.join("processed", filename),"管理股票")
    # file to dataframe
    df = read_appended_conter_cvs_file(os.path.join("processed", filename), date)
    return df

def process_appended_listed_data(filename, date):
    # change encoding from big5 to utf8
    change_encoding(filename)
    filename.replace("BIG5", "UTF-8")
    path, filename = os.path.split(filename)
    filename = "reEncode_" + filename
    # remove lines that are unnecessary and have problem to transfer to dataframe
    remove_content_before_certain_line(os.path.join("processed", filename), '"證券代號","證券名稱","成交股數","成交筆數","成交金額","開盤價"')
    # 移除= 開頭的資料(大部分是權證)
    remove_line_with_certain_start(os.path.join("processed", filename),"=")
    # 將沒有數字的資料改成數字
    replace_text_in_file(os.path.join("processed", filename), "--", "0")
    # file to dataframe
    df = read_appended_listed_cvs_file(os.path.join("processed", filename), date)
    return df
    # return 0

def remove_text_from_file(filename, text):
    with open(filename, 'r', encoding="utf8") as file:
        file_contents = file.read()

    # 指定要删除的特定字符串
    string_to_remove = text

    # 从文件内容中删除所有出现的特定字符串
    new_contents = file_contents.replace(string_to_remove, '')

    # 将新内容写回文件
    with open(filename, 'w', encoding="utf8") as file:
        file.write(new_contents)


def remove_content_before_certain_line(filename, text):
    with open(filename, 'r', encoding="utf8") as file:
        lines = file.readlines()
    # 查找要删除的句子之前的行数
    index_to_delete = -1
    for i, line in enumerate(lines):
        if text in line:
            index_to_delete = i
            break

    # 如果找到了要删除的句子，删除之前的内容
    if index_to_delete >= 0:
        new_lines = lines[index_to_delete + 1:]
        with open(filename, 'w', encoding="utf8") as file:
            file.writelines(new_lines)


def process_listed_data(filename, date):
    df = read_listed_cvs_file(filename, date)
    return df

def process_listed_BIG5_data(filename):
    change_encoding(filename)
    path, filename = os.path.split(filename)
    remove_excess_line(os.path.join("processed", filename))

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
    success_flag = False
    while(success_flag==False):
        try:
            request.urlretrieve(url, "origin/上市盤後資訊_BIG5_" +FormatedDate + ".csv")
            success_flag = True
        except:
            time.sleep(10)
    time.sleep(20)

    url2 = "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&o=csv&d="+ ROCyear +"/"+ month +"/"+day +"&s=0,asc,0"
    try:
        request.urlretrieve(url2, "origin/上櫃盤後資訊_BIG5_" + FormatedDate + ".csv")
    except:
        time.sleep(10)
        request.urlretrieve(url2, "origin/上櫃盤後資訊_BIG5_" + FormatedDate + ".csv")

    # process
    process_appended_counter_data("origin/上櫃盤後資訊_BIG5_" + FormatedDate + ".csv", FormatedDate)
    process_appended_listed_data("origin/上市盤後資訊_BIG5_" + FormatedDate + ".csv", FormatedDate)
    # process_listed_data("origin/上市盤後資訊_BIG5_" + FormatedDate + "_.csv", FormatedDate)

#下載每日三大法人資料
def download_daily_three_major_leagal_person_data():
    # 上市資料(證交所)
    # today = datetime.datetime.strptime("2022-10-24","%Y-%m-%d").date()
    today = datetime.date.today()
    print(str(today))
    today_just_number = str(today).replace("-","")
    url = "https://www.twse.com.tw/rwd/zh/fund/T86?date="+today_just_number+"&selectType=ALL&response=csv"
    # "https://www.twse.com.tw/rwd/zh/fund/T86?date=20230905&selectType=ALL&response=csv"
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