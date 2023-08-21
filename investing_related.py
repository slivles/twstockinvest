import investpy
import datetime
import database_function

def writeSuccRecord(stockCode):
    with open('finishRecord.txt', 'w', encoding="utf8") as f:
        f.write(stockCode + '\n')

def fetchData(stock_code):
    df = investpy.get_stock_historical_data(stock=stock_code,
                                            country='Taiwan',
                                            from_date='01/01/2010',
                                            to_date='24/10/2021')
    df['StockCode'] = stock_code
    df['Date'] = df.index
    print(df.head())
    # print(df.index)
    # print(df)
    writeSuccRecord(stock_code)

    return df

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
            database_function.insertToDB(df, conn)
        except:
            writeFailedRecord(codeList[i])
            print("Fetch " + codeList[i] + " Failed")

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

def writeFailedRecord(failList):
    with open('failedList.txt', 'a', encoding="utf8") as f:
        for i in failList:
            f.write(str(i) + '\n')

