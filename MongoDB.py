from pymongo import MongoClient
from datetime import datetime, tzinfo, timezone, timedelta
import pandas as pd


def load_realtime_cct():
    client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
    filter = {}
    project = {
        'data.results.CCT': 1,
        'datetime': 1
    }
    sort = list({
                    'datetime': -1
                }.items())
    limit = 1

    result = client['nl_witlab']['cas'].find(
        filter=filter,
        projection=project,
        sort=sort,
        limit=limit
    )
    dic_list = []
    dic = dict()
    for i in result:
        dic = i
        dic_list.append(dic)

    return dic_list
def load_last100_cct(times):
    client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
    filter = {
        'datetime': {
            '$gte': datetime(int(times[0][0:4]), int(times[0][5:7]), int(times[0][8:10]),
                             int(times[0][11:13]), int(times[0][14:16]), int(times[0][17:19]), tzinfo= timezone.utc),
            '$lte': datetime(int(times[1][0:4]), int(times[1][5:7]), int(times[1][8:10]),
                             int(times[1][11:13]), int(times[1][14:16]), int(times[1][17:19]), tzinfo= timezone.utc)
        }
    }
    project = {
        'datetime': 1,
        'data.results.CCT': 1
    }
    sort = list({
                    'datetime': 1
                }.items())
    limit = 100

    result = client['nl_witlab']['cas'].find(
        filter=filter,
        projection=project,
        sort=sort,
        limit=limit
    )
    dic_list = []
    dic = dict()
    for i in result:
        dic = i
        dic_list.append(dic)

    return dic_list

def load_last101_cct(times):
    client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
    filter = {
        'datetime': {
            '$gte': datetime(int(times[0][0:4]), int(times[0][5:7]), int(times[0][8:10]),
                             int(times[0][11:13]), int(times[0][14:16]), int(times[0][17:19]), tzinfo= timezone.utc),
            '$lte': datetime(int(times[1][0:4]), int(times[1][5:7]), int(times[1][8:10]),
                             int(times[1][11:13]), int(times[1][14:16]), int(times[1][17:19]), tzinfo= timezone.utc)
        }
    }
    project = {
        'datetime': 1,
        'data.results.CCT': 1
    }
    sort = list({
                    'datetime': 1
                }.items())
    limit = 101

    result = client['nl_witlab']['cas'].find(
        filter=filter,
        projection=project,
        sort=sort,
        limit=limit
    )
    dic_list = []
    dic = dict()
    for i in result:
        dic = i
        dic_list.append(dic)

    return dic_list

def time_to_time_cct(times):
    client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
    filter = {
        'datetime': {
            '$gte': datetime(int(times[0][0:4]), int(times[0][5:7]), int(times[0][8:10]),
                             int(times[0][11:13]), int(times[0][14:16]), int(times[0][17:19]), tzinfo= timezone.utc),
            '$lte': datetime(int(times[1][0:4]), int(times[1][5:7]), int(times[1][8:10]),
                             int(times[1][11:13]), int(times[1][14:16]), int(times[1][17:19]), tzinfo= timezone.utc)
        }
    }
    project = {
        'data.results.CCT': 1,
        'datetime': 1
    }
    sort = list({
                    'datetime': 1
                }.items())

    result = client['nl_witlab']['cas'].find(
        filter=filter,
        projection=project,
        sort=sort
    )
    dic_list = []
    dic = dict()
    for i in result:
        dic = i
        dic_list.append(dic)

    return dic_list

def load_last1_cct1(times):
    client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
    filter = {
        'datetime': {
            '$gte': datetime(int(times[0][0:4]), int(times[0][5:7]), int(times[0][8:10]),
                             int(times[0][11:13]), int(times[0][14:16]), int(times[0][17:19]), tzinfo= timezone.utc)
        }
    }
    project = {
        'datetime': 1,
        'data.results.CCT': 1
    }
    sort = list({
                    'datetime': 1
                }.items())
    limit = 2

    result = client['nl_witlab']['cas'].find(
        filter=filter,
        projection=project,
        sort=sort,
        limit=limit
    )
    dic_list = []
    dic = dict()
    for i in result:
        dic = i
        dic_list.append(dic)

    return dic_list




def make_ktc_to_utc(year, month, day):
    ori_date = year+"/"+month+"/"+day
    start_time = ori_date+" 00:00:00"
    end_time = ori_date+" 11:59:59"

    start_time_obj = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    end_time_obj = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')
    start_time_obj = start_time_obj - timedelta(hours=9)
    end_time_obj = end_time_obj - timedelta(hours=9)

    utc_start = start_time_obj.strftime('%Y/%m/%d %H:%M:%S')
    utc_stop = end_time_obj.strftime('%Y/%m/%d %H:%M:%S')
    # print(utc_start)
    # print(utc_stop)
    return [utc_start,utc_stop]

def make_ktc_to_utc2(year, month, day):
    ori_date = year+"/"+month+"/"+day
    start_time = ori_date+" 12:00:00"
    end_time = ori_date+" 23:59:59"

    start_time_obj = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    end_time_obj = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')
    start_time_obj = start_time_obj - timedelta(hours=9)
    end_time_obj = end_time_obj - timedelta(hours=9)

    utc_start = start_time_obj.strftime('%Y/%m/%d %H:%M:%S')
    utc_stop = end_time_obj.strftime('%Y/%m/%d %H:%M:%S')
    # print(utc_start)
    # print(utc_stop)
    return [utc_start,utc_stop]

def make_ktc_to_utc_4boot(start):
    time = str(start)
    time = time.replace(" ","T")
    time = time.replace("/","-")

    temp = time.split("T")
    date = temp[0].split("-")

    ori_date = date[0]+"/"+date[1]+"/"+date[2]
    start_time = ori_date+" "+temp[1].split(".")[0]

    start_time_obj = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    start_time_obj = start_time_obj + timedelta(hours=9)

    start_time = start_time_obj.strftime('%Y/%m/%d %H:%M:%S')
    end_time = start_time.split(" ")[0]+" 11:59:59"

    start_time_obj = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    end_time_obj = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')
    start_time_obj = start_time_obj - timedelta(hours=9)
    end_time_obj = end_time_obj - timedelta(hours=9)

    utc_start = start_time_obj.strftime('%Y/%m/%d %H:%M:%S')
    utc_stop = end_time_obj.strftime('%Y/%m/%d %H:%M:%S')
    # print(utc_start)
    # print(utc_stop)
    return [utc_start,utc_stop]


def make_ktc_to_utc_4step(time,end):
    time = str(time)
    time = time.replace(" ","T")
    time = time.replace("/","-")

    temp = time.split("T")
    date = temp[0].split("-")

    ori_date = date[0]+"/"+date[1]+"/"+date[2]
    start_time = ori_date+" "+temp[1].split(".")[0]

    time = str(end)
    time = time.replace(" ","T")
    time = time.replace("/","-")

    temp = time.split("T")
    date = temp[0].split("-")
    ori_date = date[0]+"/"+date[1]+"/"+date[2]
    end_time = ori_date+" "+temp[1].split(".")[0]

    start_time_obj = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    end_time_obj = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')

    # start_time = start_time_obj.strftime('%Y/%m/%d %H:%M:%S')
    # end_time = end_time_obj.strftime('%Y/%m/%d %H:%M:%S')
    #
    # start_time_obj = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    # end_time_obj = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')

    utc_start = start_time_obj.strftime('%Y/%m/%d %H:%M:%S')
    utc_stop = end_time_obj.strftime('%Y/%m/%d %H:%M:%S')
    # print(utc_start)
    # print(utc_stop)
    return [utc_start,utc_stop]

def make_ktc_to_utc_4step1(time):
    time = str(time)
    time = time.replace(" ","T")
    time = time.replace("/","-")
    temp = time.split("T")
    # print(temp)
    date = temp[0].split("-")

    ori_date = date[0]+"/"+date[1]+"/"+date[2]
    start_time = ori_date+" "+temp[1].split(".")[0]
    start_time_obj = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    utc_start = start_time_obj.strftime('%Y/%m/%d %H:%M:%S')

    return [utc_start]

def make_row_key_val(dic):
    # 지하 4층 높이의 dic이 있다고 가정하면 지하 3층에서 지하 4층의 dic[4층 keys]가 str인지 판별하고 맞으면 key와 val을 저장, 아니면 재귀하도록 짜야할듯.

    keyrow = []
    valrow = []
    # 1차 시도시 자가 검진
    if isinstance(dic, str) or isinstance(dic, float) or isinstance(dic, int):
        keyrow.append("1차 키 미확인")
        valrow.append(dic)
        # print([keyrow, valrow])
        return[keyrow, valrow]

    elif isinstance(dic, dict):
        key_list = list(dic.keys())
        key_list = sorted(key_list, key=str.lower)

        for key in key_list:
            if isinstance(dic[key], str) or isinstance(dic[key], int):
                keyrow.append(key)
                valrow.append(dic[key])
            elif isinstance(dic[key], float):
                keyrow.append(key)
                valrow.append(format(dic[key],"40.20f"))

            else:
                key_val = make_row_key_val(dic[key])
                if isinstance(key_val,list):
                    for temp_key in key_val[0]:
                        keyrow.append(temp_key)
                    for temp_val in key_val[1]:
                        valrow.append(temp_val)

        return [keyrow, valrow]

def mongodb_to_df(dic_list, table):
    if table == 'mongo_cas':
        main_key = 'data'
    elif table == 'mongo_cas_ird':
        main_key = 'sp_ird'

    # path = os.getcwd() + '/../' + table + '.csv'
    val_table = []
    df = pd.DataFrame()
    count = 0
    for dic in dic_list:
        val_table = []
        count = count + 1

        data_dic = dic[main_key]
        key_val = make_row_key_val(data_dic)
        val_table.append(key_val[1])
        keys = key_val[0]

        val_table[0].append(dic['datetime'])
        keys.append('datetime')

        if count == 1:
            df = pd.DataFrame(val_table, columns=keys)
        else:
            temp_df = pd.DataFrame(val_table, columns=keys)
            df = df.append(temp_df)
    # print(table+"_df load success!")
    return df