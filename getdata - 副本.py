import baostock as bs
import pandas as pd
import datetime
import multiprocessing

def downLoadAndSave2CSV(index, num, stock_list_df):
    print("Thread start :" + str(index)+ ' to'+str(index + num - 1))
    for curr in range(index, index + num):
        currStock = stock_list_df.loc[curr]
        if currStock["tradeStatus"] == 0:
            continue
        print("Downloading :" + currStock['code']+ ' No.'+str(curr))
        rs = bs.query_history_k_data_plus(currStock['code'],
        "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,tradestatus,isST",
        start_date='2017-01-01', end_date=today,
        frequency="d", adjustflag="3")
        if (rs.error_code != '0'):
            print("Failed to Download :" + currStock['code'] + ' errMsg:'+rs.error_msg)
            continue
        data_df = pd.DataFrame()
        data_df = data_df.append(rs.get_data())
        csvName = currStock['code']+'_day_k_data.csv'
        data_df.to_csv('data/'+csvName, index=False)
        
#### 登陆系统 ####
lg = bs.login()

today=datetime.date.today() 
oneday=datetime.timedelta(days=1) 
yesterday=today-oneday
today = today.strftime('%Y-%m-%d')
yesterday= yesterday.strftime('%Y-%m-%d')
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code+' date: '+ today)
print('login respond  error_msg:'+lg.error_msg)

#### 获取沪深A股历史K线数据 ####
# 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
# 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
# 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
start_time = datetime.datetime.today()
print("start time: "+start_time.strftime('%Y-%m-%d %H:%M:%S'))
stock_list = bs.query_all_stock(day=today)
stock_list_df = stock_list.get_data()
print(stock_list_df.shape[0])
downLoadAndSave2CSV(4000,1000,stock_list_df)
   # pros[i] = multiprocessing.Process(target=downLoadAndSave2CSV, args=((int)(stock_list_df.shape[0]/10)*i, (int)(stock_list_df.shape[0]/10), stock_list_df))
end_time = datetime.datetime.today()
print("end time: "+end_time.strftime('%Y-%m-%d %H:%M:%S'))
delta = end_time - start_time
print("all cost:" + str(delta.total_seconds()))
