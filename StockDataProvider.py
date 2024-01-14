import baostock as bs
import pandas as pd
import datetime
import multiprocessing
import re

class StockDataProvider:

    def __init__(self):
        #### 登陆系统 ####
        self.FILE_PATH = 'E:\lianghua\data\date_record'
        lg = bs.login()
        today=datetime.date.today() 
        oneday=datetime.timedelta(days=1) 
        yesterday=today-oneday
        self.today = today.strftime('%Y-%m-%d')
        self.yesterday= yesterday.strftime('%Y-%m-%d')
        # 显示登陆返回信息
        print('login respond error_code:'+str(lg.error_code)+' date: '+ str(self.today))
        print('login respond error_msg:'+lg.error_msg)
        self.stock_list = bs.query_all_stock(day=self.today)
        self.stock_list_df = self.stock_list.get_data()
        print(self.stock_list_df.shape[0])

    def downLoadAndSave2CSV(self, index, num, stock_list_df, frequency="d", adjustflag="3", start_date='2017-01-01', end_date=None, stock_name=None, save_mode='w'):
        print("Thread start :" + str(index)+ ' to'+str(index + num - 1))
        end_date = end_date or self.today
        for curr in range(index, index + num):
            currStock = stock_list_df.loc[curr]
            if currStock["tradeStatus"] == 0:
                continue
            start_date = '2017-01-01'
            #### 获取沪深A股历史K线数据 ####
            # 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
            # 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
            # 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
            if stock_name != None:
                print("Downloading :" + stock_name + ' No.'+str(curr))
                rs = bs.query_history_k_data_plus(stock_name,            "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,tradestatus,isST",
                start_date=start_date, end_date=end_date, frequency=frequency, adjustflag=adjustflag)
            else:
                print("Downloading :" + currStock['code']+ ' No.'+str(curr))
                rs = bs.query_history_k_data_plus(currStock['code'],            "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,tradestatus,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST",
                start_date=start_date, end_date=end_date, frequency=frequency, adjustflag=adjustflag)
            if (rs.error_code != '0'):
                print("Failed to Download :" + currStock['code'] + ' errMsg:'+rs.error_msg)
                continue
            data_df = pd.DataFrame()
            data_df = data_df.append(rs.get_data())
            
            if stock_name != None:
                csvName = stock_name + '_day_k_data.csv'
            else:
                csvName = currStock['code']+'_day_k_data.csv'
            data_df.to_csv('data/'+csvName, index=False, mode=save_mode, header=False)

    def updateAllData(self):
        start_time = datetime.datetime.today()
        print("start time: "+start_time.strftime('%Y-%m-%d %H:%M:%S'))

        self.downLoadAndSave2CSV(0, self.stock_list_df.shape[0] - 1, self.stock_list_df, start_date='2017-01-01', end_date=self.today, save_mode='a')
           # pros[i] = multiprocessing.Process(target=downLoadAndSave2CSV, args=((int)(stock_list_df.shape[0]/10)*i, (int)(stock_list_df.shape[0]/10), stock_list_df))
        end_time = datetime.datetime.today()
        self.saveDate2Record()
        print("end time: "+end_time.strftime('%Y-%m-%d %H:%M:%S'))
        delta = end_time - start_time
        print("all cost:" + str(delta.total_seconds()))

    def downloadAllData(self):  
        start_time = datetime.datetime.today()
        print("start time: "+start_time.strftime('%Y-%m-%d %H:%M:%S'))

        self.downLoadAndSave2CSV(0, self.stock_list_df.shape[0] - 1, self.stock_list_df, start_date="2017-01-01", end_date=self.today, save_mode='w')

        end_time = datetime.datetime.today()
        self.saveDate2Record()
        print("end time: "+end_time.strftime('%Y-%m-%d %H:%M:%S'))
        delta = end_time - start_time
        print("all cost:" + str(delta.total_seconds()))

       
    def updateSingleData(self, index, stock_name=None):  
        start_date = self.getCurrentDataDate()
        self.downLoadAndSave2CSV(index, 1, self.stock_list_df, start_date=start_date, end_date=self.today, stock_name=stock_name, save_mode='a')

    def downloadSingleData(self, index, stock_name=None):  
        self.downLoadAndSave2CSV(index, 1, self.stock_list_df, start_date="2017-01-01", end_date=self.today, stock_name=stock_name, save_mode='w')

        
    def getCurrentDataDate(self):
        with open(self.FILE_PATH, 'r') as file:
            content = file.read()
        # 使用正则表达式查找日期模式
        date_pattern = re.compile(r'\b\d{4}-\d{2}-\d{2}\b')
        dates = date_pattern.findall(content)
        if dates:
            # 取第一个日期
            first_date = dates[-1]

            # 将日期字符串转换为 datetime 对象，并格式化为 'YYYY-MM-DD'
            formatted_date = datetime.datetime.strptime(first_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            print("Latest data:" + str(formatted_date))

            return formatted_date
        else:
        # 将日期字符串转换为 datetime 对象，并格式化为 'YYYY-MM-DD'
            print("No dates found.")
            return None
        
    def saveDate2Record(self):
        with open(self.FILE_PATH, 'a') as file:
            file.write(f"{self.today}\n")
        print("Save date:" + str(self.today))
        
    def getSingleDataFrame(self, stock_name, frequency="k"):
        csvName = stock_name + "_day_" + frequency + "_data.csv"
        return pd.read_csv('./data/' + csvName, header=0, parse_dates=True, index_col=0)
         
    def getAllList():
        return self.stock_list