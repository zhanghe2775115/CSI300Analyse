"""
邢不行量化小讲堂系列文章配套代码
文章标题：听说MACD是技术指标之王？我们用Python来验验成色
文章链接：https://mp.weixin.qq.com/s/8iPlZ0w4pLQz0bYCtcK5Yw
邢不行微信：xbx3642
"""
import config as config
import pandas as pd

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 50000)  # 最多显示数据的行数

# ===读取所有股票代码的列表
stock_code_list = config.get_stock_code_list_in_one_dir(config.root_path1 + '/data', end_with='csv')

all_stock_data = pd.DataFrame()
for code in stock_code_list:
    print(code)
    # 读入股票数据
    path = config.root_path1 + '/data/' + code + '.csv'
    pd.read_csv(path, encoding='gbk', skiprows=1, parse_dates=['交易日期'])
    df = df[['股票代码', '交易日期', '开盘价', '最高价', '最低价', '收盘价', '前收盘价']]

    # 计算复权价格
    df['复权因子'] = (df['收盘价'] / df['前收盘价']).cumprod()
    df['收盘价_复权'] = df['复权因子'] * (df.iloc[-1]['收盘价'] / df.iloc[-1]['复权因子'])

    # 计算交易信号
    try:
        df['EMA_short'] = df['收盘价_复权'].ewm(span=12, adjust=False).mean()
        df['EMA_long'] = df['收盘价_复权'].ewm(span=26, adjust=False).mean()
        df['DIF'] = df['EMA_short'] - df['EMA_long']
        df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
        df['MACD'] = (df['DIF'] - df['DEA']) * 2

        # macd转正，产生买入信号
        condition1 = df['MACD'] > 0
        condition2 = df['MACD'].shift(1) <= 0
        df.loc[condition1 & condition2, 'macd_signal'] = 1
        # macd转负，产生卖出信号
        condition1 = df['MACD'] < 0
        condition2 = df['MACD'].shift(1) >= 0
        df.loc[condition1 & condition2, 'macd_signal'] = 0
    except:
        continue
    else:
        # 合并数据
        all_stock_data = all_stock_data.append(df, ignore_index=True)

print(all_stock_data)

# 计算N日后涨跌幅
day_list = [1, 5, 20]
for i in day_list:
    df['%s日后涨跌幅' % i] = (df['收盘价_复权'].shift(-i) - df['收盘价_复权']) / df['收盘价_复权']
    df['%s日后是否上涨' % i] = df['%s日后涨跌幅' % i] > 0
    df['%s日后是否上涨' % i].fillna(value=False, inplace=True)

# 统计数据
for signal, group in df.groupby('macd_signal'):
    print(signal)
    print(group[[str(i) + '日后涨跌幅' for i in day_list]].describe())
    for i in day_list:
        if signal == 1:
            print(str(i) + '天后涨跌幅大于0概率', '\t', float(group[group[str(i) + '日后涨跌幅'] > 0].shape[0]) / group.shape[0])
        elif signal == 0:
            print(str(i) + '天后涨跌幅小于0概率', '\t', float(group[group[str(i) + '日后涨跌幅'] < 0].shape[0]) / group.shape[0])
