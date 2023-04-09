"""
邢不行量化小讲堂系列文章配套代码
文章标题：听说MACD是技术指标之王？我们用Python来验验成色
文章链接：https://mp.weixin.qq.com/s/8iPlZ0w4pLQz0bYCtcK5Yw
邢不行微信：xbx3642
"""
import pandas as pd
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 50000)  # 最多显示数据的行数

# 读入股票数据
df = pd.read_csv('sh600000.csv', encoding='gbk', skiprows=1, parse_dates=['交易日期'])

# 计算复权因子
df['复权因子'] = (df['收盘价'] / df['前收盘价']).cumprod()
df['收盘价_复权'] = df['复权因子'] * (df.iloc[-1]['收盘价'] / df.iloc[-1]['复权因子'])

# 计算MACD
df['EMA_short'] = df['收盘价_复权'].ewm(span=12, adjust=False).mean()
df['EMA_long'] = df['收盘价_复权'].ewm(span=26, adjust=False).mean()
df['DIF'] = df['EMA_short'] - df['EMA_long']
df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
df['MACD'] = (df['DIF'] - df['DEA']) * 2

print(df)
