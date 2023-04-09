"""
邢不行量化小讲堂系列文章配套代码
文章标题：听说MACD是技术指标之王？我们用Python来验验成色
文章链接：https://mp.weixin.qq.com/s/8iPlZ0w4pLQz0bYCtcK5Yw
邢不行微信：xbx3642
"""
import pandas as pd
import os

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数

# 获取项目根目录
_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
root_path = os.path.abspath(os.path.join(_, '../..'))  # 返回根目录文件夹

# 获取当前程序的地址
current_file = __file__

# 程序根目录地址
root_path1 = os.path.abspath(os.path.join(current_file, os.pardir))

# 输入数据根目录地址
input_data_path = os.path.abspath(os.path.join(root_path1, 'data', 'input_data'))

# 输出数据根目录地址
output_data_path = os.path.abspath(os.path.join(root_path1, 'data', 'output_data'))


def get_stock_code_list_in_one_dir(path, end_with='csv'):
    """
    从指定文件夹下，导入所有csv文件的文件名
    :param path:
    :param end_with:
    :return:
    """
    stock_list = []

    # 系统自带函数os.walk，用于遍历文件夹中的所有文件
    for root, dirs, files in os.walk(path):
        if files:  # 当files不为空的时候
            for f in files:
                if f.endswith('.' + end_with):
                    stock_list.append(f[:8])

    return sorted(stock_list)

# 导入数据
def import_stock_data(stock_code, other_columns=[]):
    """
    导入在data/input_data/stock_data下的股票数据。
    :param stock_code: 股票数据的代码，例如'sh600000'
    :param other_columns: 若为默认值，只导入以下基础字段：'交易日期', '股票代码', '开盘价', '最高价', '最低价', '收盘价', '涨跌幅', '成交额'。
    若不为默认值，会导入除基础字段之外其他指定的字段
    :return:
    """
    df = pd.read_csv(input_data_path + '/stock_data/' + stock_code + '.csv', encoding='gbk')
    df.columns = [i.encode('utf8') for i in df.columns]
    df = df[['交易日期', '股票代码', '开盘价', '最高价', '最低价', '收盘价', '涨跌幅', '成交额'] + other_columns]
    df.sort_values(by=['交易日期'], inplace=True)
    df['交易日期'] = pd.to_datetime(df['交易日期'])
    df['股票代码'] = stock_code
    df.reset_index(inplace=True, drop=True)

    return df