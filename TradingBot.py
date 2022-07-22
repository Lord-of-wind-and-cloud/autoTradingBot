import pandas as pd


def signal_moving_average(df: pd.DataFrame, para=[5, 60]):
    """
    简单的移动平均线策略
    当短期均线由下向上穿过长期均线的时候，买入；然后由上向下穿过的时候，卖出。
    :param df: 原始数据
    :param para: 参数，[ma_short, ma_long]
    :return:
    """
    # ===计算指标
    ma_short = para[0]
    ma_long = para[1]

    # 计算均线
    df['ma_short'] = df['close'].rolling(ma_short, min_periods=1).mean()
    df['ma_long'] = df['close'].rolling(ma_long, min_periods=1).mean()

    # ===找出买入信号
    condition1 = df['ma_short'] > df['ma_long']  # 短期均线 > 长期均线
    condition2 = df['ma_short'].shift(1) <= df['ma_long'].shift(1)  # 之前的短期均线 <= 长期均线
    df.loc[condition1 & condition2, 'signal'] = 1

    # ===找出卖出信号
    condition1 = df['ma_short'] < df['ma_long']  # 短期均线 < 长期均线
    condition2 = df['ma_short'].shift(1) >= df['ma_long'].shift(1)  # 之前的短期均线 >= 长期均线
    df.loc[condition1 & condition2, 'signal'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    df.drop(['ma_short', 'ma_long'], axis=1, inplace=True)

    # ===由signal计算出实际的每天持有仓位
    # signal的计算运用了收盘价，是每根K线收盘之后产生的信号，到第二根开盘的时候才买入，仓位才会改变。
    df['pos'] = df['signal'].shift()
    df['pos'].fillna(method='ffill', inplace=True)
    df['pos'].fillna(value=0, inplace=True)

    return df

from datetime import datetime, timedelta
import time
import pandas as pd
from email.mime.text import MIMEText
from smtplib import SMTP_SSL


# 计算当前时间到下一个交易周期的 sleep 时间
def next_run_time(time_interval, ahead_time=1):
    if time_interval.endswith('m'):
        now_time = datetime.now()
        time_interval = int(time_interval.strip('m'))

        target_min = (int(now_time.minute / time_interval) + 1) * time_interval
        if target_min < 60:
            target_time = now_time.replace(minute=0, second=0, microsecond=0)
        else:
            if now_time.hour == 23:
                target_time = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
                target_time += timedelta(days=1)
            else:
                target_time = now_time.replace(hour=now_time.hour + 1, minute=0, second=0, microsecond=0)

        # sleep直到靠近目标时间之前
        if (target_time - datetime.now()).seconds < ahead_time + 1:
            print('距离target_time不足', ahead_time, '秒，下下个周期再运行')
            target_time += timedelta(minutes=time_interval)
        print('下次运行时间', target_time)
        return target_time
    else:
        exit('time_interval doesn\'t end with m')

    return datetime.now()


# 获取okex的k线数据
def get_okex_candle_data(exchange, symbol, time_interval):
    # 抓取数据
    content = exchange.fetch_ohlcv(symbol, timeframe=time_interval, since=0)

    # 整理数据
    df = pd.DataFrame(content, dtype=float)
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')
    # 北京时间 = 格林威治时间 + 8小时
    df['candle_begin_time_GMT8'] = df['candle_begin_time'] + timedelta(hours=8)
    df = df[['candle_begin_time_GMT8', 'open', 'high', 'low', 'close', 'volume']]

    return df


def place_order(exchange, order_type, buy_or_sell, symbol, price, amount):
    """
    下单
    :param exchange: 交易所
    :param order_type: limit, market
    :param buy_or_sell: buy, sell
    :param symbol: 买卖品种
    :param price: 当market订单的时候，price无效
    :param amount: 买卖量
    :return:
    """
    for i in range(5):
        try:
            # 限价单
            if order_type == 'limit':
                # 买
                if buy_or_sell == 'buy':
                    order_info = exchange.create_limit_buy_order(symbol, amount, price)  # 买单
                # 卖
                elif buy_or_sell == 'sell':
                    order_info = exchange.create_limit_sell_order(symbol, amount, price)  # 卖单
            # 市价单
            elif order_type == 'market':
                # 买
                if buy_or_sell == 'buy':
                    order_info = exchange.create_market_buy_order(symbol=symbol, amount=amount)  # 买单
                # 卖
                elif buy_or_sell == 'sell':
                    order_info = exchange.create_market_sell_order(symbol=symbol, amount=amount)  # 卖单
            else:
                pass

            print('下单成功：', order_type, buy_or_sell, symbol, price, amount)
            print('下单信息：', order_info, '\n')
            return order_info

        except Exception as e:
            print('下单报错，1s后重试', e)
            time.sleep(1)
    print('下单报错次数过多，程序终止')
    exit()


class QQMail:
    user = 'xxx@qq.com'  # QQ邮箱地址
    pwd = '授权码'  # 授权码  https://jingyan.baidu.com/article/29697b91072c51ab20de3c3f.html

    def __init__(self):
        self.smtp = SMTP_SSL('smtp.qq.com', 465)
        self.smtp.login(self.user, self.pwd)

    def send_message(self, to, subject, content):
        msg = MIMEText(content)

        msg['Subject'] = subject  # 标题
        msg['From'] = self.user  # 发件人
        msg['To'] = to  # 收件人

        self.smtp.send_message(msg)

    def quit(self):
        self.smtp.quit()


# 自动发送邮件
def auto_send_email(to_address, subject, content):
    mail = QQMail()
    mail.send_message(to_address, subject, content)
    mail.quit()

import ccxt
from datetime import datetime, timedelta
from time import sleep
import pandas as pd
from .trade import next_run_time, auto_send_email, place_order, get_okex_candle_data
from .signals import signal_moving_average

"""
自动交易主要流程

# 通过while语句，不断的循环

# 每次循环中需要做的操作步骤
1. 更新账户信息
2. 获取实时数据
3. 根据最新数据计算买卖信号
4. 根据目前仓位、买卖信息，结束本次循环，或者进行交易
5. 交易

"""

time_interval = '1m'  # 运行时间间隔

# 创建交易所对象
exchange = ccxt.okex5()

# 设置代理
exchange.proxies = {
    'http': 'http://127.0.0.1:6666',
    'https': 'http://127.0.0.1:6666',
}

# 设置apiKey和apiSecret
exchange.apiKey = ''
exchange.secret = ''
exchange.password = ''  # okex特有的参数Passphrase，如果不设置会报错：AuthenticationError: requires `password`

symbol = 'ETH/USDT'  # 交易对
base_coin = symbol.split('/')[-1]
trade_coin = symbol.split('/')[0]

para = [20, 200]  # 策略参数

# ====主程序
while True:
    # ===监控邮件内容
    email_title = '策略报表'
    email_content = ''

    # ===从服务器更新账户balance信息
    balance = exchange.fetch_balance()['total']
    base_coin_amount = float(balance[base_coin])
    trade_coin_amount = float(balance[trade_coin])
    print('当前资产：\n', base_coin, base_coin_amount, trade_coin, trade_coin_amount)

    # ===sleep直到运行时间
    run_time = next_run_time(time_interval)
    sleep(max(0, (run_time - datetime.now()).seconds))
    while True:  # 在靠近目标时间时
        if datetime.now() < run_time:
            continue
        else:
            break

    # ===获取最新数据
    while True:
        # 获取数据
        df = get_okex_candle_data(exchange, symbol, time_interval)
        # 判断是否包含最新的数据
        _temp = df[df['candle_begin_time_GMT8'] == (run_time - timedelta(minutes=int(time_interval)))]
        if _temp.empty:
            print('获取数据不包含最新的数据，重新获取')
            continue
        else:
            break

    # ===产生交易信号
    df = df[df['candle_begin_time_GMT8'] < pd.to_datetime(run_time)]  # 去除target_time周期的数据
    df = signal_moving_average(df, para=para)
    signal = df.iloc[-1]['signal']
    # signal = 1
    print('\n 交易信号', signal)

    # ====卖出品种
    if trade_coin_amount > 0 and signal == 0:
        print('\n卖出')
        # 获取最新的卖出价格
        price = exchange.fetch_ticker(symbol)['bid']  # 获取买一价格
        # 下单
        place_order(exchange, order_type='limit', buy_or_sell='sell', symbol=symbol, price=price * 0.98, amount=trade_coin_amount)
        # 邮件标题
        email_title += '_卖出_' + trade_coin
        # 邮件内容
        email_content += '卖出信息：\n'
        email_content += '卖出数量：' + str(trade_coin_amount) + '\n'
        email_content += '卖出价格：' + str(price) + '\n'

    # ====买入品种
    if trade_coin_amount == 0 and signal == 1:
        print('\n买入')
        # 获取最新的买入价格
        price = exchange.fetch_ticker(symbol)['ask']  # 获取卖一价格
        # 计算买入数量
        buy_amount = base_coin_amount / price
        # 获取最新的卖出价格
        place_order(exchange, order_type='limit', buy_or_sell='buy', symbol=symbol, price=price * 1.02, amount=buy_amount)
        # 邮件标题
        email_title += '_买入_' + trade_coin
        # 邮件内容
        email_content += '买入信息：\n'
        email_content += '买入数量：' + str(buy_amount) + '\n'
        email_content += '买入价格：' + str(price) + '\n'

    # ====发送邮件
    # 每个半小时发送邮件
    if run_time.minute % 30 == 0:
        # 发送邮件
        auto_send_email('462915202@qq.com', email_title, email_content)

    # ====本次交易结束
    print(email_title)
    print(email_content)
    print('====本次运行完毕\n')
    sleep(6 * 1)

