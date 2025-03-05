# query_strategy.py
import logging
from datetime import datetime
from typing import Dict
import backtrader as bt
from backtrader import indicators as btind

class EnhancedQueryStrategy(bt.Strategy):
    params = (
        ('rsi_period', 14),
        ('ma_period', 200),
        ('atr_period', 14),
        ('min_history_bars', 200),
        ('oversold', 20),
        ('overbought', 90),
        ('debug', False),
        ('enable_plot', True)
    )

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.inds = {}
        self.plot_data = {}
        
        for data in self.datas:
            if not hasattr(data, '_name') or not data._name:
                self.logger.error(f"无效数据源: {repr(data)} 缺少名称属性")
                continue
            if len(data.close) < 1:
                self.logger.error(f"空数据源: {data._name}")
                continue
            
            try:
                timeframe = '1h' if '1h' in data._name else '1m'
                self.inds[data] = {
                    'ma': btind.SMA(data.close, period=self.params.ma_period),
                    'rsi': btind.RSI(data.close, period=self.params.rsi_period),
                    'atr': btind.ATR(data, period=self.params.atr_period)
                }
                self.logger.info(f"成功初始化指标: {data._name}")
                
                # 绘图数据存储
                self.plot_data[data._name] = {
                    'time': [],
                    'price': [],
                    'ma': [],
                    'rsi': [],
                    'atr': []
                }
            except Exception as e:
                self.logger.error(f"指标初始化失败 [{data._name}]: {str(e)}")
                continue

    def next(self):
        for data, ind in self.inds.items():
            # 确保有足够历史数据
            if len(data) < self.params.min_history_bars:
                # 获取最早数据时间
                first_date = data.datetime.datetime(0)
                required_date = datetime.now() - timedelta(days=30)
                if first_date > required_date:
                    self.logger.error(f"数据不足 {data._name}: 最早数据 {first_date}")
                    return
            self.evaluate_signals(data, ind)
            self.record_plot_data(data)

    def record_plot_data(self, data):
        """记录绘图数据"""
        name = data._name
        self.plot_data[name]['time'].append(data.datetime.datetime())
        self.plot_data[name]['price'].append(data.close[0])
        self.plot_data[name]['ma'].append(self.inds[data]['ma'][0])
        self.plot_data[name]['rsi'].append(self.inds[data]['rsi'][0])
        self.plot_data[name]['atr'].append(self.inds[data]['atr'][0])

    def evaluate_signals(self, data, ind):
        try:
            condition = (
                (ind['rsi'][0] < self.params.oversold) and
                (data.close[0] > ind['ma'][0]) and
                (data.close[0] > data.close[-1] + ind['atr'][0])
            )
            
            if condition:
                self.log_signal(data, 'BUY', ind)
                
        except Exception as e:
            self.logger.error(f"信号计算错误: {str(e)}")

    def log_signal(self, data, signal: str, ind: Dict):
        dt = data.datetime.datetime()
        log_msg = (
            f"{dt} | {data._name} | {signal} | "
            f"Price: {data.close[0]:.2f} | "
            f"RSI: {ind['rsi'][0]:.2f} | "
            f"MA: {ind['ma'][0]:.2f} | "
            f"ATR: {ind['atr'][0]:.2f}"
        )
        self.logger.info(log_msg)
        
    def stop(self):
        """策略结束时触发绘图"""
        if self.params.enable_plot:
            self.save_plot_data()

    def save_plot_data(self):
        """保存绘图数据到数据库"""
        from monitor import DataManager
        dm = DataManager()
        for symbol, data in self.plot_data.items():
            if not data['time']:
                continue
            dm.save_plot_data(
                symbol=symbol.split('_')[0],
                timeframe='1h' if '1h' in symbol else '1m',
                data=data
            )