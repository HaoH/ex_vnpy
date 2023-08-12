import logging
from datetime import datetime

from pandas import DataFrame, Series

logger = logging.getLogger("StrendSensor")


class SupertrendSensor(object):

    def __init__(self, trend_type, valid_bars, trend_source, atr_factor, setting=None):
        self.name = "Supertrend"
        self.trend_type = trend_type
        self.valid_bars = valid_bars
        self.trend_source = trend_source  # 指标计算的source
        self.atr_factor = atr_factor
        # super trend 相关数据
        self.supertrend_df: DataFrame = None
        self.inited: bool = False

    def init_sensor(self, source_df, ind_values):
        if source_df is None or ind_values is None:
            return

        self.supertrend_df = DataFrame(columns=['atr', 'ph', 'pl', 'pp', 'center', 'up', 'down', 'trend', 'signal'],
                                       index=source_df.index)
        self.supertrend_df.fillna(0, inplace=True)
        self.inited = True

        # 初始化指标取值
        start = len(self.supertrend_df) - len(ind_values)
        if start >= 0:   # 对于MACD之类的指标，开始的一段时间没有指标值
            self.supertrend_df.loc[self.supertrend_df.index[start:], 'atr'] = ind_values
            for i in range(start, len(source_df)):  # 从第3个bar开始，尤其是处理周线数据时，做好初始化工作
                self.detect_next_trend(source_df.iloc[:i + 1])

    def detect_next_trend(self, source_df: DataFrame):
        new_ph = 0
        new_pl = 0
        new_pp = 0
        if self.trend_type == "PIVOT":
            new_df = source_df.iloc[-1 * (self.valid_bars * 2 + 1):]
            high_max = new_df.high.max()
            if high_max == new_df.high[self.valid_bars] and len(new_df[new_df['high'] == high_max]) == 1:
                new_ph = new_df.high[self.valid_bars]
            low_min = new_df.low.min()
            if low_min == new_df.low[self.valid_bars] and len(new_df[new_df['low'] == low_min]) == 1:
                new_pl = new_df.low[self.valid_bars]
            new_pp = new_ph if new_ph > 0 else (new_pl if new_pl > 0 else 0)
            last_center = self.supertrend_df.loc[source_df.index[-2]]['center']
            center = last_center
            if new_pp > 0 and center == 0:
                center = new_pp
            if new_pp > 0 and center > 0:
                center = (center * 2 + new_pp) / 3
            elif new_pp == 0 and center == 0:
                center = (source_df.iloc[-1]['high'] + source_df.iloc[-1]['low']) / 2

        else:
            center = (source_df.iloc[-1]['high'] + source_df.iloc[-1]['low']) / 2

        atr = self.supertrend_df.loc[source_df.index[-1]]['atr']
        down = center - self.atr_factor * atr
        up = center + self.atr_factor * atr

        close = source_df.iloc[-1][self.trend_source]
        last_close = source_df.iloc[-2][self.trend_source]
        if source_df.index[-1] == self.supertrend_df.index[-1]:
            last_data = self.supertrend_df.loc[source_df.index[-2]]
        else:
            last_data = self.supertrend_df.loc[source_df.index[-1]]   # 每个星期第一天
        new_down = down
        if last_close > last_data['down']:
            new_down = max(last_data['down'], new_down)
        new_up = up
        if last_close < last_data['up']:
            new_up = min(last_data['up'], new_up)

        new_trend = last_data['trend']
        if close > last_data['up'] > 0:
            new_trend = 1
        elif close < last_data['down']:
            new_trend = -1
        elif last_data['trend'] != 0:
            new_trend = last_data['trend']
        else:   # 初始化数据
            price_diff = source_df.iloc[-1][self.trend_source] - source_df.iloc[0][self.trend_source]
            # new_trend = 1 if price_diff > 0 else -1
            new_trend = -1      # 初始化趋势默认为下跌

        new_signal = new_trend if new_trend != last_data['trend'] else 0
        self.supertrend_df.loc[source_df.index[-1]] = Series(
            data=[atr, new_ph, new_pl, new_pp, center, new_up, new_down, new_trend, new_signal], index=['atr', 'ph', 'pl', 'pp', 'center', 'up', 'down', 'trend', 'signal'])

    def update_bar(self, source_df, ind_values):
        if not self.inited:
            self.init_sensor(source_df, ind_values)
            return

        self.supertrend_df.loc[source_df.index[-1], 'atr'] = ind_values[-1]
        self.detect_next_trend(source_df)

    @property
    def last_trend_signal(self) -> float:
        return self.supertrend_df.iloc[-1]['trend']

    @property
    def trend_down_price(self) -> float:
        return self.supertrend_df.iloc[-1]['down']

    @property
    def trend_up_price(self) -> float:
        return self.supertrend_df.iloc[-1]['up']

    @property
    def trend_start_date(self) -> datetime:
        return self.supertrend_df[self.supertrend_df['signal'] != 0].index[-1]

    @property
    def last_trend_breakup_price(self) -> float:
        change_df = self.supertrend_df[self.supertrend_df['signal'] != 0]
        if len(change_df) == 0:
            return None

        if change_df.iloc[-1]['signal'] == 1:
            return change_df.iloc[-1]['up']
        elif change_df.iloc[-1]['signal'] == -1:
            return change_df.iloc[-1]['down']
