import logging
import traceback
from dataclasses import is_dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
from pandas import DataFrame, Series
from pandas.tseries.frequencies import to_offset

from vnpy.trader.constant import Interval, Exchange

import talipp.indicators as tainds
from talipp.indicators import Indicator
from talipp.indicator_util import has_valid_values, composite_to_lists, valid_values_length

import ex_vnpy.indicators as exinds
from ex_vnpy.object import ExBarData
from ex_vnpy.sensor.centrum_sensor import CentrumSensor


logger = logging.getLogger("SourceManager")

class SourceManager(object):
    """
    For:
    1. time series container of bar data
    2. calculating technical indicator value
    """

    def __init__(self, bars: list[ExBarData] = [], ta: dict = {}, centrum: bool = False, min_size: int = 100):
        """Constructor"""
        self.exchange: Exchange = None
        self.interval: Interval = None
        self.symbol: str = None
        self.gateway_name: str = None
        # TODO: to delete
        # self.func_price_map = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        #               'volume': lambda x: x.sum(min_count=1), 'turnover': lambda x: x.sum(min_count=1)}

        self.data_df: DataFrame = None
        self.daily_df: DataFrame = None
        self.weekly_df: DataFrame = None
        self.inited: bool = False
        self.size: int = min_size
        self.today: datetime = bars[-1].datetime if len(bars) > 0 else None
        self.centrum = centrum

        self.init_data_df(bars)
        self.update_weekly_df()

        # 更新central recognizer
        self.dc_sensor = CentrumSensor()
        self.wc_sensor = CentrumSensor()
        self.init_central_sensor()

        # 更新增量指标
        self.indicators: Dict[str, Indicator] = {}
        self.ind_inputs: Dict[str, List] = {}
        self.ind_outputs: Dict[str, Any] = {}
        self.ind_interval: Dict[str, Interval] = {}

        self.ta = ta
        if ta is not None and len(ta) > 0:
            for name, ind in self.ta.items():
                ind_name = name
                params = ind["params"] if "params" in ind and (isinstance(ind["params"], tuple) or isinstance(ind["params"], list)) else tuple()
                module_name = tainds if hasattr(tainds, ind["kind"]) else exinds
                self.indicators[ind_name] = getattr(module_name, ind["kind"])(*params)
                self.ind_inputs[ind_name] = ind["input_values"]
                self.ind_outputs[ind_name] = ind['output_values']
                self.ind_interval[ind_name] = ind['interval']

            self.init_indicators()

        if self.count >= self.size:
            self.inited = True

    def init_heikin_ashi_candle_df(self, ha_df: DataFrame):
        # 计算Heikin-Ashi蜡烛图的值
        ha_df['ha_close'] = (ha_df['open'] + ha_df['high'] + ha_df['low'] + ha_df['close']) / 4
        ha_df['ha_open'] = ha_df['open']
        ha_open_index = ha_df.columns.get_loc('ha_open')
        # ha_df.iat[0, ha_open_index] = ha_df.iloc[0]['open']
        for i in range(1, len(ha_df)):
            ha_df.iat[i, ha_open_index] = (ha_df.iloc[i-1]['ha_open'] + ha_df.iloc[i-1]['ha_close']) / 2

        ha_df['ha_high'] = ha_df[['high', 'ha_open', 'ha_close']].max(axis=1)
        ha_df['ha_low'] = ha_df[['low', 'ha_open', 'ha_close']].min(axis=1)
        return ha_df

    def init_data_df(self, bars: list[ExBarData]):
        # auto make columns, according to ExBarData
        init_data = [item.to_dict() for item in bars]
        self.data_df = pd.DataFrame(data=init_data, columns=ExBarData.columns(exclude=['symbol_id', 'symbol', 'exchange', 'interval']))

        # 增加Heikin Ashi蜡烛图信息
        self.init_heikin_ashi_candle_df(self.data_df)

        self.data_df.index = pd.DatetimeIndex(self.data_df['datetime'])
        self.daily_df = self.data_df  # 先不做tick到daily的聚合

        if self.exchange is None and len(bars) > 0:
            self.exchange = bars[0].exchange
            self.interval = bars[0].interval
            self.symbol = bars[0].symbol
            self.gateway_name = bars[0].gateway_name

    def init_central_sensor(self):
        if self.centrum:
            self.dc_sensor.init_sensor(self.daily_df)
            self.wc_sensor.init_sensor(self.weekly_df)

    def init_indicators(self):
        if self.inited or len(self.daily_df) < 1:
            return

        # 初始化 indicator 指标计算
        for ind_name, ind in self.indicators.items():
            input_names = self.ind_inputs[ind_name]
            interval = self.ind_interval[ind_name]
            source_df = self.get_dataframe(interval)
            if len(input_names) == 1:
                input_values = source_df[input_names[0]].to_list()
            else:
                # data = source_df[input_names].to_dict("list")
                # input_values = OHLCVFactory.from_dict(data)

                data = source_df[input_names].to_dict("records")
                input_values = ExBarData.from_dicts(data)
            try:
                ind.initialize(input_values=input_values)
            except Exception as e:
                logger.error(f"[SM] indicator initialize error! {e}")
                traceback.print_exc()

    def update_bar(self, bar: ExBarData) -> None:
        """
        Update new bar data into array manager.
        """
        if len(self.data_df) == 0:
            self.init_data_df([bar])
        else:
            new_dict = bar.to_dict()

            # add heikin ashi
            new_dict['ha_close'] = (new_dict['open'] + new_dict['high'] + new_dict['low'] + new_dict['close']) / 4
            last_bar = self.data_df.iloc[-1]
            new_dict['ha_open'] = (last_bar['ha_open'] + last_bar['ha_close']) / 2
            new_dict['ha_high'] = max(new_dict['high'], new_dict['ha_open'], new_dict['ha_close'])
            new_dict['ha_low'] = min(new_dict['low'], new_dict['ha_open'], new_dict['ha_close'])

            # TODO: check all isoformat
            # nt = pd.to_datetime(bar.datetime.isoformat())
            nt = pd.to_datetime(bar.datetime)
            self.data_df.loc[nt, :] = new_dict

        self.update_weekly_df()
        self.today = bar.datetime
        if self.centrum:
            self.dc_sensor.update_bar(self.daily_df)
            # if week_bar_cnt < len(self.weekly_df):   # 只有在week bar完成，才进行pivot探测
            self.wc_sensor.update_bar(self.weekly_df)

        if not self.inited and self.count >= self.size:
            self.init_indicators()
            self.inited = True
        else:
            self.update_indicators()

    def update_indicators(self):
        # 更新指标计算
        for ind_name, ind in self.indicators.items():
            source_df = self.get_dataframe(self.ind_interval[ind_name])
            input_names = self.ind_inputs[ind_name]
            new_bar = source_df[input_names].iloc[-1]

            # use ExBarData instead of OHLCV
            # new_data = new_bar[input_names[0]] if len(input_names) == 1 else OHLCV(**(new_bar.to_dict()))
            new_data = new_bar[input_names[0]] if len(input_names) == 1 else ExBarData.from_dict(new_bar.to_dict())

            ind_len = len(ind.input_values)
            data_len = len(source_df)
            if data_len == ind_len:
                ind.update(new_data)
            elif data_len == ind_len + 1:
                ind.add(new_data)
            else:
                logger.error("[ERROR] INDICATOR ERROR, length is not matched!")
                raise Exception()

    def resample_to_week_data(self, df):
        agg_dict = {}
        for column in df.columns:
            if column in ['symbol', 'exchange', 'interval', 'symbol_id', 'datetime', 'stype']:
                continue

            col_type = df[column].dtype
            # 根据数据类型决定聚合函数
            if column.endswith('open'):
                agg_dict[column] = 'first'
            elif column.endswith('high'):
                agg_dict[column] = 'max'
            elif column.endswith('low'):
                agg_dict[column] = 'min'
            elif column.endswith('close'):
                agg_dict[column] = 'last'
            elif col_type == 'int64' or col_type == 'float64':
                # 数值型列使用'sum'作为聚合函数，考虑min_count=1
                agg_dict[column] = lambda x: x.sum(min_count=1)
            else:
                logger.warning(f"column not support resample: {column}")

        w_df = df.resample('W').agg(agg_dict).dropna()
        w_df.index = w_df.index + to_offset("-2D")
        w_df['datetime'] = w_df.index
        return w_df

    def update_weekly_df(self):
        if self.daily_df is None or len(self.daily_df) <= 0:
            return

        if self.weekly_df is None:
            self.weekly_df = self.resample_to_week_data(self.daily_df)
            self.init_heikin_ashi_candle_df(self.weekly_df)
        else:
            last_week_dt = self.weekly_df.index[-1]
            last_bar = self.daily_df.iloc[-1]
            _, last_week, _ = (last_week_dt - timedelta(days=last_week_dt.weekday())).isocalendar()
            _, new_week, _ = (last_bar.name - timedelta(days=last_bar.name.weekday())).isocalendar()
            if last_week == new_week:
                last_week_s = self.weekly_df.iloc[-1].copy()

                last_week_s["high"] = max(last_week_s["high"], last_bar["high"])
                last_week_s["low"] = min(last_week_s["low"], last_bar["low"])
                last_week_s["close"] = last_bar["close"]
                last_week_s["volume"] += last_bar["volume"]
                last_week_s["turnover"] += last_bar["turnover"]

                last_week_s["ha_close"] = (last_week_s['open'] + last_week_s['high'] + last_week_s['low'] + last_week_s['close']) / 4
                last_week_s["ha_high"] = max(last_week_s["high"], last_week_s["ha_open"], last_week_s["ha_close"])
                last_week_s["ha_low"] = min(last_week_s["low"], last_week_s["ha_open"], last_week_s["ha_close"])

                self.weekly_df.loc[last_week_s.name] = last_week_s
            else:
                last_bar_s = self.daily_df.iloc[-1].copy()
                new_index = last_bar_s.name - timedelta(days=last_bar_s.name.weekday()) + timedelta(days=4)
                last_bar_s['ha_close'] = (last_bar_s['open'] + last_bar_s['high'] + last_bar_s['low'] + last_bar_s['close']) / 4
                last_bar_s['ha_open'] = (self.weekly_df.iloc[-1]['ha_open'] + self.weekly_df.iloc[-1]['ha_close']) / 2
                last_bar_s['ha_high'] = max(last_bar_s['high'], last_bar_s['ha_open'], last_bar_s['ha_close'])
                last_bar_s['ha_low'] = min(last_bar_s['low'], last_bar_s['ha_open'], last_bar_s['ha_close'])
                self.weekly_df.loc[new_index] = last_bar_s

    def recent_week_high(self, recent_weeks: int = 7, last_contained: bool = True) -> float:
        return self.recent_high(Interval.WEEKLY, recent_weeks, last_contained)

    def recent_daily_high(self, recent_days: int = 7, last_contained: bool = True) -> float:
        return self.recent_high(Interval.DAILY, recent_days, last_contained)

    def recent_high(self, interval: Interval, recent_bars: int = 7, last_contained: bool = True) -> float:
        """
        判断最近N天/周的价格高点
        :param recent_bars:
        :param last_contained: 是否包含最后一周的数据，默认为包含。如果当周要根据价格突破判断入场位置的话，则不应该包含当周（最后一周）的数据；
        :return:
        """
        source_df = self.weekly_df if interval == Interval.WEEKLY else self.daily_df
        if source_df is None:
            return None

        n = recent_bars if last_contained else recent_bars + 1
        if len(source_df) < recent_bars + 1:
            n = len(source_df)

        high = source_df['high']
        return high[-1 * n:].max() if last_contained else high[-1 * n: -1].max()

    def recent_week_high_since(self, start: datetime):
        # start 日期以来最高点(不包含本周), [start, last_week)
        if self.weekly_df is None or start not in self.weekly_df.index:
            return None

        recent_df = self.weekly_df.loc[start:]
        return recent_df['high'][:-1].max()

    def recent_week_low(self, recent_weeks: int = 7, last_contained: bool = True) -> float:
        return self.recent_low(Interval.WEEKLY, recent_weeks, last_contained)

    def recent_daily_low(self, recent_days: int = 7, last_contained: bool = True) -> float:
        return self.recent_low(Interval.DAILY, recent_days, last_contained)

    def recent_low(self, interval, recent_bars: int = 7, last_contained: bool = True) -> float:
        source_df = self.weekly_df if interval == Interval.WEEKLY else self.daily_df
        if source_df is None:
            return None

        n = recent_bars if last_contained else recent_bars + 1
        if len(source_df) < recent_bars + 1:
            n = len(source_df)

        low = source_df['low']
        return low[-1 * n:].min() if last_contained else low[-1 * n: -1].min()


    def recent_week_hl_gap(self, recent_weeks, last_contained: bool = True):
        """
        计算最近的week_num的high-low的最大值
        :param recent_weeks:
        :return:
        """
        if self.weekly_df is None:
            return 0

        n = recent_weeks if last_contained else recent_weeks + 1
        if len(self.weekly_df) < recent_weeks + 1:
            n = len(self.weekly_df)

        data = self.weekly_df.iloc[-1 * n:] if last_contained else self.weekly_df.iloc[-1 * n: -1]
        hl_gap_s = (data['high'] - data['low'])/data['close']
        return hl_gap_s.max()

    def last_week_hl_gap(self, week_num):
        """
        计算第week_num的high-low
        :param week_num:
        :return:
        """
        if self.weekly_df is None:
            return 0

        n = week_num
        if len(self.weekly_df) < week_num:
            n = len(self.weekly_df)

        data = self.weekly_df.iloc[-1 * n]
        return (data['high'] - data['low'])/data['close']

    def get_dataframe(self, interval: Interval):
        return self.weekly_df if interval == Interval.WEEKLY else self.data_df

    @property
    def count(self) -> int:
        """
        Get open price time series.
        """
        return len(self.data_df)

    @property
    def open(self) -> pd.Series:
        """
        Get open price time series.
        """
        return self.data_df['open']

    @property
    def high(self) -> pd.Series:
        """
        Get high price time series.
        """
        return self.data_df['high']

    @property
    def low(self) -> pd.Series:
        """
        Get low price time series.
        """
        return self.data_df['low']

    @property
    def close(self) -> pd.Series:
        """
        Get close price time series.
        """
        return self.data_df['close']

    @property
    def volume(self) -> pd.Series:
        """
        Get trading volume time series.
        """
        return self.data_df['volume']

    @property
    def turnover(self) -> pd.Series:
        """
        Get trading turnover time series.
        """
        return self.data_df['turnover']

    @property
    def open_interest(self) -> pd.Series:
        """
        Get trading volume time series.
        """
        return self.data_df['open_interest']

    @property
    def latest_daily_bar(self) -> Series:
        return self.prior_bar(Interval.DAILY, 1)

    @property
    def latest_week_bar(self) -> Series:
        return self.prior_bar(Interval.WEEKLY, 1)

    @property
    def prior_daily_bar(self) -> Series:
        """
        当日/周的上一日/周
        """
        return self.prior_bar(Interval.DAILY, 2)

    @property
    def prior_weekly_bar(self) -> Series:
        """
        当日/周的上一日/周
        """
        return self.prior_bar(Interval.WEEKLY, 2)

    def prior_bar(self, interval: Interval, bar_count: int) -> Series:
        source_df = self.daily_df if interval == Interval.DAILY else self.weekly_df
        if source_df is None or len(source_df) < bar_count:
            return None

        return source_df.iloc[-1 * bar_count, :]

    @property
    def is_up(self) -> bool:
        if self.daily_df is None or len(self.daily_df) < 2:
            return False

        highs = self.daily_df['high']
        return highs[-1] >= highs[-2]

    @property
    def is_down(self) -> bool:
        if self.daily_df is None or len(self.daily_df) < 2:
            return False

        lows = self.daily_df['low']
        return lows[-1] <= lows[-2]

    @property
    def last_date(self) -> datetime:
        return self.today

    @property
    def last_bottom_low_d(self) -> float:
        return self.last_pivot_price(Interval.DAILY, 'bottom', 'low')

    @property
    def last_bottom_low_w(self) -> float:
        return self.last_pivot_price(Interval.WEEKLY, 'bottom', 'low')

    @property
    def last_top_high_d(self) -> float:
        return self.last_pivot_price(Interval.DAILY, 'top', 'high')

    @property
    def last_top_high_w(self) -> float:
        return self.last_pivot_price(Interval.WEEKLY, 'top', 'high')

    @property
    def last_top_date_w(self) -> datetime:
        return self.last_pivot_date(Interval.WEEKLY, "top")

    @property
    def last_bottom_date_w(self) -> datetime:
        return self.last_pivot_date(Interval.WEEKLY, "bottom")

    @property
    def last_top_date_d(self) -> datetime:
        return self.last_pivot_date(Interval.DAILY, "top")

    @property
    def last_bottom_date_d(self) -> datetime:
        return self.last_pivot_date(Interval.DAILY, "bottom")

    def last_pivot_date(self, interval: Interval, pivot_type: str) -> datetime:
        if not self.centrum or (interval == Interval.DAILY and self.daily_df is None) or \
                (interval == Interval.WEEKLY and self.weekly_df is None):
            return None

        source_detector = self.dc_sensor if interval == Interval.DAILY else self.wc_sensor
        return source_detector.last_top_date if pivot_type == "top" else source_detector.last_bottom_date

    def last_pivot_price(self, interval: Interval, pivot_type: str, price_type: str = 'low') -> float:
        index = self.last_pivot_date(interval, pivot_type)
        source_detector = self.dc_sensor if interval == Interval.DAILY else self.wc_sensor
        return source_detector.pivot_df.loc[index, price_type] if index is not None else None

    def get_centrum_sensor(self, interval: Interval) -> CentrumSensor:
        return self.dc_sensor if interval == Interval.DAILY else self.wc_sensor

    def get_indicator_origin_values(self, ind_name):
        indicator = self.indicators[ind_name]
        if not has_valid_values(indicator):
            return None

        if is_dataclass(indicator.get_output_value_type()):
            outputs = composite_to_lists(indicator)
            for new_name, origin_name in self.ind_outputs[ind_name].items():
                if new_name != origin_name:        # 名字存在映射
                    outputs[new_name] = outputs.pop(origin_name)
        else:
            new_name = self.ind_outputs[ind_name]
            outputs = {new_name: indicator.output_values}
        return outputs

    def get_indicator_values(self, ind_name):
        """
        过滤掉指标头部的None值
        """
        outputs = self.get_indicator_origin_values(ind_name)
        if not outputs:
            return None

        for key in outputs.keys():
            ind_value = outputs[key]
            start = len(ind_value) - valid_values_length(ind_value)
            outputs[key] = ind_value[start:]
        return outputs

    def get_indicator_value(self, ind_name, key):
        outputs = self.get_indicator_values(ind_name)
        return outputs[key] if outputs is not None else None

    def latest_week_days(self) -> int:
        if self.weekly_df is None:
            return None

        latest_week_date = self.weekly_df.index[-1]
        week_first_day = latest_week_date - timedelta(days=5)
        for i in range(2, 7):
            if self.daily_df.index[-1 * i] < week_first_day:
                return i - 1
        return 1
