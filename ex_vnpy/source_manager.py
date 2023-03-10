from datetime import datetime
from typing import Optional, Any

import pandas as pd
from pandas import DataFrame, Series

from ex_vnpy.centrum_detector import CentrumDetector
from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.object import BarData
from pandas.tseries.frequencies import to_offset


class SourceManager(object):
    """
    For:
    1. time series container of bar data
    2. calculating technical indicator value
    """
    func_price_map = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
                      'volume': lambda x: x.sum(min_count=1), 'turnover': lambda x: x.sum(min_count=1)}

    def __init__(self, bars: list[BarData], centrum: bool = False, min_size: int = 100):
        """Constructor"""
        self.exchange: Exchange = None
        self.interval: Interval = None
        self.symbol: str = None
        self.gateway_name: str = None

        self.daily_df: DataFrame = None
        self.weekly_df: DataFrame = None
        self.inited: bool = False
        self.size: int = min_size
        self.today: datetime = bars[-1].datetime if len(bars) > 0 else None
        self.centrum = centrum

        for x in bars:
            x.datetime = x.datetime.isoformat()
        self.data_df = pd.DataFrame(data=bars, columns=['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'turnover', 'open_interest', 'datetime'])
        self.data_df.rename(
            columns={'open_price': 'open', 'high_price': 'high', 'low_price': 'low', 'close_price': 'close'},
            inplace=True)
        self.data_df.index = pd.DatetimeIndex(self.data_df['datetime'])

        if len(bars) > 0:
            self.update_meta(bars[0].__dict__)
            if self.inited:
                self.update_weekly_df()

        self.dc_detector = CentrumDetector()
        self.wc_detector = CentrumDetector()
        if self.centrum:
            self.dc_detector.init_detector(self.daily_df)
            self.wc_detector.init_detector(self.weekly_df)

    def update_meta(self, setting):
        if not self.inited and self.count >= self.size:
            self.inited = True
            self.daily_df = self.data_df  # 先不做tick到daily的聚合

        if self.exchange is None:
            self.exchange = setting['exchange']
            self.interval = setting['interval']
            self.symbol = setting['symbol']
            self.gateway_name = setting['gateway_name']

    def update_bar(self, bar: BarData) -> None:
        """
        Update new bar data into array manager.
        """
        new_dict = bar.__dict__
        new_dict['open'] = new_dict['open_price']
        new_dict['high'] = new_dict['high_price']
        new_dict['low'] = new_dict['low_price']
        new_dict['close'] = new_dict['close_price']
        nt = pd.to_datetime(bar.datetime.isoformat())
        self.data_df.loc[nt, :] = new_dict
        self.update_meta(new_dict)
        self.today = bar.datetime
        if self.centrum:
            self.dc_detector.new_bar(self.daily_df)

        if self.inited:
            week_bar_cnt = len(self.weekly_df) if self.weekly_df is not None else 0
            self.update_weekly_df()
            if self.centrum and self.weekly_df is not None and week_bar_cnt < len(self.weekly_df):   # 只有在week bar完成，才进行pivot探测
                self.wc_detector.new_bar(self.weekly_df.iloc[:-1])

    def resample_to_week_data(self, df):
        w_df = df.resample('W').agg(self.func_price_map).dropna()
        w_df.index = w_df.index + to_offset("-2D")
        return w_df

    def update_weekly_df(self):
        if self.daily_df is None:
            return

        if self.weekly_df is None:
            self.weekly_df = self.resample_to_week_data(self.daily_df)
        else:
            recent_df = self.resample_to_week_data(self.daily_df[-10:])
            last_week_df = self.weekly_df.tail(1)
            new_data_index = list(recent_df.index).index(last_week_df.index)
            if 0 <= new_data_index < len(recent_df):
                self.weekly_df.drop(last_week_df.index, inplace=True)
                self.weekly_df = pd.concat([self.weekly_df, recent_df[new_data_index:]])

    def recent_week_high(self, recent_weeks: int = 7) -> float:
        if self.weekly_df is None:
            return None

        n = recent_weeks
        if len(self.weekly_df) < recent_weeks:
            n = len(self.weekly_df)

        return self.weekly_df['high'][-1 * n:].max()

    def recent_week_low(self, recent_weeks: int = 7) -> float:
        if self.weekly_df is None:
            return None

        n = recent_weeks
        if len(self.weekly_df) < recent_weeks:
            n = len(self.weekly_df)

        return self.weekly_df['low'][-1 * n:].min()

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
        if self.daily_df is None:
            return None

        return self.data_df.iloc[-1, :]

    @property
    def latest_week_bar(self) -> Series:
        if self.weekly_df is None:
            return None

        return self.weekly_df.iloc[-1, :]

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
    def last_bottom_daily(self) -> Optional[Any]:
        if not self.centrum or self.weekly_df is None:
            return None

        index = self.dc_detector.last_bottom_date
        return self.daily_df.loc[index]

    @property
    def last_bottom_weekly(self) -> Optional[Any]:
        if not self.centrum or self.weekly_df is None:
            return None

        index = self.wc_detector.last_bottom_date
        return self.weekly_df.loc[index]

    @property
    def last_top_daily(self) -> Optional[Any]:
        if not self.centrum or self.weekly_df is None:
            return None

        index = self.dc_detector.last_top_date
        return self.daily_df.loc[index]

    @property
    def last_top_weekly(self) -> Optional[Any]:
        if not self.centrum or self.weekly_df is None:
            return None

        index = self.wc_detector.last_top_date
        return self.weekly_df.loc[index]
