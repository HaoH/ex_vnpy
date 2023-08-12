from abc import ABC
from datetime import timedelta
from enum import Enum

import pandas as pd
from pandas import DataFrame

from ex_vnpy.manager.source_manager import SourceManager
from vnpy.trader.constant import Direction
from vnpy.trader.utility import virtual


class DetectorType(Enum):
    Trend = 'trend'  # 趋势探测器
    Wave = 'wave'  # 震荡探测器
    Other = 'other'  # 其他辅助探测器


class Signal(object):
    """
    一次入场信号，包括了 触发价格、入场价格、止损价格、止盈价格
    """

    direction: Direction = Direction.LONG
    weight: float = 1
    trigger_price: float = 0
    buy_price: float = 0
    sl_price: float = 0
    detector: 'SignalDetector' = None

    def __init__(self, weight, trigger_price, buy_price, sl_price, detector):
        self.weight = weight
        self.trigger_price = trigger_price
        self.buy_price = buy_price
        self.sl_price = sl_price
        self.detector = detector


class SignalDetector(ABC):
    """"""
    parameters = []
    sd_type: DetectorType = None
    weight: int = 1
    stop_loss_rate: float = 0.08
    active_state: bool = True
    inited: bool = False

    def __init__(self, setting=None):
        """"""
        self.name = self.__class__.__name__
        self.sd_type = DetectorType.Other
        self.weight = 1
        self.update_setting(setting)

    def update_setting(self, setting: dict):
        """
        Update parameter with value in setting dict.
        """
        if setting is None:
            return

        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    @classmethod
    def get_class_parameters(cls):
        """
        Get default parameters dict of strategy class.
        """
        class_parameters = {}
        for name in cls.parameters:
            class_parameters[name] = getattr(cls, name)
        return class_parameters

    def get_parameters(self):
        """
        Get strategy parameters dict.
        """
        signal_detector_parameters = {}
        for name in self.parameters:
            signal_detector_parameters[name] = getattr(self, name)
        return signal_detector_parameters

    def enable(self, enable=True):
        self.active_state = enable

    @property
    def is_active(self) -> bool:
        return self.active_state

    def to_string(self):
        params = []
        for param in self.parameters:
            params.append(str(getattr(self, param)))
        return self.name + '/' + '/'.join(params)

    @virtual
    def get_result_detail(self):
        pass

    @virtual
    def signal_string(self):
        return self.to_string()

    @virtual
    def is_entry_signal(self, sm: SourceManager) -> Signal:
        """"""
        pass

    @virtual
    def is_exit_signal(self, sm: SourceManager) -> Signal:
        """"""
        pass

    def init_detector(self, sm: SourceManager) -> bool:
        pass

    def update_bar(self, sm: SourceManager):
        pass

    @staticmethod
    def resample_down(week_df, day_index):
        day_df = week_df.resample('B').bfill()

        if len(day_df) > 0:
            first_item = day_df.iloc[0].to_dict()
            first_dt = day_df.index[0]
            first_week_day_df = DataFrame([first_item for _ in range(4)],
                                          index=[first_dt - timedelta(i) for i in range(4, 0, -1)])
            day_df = pd.concat([first_week_day_df, day_df])

        day_df = day_df[day_df.index.isin(list(day_index))]
        return day_df

    def get_trigger_price_for_last_signal(self, sm: SourceManager, strength: float) -> float:
        """
        根据信号探测器最后一个信号的特点，确定订单触发价格
        :param sm:
        :param strength: 信号强度，不同强度的信号可以个性化调整下单触发价格
        :return:
        """
        trigger_price = sm.recent_week_high(3)
        # 当low pivot所在的bar太长的时候，避免出现触发价太高的情况，将触发价设置在pivot所在周的高点
        if sm.last_week_hl_gap(2) > 0.12:
            trigger_price = sm.recent_week_high(2)
        return trigger_price

    def get_buy_price_for_last_signal(self, sm: SourceManager, strength: float) -> float:
        """
        根据信号探测器最后一个信号的特点，确定订单的买入价格
        :param sm:
        :param strength: 信号强度，不同强度的信号可以个性化调整下单价格
        :return:
        """
        buy_price = sm.latest_daily_bar['close']
        if self.stop_loss_rate > 0:
            # TODO: 对于隐式背离，last_bottom_low_w 可能远远低于当前价， 当前处于 ///^ 的位置，会导致买入价异常低，订单基本无效；
            last_bottom_low_w = sm.last_bottom_low_w if sm.last_bottom_low_w is not None else buy_price
            low = min(sm.recent_week_low(3), last_bottom_low_w)  # 最新一周的pivot可能还未成型
            buy_price = low / (1 - self.stop_loss_rate)
            # 如果最近出现了超过12%幅度的bar，说明价格到了强弩之末，可能暴力反弹，允许买入价适当提高3%
            if sm.recent_week_hl_gap(3) > 0.12:
                buy_price *= 1.03
        return buy_price

    def stoploss_price(self, sm: SourceManager, tp: 'TradePlan'):
        return None


