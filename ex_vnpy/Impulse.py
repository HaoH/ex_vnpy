from typing import List, Any

from talipp.indicators import MACD, EMA
from talipp.indicators.Indicator import Indicator, ValueExtractorType


class Impulse(Indicator):
    """
    Elder Impulse System

    Output: a list of floats
    1: green color
    0: bule color
    -1: red color
    """

    def __init__(self, fast_period: int, slow_period: int, signal_period: int, ema_period: int, input_values: List[float] = None, input_indicator: Indicator = None, value_extractor: ValueExtractorType = None):
        super().__init__(value_extractor = value_extractor)

        self.macd = MACD(fast_period, slow_period, signal_period)
        self.ema = EMA(ema_period)

        self.add_sub_indicator(self.macd)
        self.add_sub_indicator(self.ema)

        self.initialize(input_values, input_indicator)

    def _calculate_new_value(self) -> Any:
        if len(self.macd) >= 2 and self.macd[-2].histogram is not None and len(self.ema) > 0:
            his_trend = 0
            if self.macd[-1].histogram > self.macd[-2].histogram:
                his_trend = 1
            elif self.macd[-1].histogram < self.macd[-2].histogram:
                his_trend = -1

            ema_trend = 0
            if self.ema[-1] > self.ema[-2]:
                ema_trend = 1
            elif self.ema[-1] < self.ema[-2]:
                ema_trend = -1

            trend_sum = his_trend + ema_trend
            return 1 if trend_sum > 0 else -1 if trend_sum < 0 else 0
        return None