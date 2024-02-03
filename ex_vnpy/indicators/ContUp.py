from typing import List, Any
from talipp.indicators.Indicator import Indicator
from talipp.ohlcv import OHLCV


class ContUp(Indicator):
    """
    统计连续上涨天数
    """
    def __init__(self, input_values: List[float] = None):
        super(ContUp, self).__init__()
        self.initialize(input_values)

    def _calculate_new_value(self) -> Any:
        if len(self.input_values) < 2:
            return 0

        current_close = self.input_values[-1]
        prev_close = self.input_values[-2]

        if current_close > prev_close:
            new_days = self.output_values[-1] + 1
        else:
            new_days = 0

        return new_days

