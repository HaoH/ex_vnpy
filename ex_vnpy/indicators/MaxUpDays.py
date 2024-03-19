from typing import List, Any

from talipp.indicator_util import has_valid_values
from talipp.indicators.Indicator import Indicator
from talipp.ohlcv import OHLCV


class MaxUpDays(Indicator):
    """
    统计近似连续涨停天数
    """
    def __init__(self, input_values: List[float] = None):
        super(MaxUpDays, self).__init__()

        self.break_days = []
        self.add_managed_sequence(self.break_days)

        self.initialize(input_values)

    def _calculate_new_value(self) -> Any:
        if not has_valid_values(self.input_values, 2):
            self.break_days.append(1)
            return 0

        current_close = self.input_values[-1]
        prev_close = self.input_values[-2]

        if (current_close - prev_close) / prev_close > 0.095:
            self.break_days.append(0)
            up_days = self.output_values[-1] + 1
        else:
            if self.break_days[-1] == 0:
                self.break_days.append(1)
                up_days = self.output_values[-1]
            else:
                self.break_days.append(self.break_days[-1] + 1)
                up_days = 0

        return up_days

