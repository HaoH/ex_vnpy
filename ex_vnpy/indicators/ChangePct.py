from typing import List, Any
from talipp.indicators.Indicator import Indicator
from talipp.ohlcv import OHLCV


class ChangePct(Indicator):
    """
    统计时间段内的Change Percent
    """
    def __init__(self, period: int, is_plus: bool = True, input_values: List[float] = None):
        super(ChangePct, self).__init__()
        self.period = period
        self.is_plus = is_plus      # 是否返回增量变化
        self.initialize(input_values)

    def _calculate_new_value(self) -> Any:
        if len(self.input_values) < 2:
            return 0

        current_input = self.input_values[-1]
        if len(self.input_values) < self.period + 1:
            prev_input = self.input_values[0]
        else:
            prev_input = self.input_values[-1 * (self.period + 1)]

        if prev_input == 0:
            return 0

        if self.is_plus:
            change_pct = (current_input - prev_input) / prev_input
        else:
            change_pct = current_input / prev_input
        return change_pct

