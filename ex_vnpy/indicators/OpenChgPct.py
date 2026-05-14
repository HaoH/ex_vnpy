from typing import Any

from talipp.indicator_util import has_valid_values
from talipp.indicators.Indicator import Indicator
from talipp.ohlcv import OHLCV


class OpenChgPct(Indicator):
    """
    统计时间段内的开盘价格变动 Open Change Percent
    """
    def __init__(self, input_values: list[OHLCV] | None = None):
        super(OpenChgPct, self).__init__()
        self.initialize(input_values)

    def _calculate_new_value(self) -> Any:
        if not has_valid_values(self.input_values, 2):
            return 0

        current_input = self.input_values[-1]
        prev_input = self.input_values[-2]

        if prev_input is None:
            return 0

        change_pct = (current_input.open - prev_input.close) / prev_input.close
        return change_pct

