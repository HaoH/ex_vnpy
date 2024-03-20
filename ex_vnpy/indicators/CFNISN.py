from typing import List, Any

from talipp.indicator_util import has_valid_values, valid_values_length
from talipp.indicators.Indicator import Indicator
from ex_vnpy.capital_data import CapitalData
from ex_vnpy.indicators.CFNI import CFNI


class CFNISN(Indicator):
    """
    统计主力资金最近N天净流入累计
    """

    def __init__(self, dim: str, days: int, input_values: List[CapitalData] = None):
        super(CFNISN, self).__init__()

        self.days = days
        self.cfni = CFNI(dim)
        self.add_sub_indicator(self.cfni)

        self.initialize(input_values)

    def _calculate_new_value(self) -> Any:
        input_len = valid_values_length(self.input_values)
        if input_len <= self.days:
            return sum(self.cfni[:input_len])
        else:
            return self.output_values[-1] + self.cfni[-1] - self.cfni[-1 * (self.days + 1)]
