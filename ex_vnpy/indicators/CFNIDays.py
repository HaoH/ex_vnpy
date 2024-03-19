from typing import List, Any

from talipp.indicator_util import has_valid_values, valid_values_length
from talipp.indicators.Indicator import Indicator
from ex_vnpy.capital_data import CapitalData
from ex_vnpy.indicators.CFNI import CFNI


class CFNIDays(Indicator):
    """
    统计主力资金连续净流入天数
    """
    def __init__(self, dim: str, input_values: List[CapitalData] = None):
        super(CFNIDays, self).__init__()

        self.cfni = CFNI(dim)
        self.add_sub_indicator(self.cfni)

        self.initialize(input_values)

    def _calculate_new_value(self) -> Any:
        input_len = valid_values_length(self.input_values)
        if input_len < 1:
            return 0
        elif input_len < 2:
            return 1 if self.cfni[-1] > 0 else -1 if self.cfni[-1] < 0 else 0
        else:
            if self.cfni[-1] > 0 and self.cfni[-2] > 0:
                return self.output_values[-1] + 1
            elif self.cfni[-1] < 0 and self.cfni[-2] < 0:
                return self.output_values[-1] - 1
            elif self.cfni[-1] > 0 and self.cfni[-2] <= 0:
                return 1
            elif self.cfni[-1] < 0 and self.cfni[-2] >= 0:
                return -1
            elif self.cfni[-1] == 0:
                return 0

            return 0
