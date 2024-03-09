from typing import List, Any
from talipp.indicators.Indicator import Indicator
from ex_vnpy.capital_data import CapitalData
from ex_vnpy.indicators.CFNI import CFNI


class CFNIS(Indicator):
    """
    Capital Flow Net Income Sum
    对资金净流入进行汇总
    """

    def __init__(self, dim: str, input_values: List[CapitalData] = None):
        super().__init__()
        self.dim = dim

        self.cfni = CFNI(dim)
        self.add_sub_indicator(self.cfni)

        self.initialize(input_values)

    def _calculate_new_value(self) -> Any:
        input_len = len(self.input_values)
        if input_len <= 0:
            return 0
        elif input_len == 1:
            return self.cfni[-1]
        else:
            return self.output_values[-1] + self.cfni[-1]