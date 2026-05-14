from typing import Any

from talipp.indicator_util import has_valid_values, valid_values_length
from talipp.indicators.Indicator import Indicator
from ex_vnpy.capital_data import CapitalData
from ex_vnpy.indicators.CFNI import CFNI


class CFNIRatio(Indicator):
    """
    Capital Flow Net Income Ratio
    大单净量：大单净流入/流通股股数
    """

    def __init__(self, dim: str, input_values: list[CapitalData] | None = None):
        super().__init__()
        self.dim = dim

        self.cfni = CFNI(dim)
        self.add_sub_indicator(self.cfni)

        self.initialize(input_values)

    def _calculate_new_value(self) -> Any:
        if not has_valid_values(self.input_values, 1) or self.dim not in ('volume', 'order_volume'):
            return 0

        nv = self.input_values[-1]
        return self.cfni[-1] / nv.circulation_shares if nv.circulation_shares != 0 else 0