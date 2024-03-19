from typing import List, Any

from talipp.indicator_util import has_valid_values
from talipp.indicators.Indicator import Indicator
from ex_vnpy.capital_data import CapitalData


class CFNI(Indicator):
    """
    Capital Flow Net Income
    对资金净流入进行汇总
    """

    def __init__(self, dim: str, input_values: List[CapitalData] = None):
        super().__init__()

        self.dim = dim
        self.initialize(input_values)

    def _calculate_new_value(self) -> Any:
        if not has_valid_values(self.input_values, 1):
            return None

        nv = self.input_values[-1]
        net_capital = 0
        if self.dim == 'volume':
            net_capital = nv.volume_buy_XL + nv.volume_buy_L - nv.volume_sell_XL - nv.volume_sell_L
        elif self.dim == 'order_count':
            net_capital = nv.order_count_buy_XL + nv.order_count_buy_L - nv.order_count_sell_XL - nv.order_count_sell_L
        elif self.dim == 'order_volume':
            net_capital = nv.order_volume_buy_XL + nv.order_volume_buy_L - nv.order_volume_sell_XL - nv.order_volume_sell_L
        elif self.dim == 'turnover':
            net_capital = nv.turnover_buy_XL + nv.turnover_buy_L - nv.turnover_sell_XL - nv.turnover_sell_L

        return net_capital