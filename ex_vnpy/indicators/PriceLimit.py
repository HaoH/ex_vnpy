from typing import List, Any

from talipp.indicator_util import has_valid_values
from talipp.indicators.Indicator import Indicator
from vnpy.trader.constant import Exchange


class PriceLimit(Indicator):
    """
    计算每天的价格涨跌停价格Up Limit, Down Limit
    """
    def __init__(self, symbol: str, input_values: List[float] = None):
        super(PriceLimit, self).__init__()
        # 规则
        # 1） 主板、中小板，涨跌幅10%
        # 2） 创业板、科创板，涨跌幅20%,  300、688
        # 3） 退市整理板、风险警示板，涨跌幅5%
        # 4） 北交所，涨跌幅30%，43、82、83、87、88 (暂不支持）
        self.change_limit = 0.1
        if symbol[:3] in ("688", "300"):
            self.change_limit *= 2
        elif symbol[:2] in ("43", "82", "83", "87", "88"):
            self.change_limit *= 3

        self.initialize(input_values)

    def _calculate_new_value(self) -> Any:
        if not has_valid_values(self.input_values, 2):
            return 0

        prev_input = self.input_values[-2]
        if prev_input == 0:
            return 0

        up_limit = round(prev_input * (1 + self.change_limit), 2)
        down_limit = round(prev_input * (1 - self.change_limit), 2)

        return (up_limit, down_limit)

