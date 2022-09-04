from dataclasses import dataclass
from datetime import datetime

from vnpy.trader.constant import Exchange
from vnpy.trader.object import BaseData


@dataclass
class BasicStockData(BaseData):
    """
    """

    symbol: str
    name: str
    exchange: Exchange

    industry_first: str
    industry_second: str
    industry_third: str
    industry_forth: str

    industry_code_zz: str
    industry_code: str

    ex_date: datetime
    update_dt: datetime

    index_sz50: bool = False
    index_hs300: bool = False
    index_zz500: bool = False
    index_zz800: bool = False
    index_zz1000: bool = False
    index_normal: bool = False

    shares_total: float = 0
    shares_total_a: float = 0
    shares_circ_a: float = 0
    shares_non_circ_a: float = 0

    def __post_init__(self):
        """"""
        self.vt_symbol = f"{self.symbol}.{self.exchange.value}"
