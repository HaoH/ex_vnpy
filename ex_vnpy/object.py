from dataclasses import dataclass
from datetime import datetime

from vnpy.trader.constant import Exchange, Market
from vnpy.trader.object import BaseData

@dataclass
class BasicSymbolData:
    id: int
    symbol: str
    name: str
    exchange: Exchange
    market: Market
    type: str
    status: str

    def __post_init__(self):
        """"""
        if type(self.exchange) == str:
            self.exchange = Exchange(self.exchange)
        if type(self.market) == str:
            self.market = Market(self.market)
        self.vt_symbol = f"{self.symbol}.{self.exchange.value}"


@dataclass
class BasicStockData(BasicSymbolData):
    """
    """
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

    # shares_total: float = 0
    # shares_total_a: float = 0
    # shares_circ_a: float = 0
    # shares_non_circ_a: float = 0


@dataclass
class BasicIndexData(BasicSymbolData):
    full_name: str
    volume: int
    turnover: int
    update_dt: datetime

    publish_date: datetime = None
    exit_date: datetime = None
    has_price: bool = True
    has_weight: bool = True
    has_components: bool = True
    is_core_index: bool = False


    # symbol_type = "INDX"
