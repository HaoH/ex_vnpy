from dataclasses import dataclass
from datetime import datetime

from vnpy.trader.constant import Exchange, Market
from vnpy.trader.object import BaseData, BarData


@dataclass
class BasicSymbolData(BaseData):
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


@dataclass
class ExBarData(BarData):
    """
    扩展BarData，加入资金流数据
    """
    order_count_buy_XL: int = 0
    order_count_buy_L: int = 0
    order_count_buy_M: int = 0
    order_count_buy_S: int = 0
    order_count_sell_XL: int = 0
    order_count_sell_L: int = 0
    order_count_sell_M: int = 0
    order_count_sell_S: int = 0
    order_volume_buy_XL: int = 0
    order_volume_buy_L: int = 0
    order_volume_buy_M: int = 0
    order_volume_buy_S: int = 0
    order_volume_sell_XL: int = 0
    order_volume_sell_L: int = 0
    order_volume_sell_M: int = 0
    order_volume_sell_S: int = 0
    volume_buy_XL: int = 0
    volume_buy_L: int = 0
    volume_buy_M: int = 0
    volume_buy_S: int = 0
    volume_sell_XL: int = 0
    volume_sell_L: int = 0
    volume_sell_M: int = 0
    volume_sell_S: int = 0
    turnover_buy_XL: float = 0
    turnover_buy_L: float = 0
    turnover_buy_M: float = 0
    turnover_buy_S: float = 0
    turnover_sell_XL: float = 0
    turnover_sell_L: float = 0
    turnover_sell_M: float = 0
    turnover_sell_S: float = 0

    @property
    def open(self):
        return self.open_price

    @property
    def high(self):
        return self.high_price

    @property
    def low(self):
        return self.low_price

    @property
    def close(self):
        return self.close_price
