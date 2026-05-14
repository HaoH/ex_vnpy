from dataclasses import dataclass
import datetime as dt

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

    def __post_init__(self) -> None:
        """"""
        if isinstance(self.exchange, str):
            self.exchange = Exchange(self.exchange)
        if isinstance(self.market, str):
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

    ex_date: dt.datetime
    update_dt: dt.datetime

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
    update_dt: dt.datetime

    publish_date: dt.datetime | None = None
    exit_date: dt.datetime | None = None
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
    circulation_shares: float = 0

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

@dataclass
class SharesData(BaseData):
    """
    Candlestick bar data of a certain trading period.
    """
    symbol: str = ""
    exchange: Exchange = Exchange.SSE
    # date: dt.date = None
    start_dt: dt.date | None = None
    end_dt: dt.date | None = None
    total: float = 0.0              # 总股本
    circulation_a: float = 0.0      # 流通 A 股
    non_circulation_a: float = 0.0  # 非流通 A 股
    total_a: float = 0.0            # A 股总股本
    free_circulation: float = 0.0   # 自由流通股本

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"

@dataclass
class DailyStatData(BarData):

    # 当天收盘价
    change_pct: float = 0.0
    open_chg_pct: float = 0.0
    up_limit: float = 0.0
    down_limit: float = 0.0
    volume_ratio: float = 0.0
    change_pct_5u: float = 0.0
    change_pct_10u: float = 0.0
    change_pct_22u: float = 0.0
    cont_up_days: int = 0
    cont_max_up_days: int = 0

    # 主力连续净流入天数
    capital_ni_days: int = 0
    # 主力当日净流入成交量
    capital_ni_volume: int = 0
    # 主力当日净流入金额
    capital_ni_turnover: int = 0
    # 主力3日净流入成交量
    capital_ni_volume_3u: int = 0
    # 主力3日净流入金额
    capital_ni_turnover_3u: int = 0
    # 主力净流入占流通盘比例
    capital_ni_ratio: float = 0

    # 流通股数
    circulation_shares: float = 0.0
