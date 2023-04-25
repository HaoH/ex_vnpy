from typing import Any

from ex_vnpy.position import Position
from src.helper.order_manager import OrderManager
from vnpy.trader.utility import virtual
from vnpy_ctastrategy import CtaTemplate

from ex_vnpy.source_manager import SourceManager
from src.signals import SignalDetector


class ExStrategyTemplate(CtaTemplate):

    signalDetectors = []
    sm: SourceManager = None
    om: OrderManager = None
    position: Position = None
    price_tick: float = 0.01

    def __init__(
        self,
        cta_engine: Any,
        strategy_name: str,
        vt_symbol: str,
        setting: dict,
    ):

        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.symbol_name = ""
        self.sm = None
        self.om = None
        self.position = Position(price_tick=self.price_tick)
        self.today = None

    def set_source_manager(self, source: SourceManager):
        self.sm = source

    def on_init_data(self, sm: SourceManager, om: OrderManager) -> None:
        self.set_source_manager(sm)
        self.om = om

    def add_signal_detector(self, detector: SignalDetector):
        self.signalDetectors.append(detector)

    def do_scan(self, daily_df, weekly_df) -> list:
        signals = []
        for detector in self.signalDetectors:
            if detector.is_entry_signal(daily_df, weekly_df):
                signals.append(detector.signal_string())
        return signals

    def do_scan_incremental(self):
        signals = []
        for detector in self.signalDetectors:
            if detector.is_entry_signal_incremental(self.sm):
                signals.append(detector.signal_string())
        return signals

    def scan_till_today(self) -> list:
        if self.sm.daily_df is not None and self.sm.weekly_df is not None:
            return self.do_scan(self.sm.daily_df, self.sm.weekly_df)
        return []

    def init_strategy(self):
        """
        回测的时候，初始化策略，对每一个指标进行首次计算
        :return:
        """
        for detector in self.signalDetectors:
            detector.init_detector(self.sm)

    @virtual
    def to_string(self) -> str:
        pass

