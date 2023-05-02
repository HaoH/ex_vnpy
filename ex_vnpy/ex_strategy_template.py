from typing import Any

from ex_vnpy.position import Position
from src.helper.order_manager import OrderManager
from vnpy.trader.constant import Interval
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

    def do_scan(self) -> list:
        """
        根据当前的source manager的数据状态、策略配置，进行信号扫描
        :return:
        """
        signals = []
        for detector in self.signalDetectors:
            if detector.is_entry_signal(self.sm):
                signals.append(detector.signal_string())
        return signals

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

    def to_tv_pine_code(self, interval: Interval):
        pass