import logging
import os
from datetime import timedelta, datetime
from typing import Any, Dict, List, Tuple

from ex_vnpy.trade_plan import TradePlan
from ex_vnpy.position_manager import PositionManager
from src.helper.order_manager import OrderManager
from ex_vnpy.signal import SignalDetector, DetectorType, Signal
from vnpy.trader.constant import Interval, OrderType, Direction, Offset
from vnpy.trader.utility import virtual, TEMP_DIR
from vnpy_ctastrategy import CtaTemplate

from ex_vnpy.source_manager import SourceManager

logger = logging.getLogger("ExStrategyTemp")


class ExStrategyTemplate(CtaTemplate):

    detectors: Dict[DetectorType, List[SignalDetector]] = {}
    sm: SourceManager = None    # 数据管理器
    om: OrderManager = None     # 订单管理器
    pm: PositionManager = None  # 仓位控制器
    fix_capital = 10000     # 资金总量
    price_tick: float = 0.01
    stop_loss_rate: float = 0.08
    unit_size: int = 100
    ta: Dict = None
    stoploss_ind: Dict = None

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
        self.pm = PositionManager(fix_capital=self.fix_capital, stop_loss_rate=self.stop_loss_rate,
                                  price_tick=self.price_tick, unit_size=self.unit_size, stoploss_ind=self.stoploss_ind)
        self.today = None

    def init_source_manager(self, source: SourceManager):
        self.sm = source
        self.sm.init_indicators(self.ta)

    def on_init_data(self, sm: SourceManager, om: OrderManager) -> None:
        self.init_source_manager(sm)
        self.om = om

    def add_signal_detector(self, detector: SignalDetector):
        if detector.sd_type not in self.detectors.keys():
            self.detectors[detector.sd_type] = []
        self.detectors[detector.sd_type].append(detector)

    def do_scan(self) -> List[Signal]:
        """
        根据当前的source manager的数据状态、策略配置，进行信号扫描
        :return: 返回所有[(有效信号,信号强度)] 列表
        """
        signals = []
        for sd_type, detectorList in self.detectors.items():
            for detector in detectorList:
                signal = detector.is_entry_signal(self.sm)
                if signal:
                    signals.append(signal)
        return signals

    def init_strategy(self):
        """
        回测的时候，初始化策略，对每一个指标进行首次计算
        :return:
        """
        for sd_type, detectorList in self.detectors.items():
            for detector in detectorList:
                detector.init_detector(self.sm)
        # for detector in self.detectors:
        #     detector.init_detector(self.sm)

    @virtual
    def to_string(self) -> str:
        pass

    def to_tv_pine_code(self, interval: Interval):
        # self.om.trades
        hold_days = []
        entry_days = []
        exit_days = []
        all_trades = list(self.om.trades.values())
        for x in range(0, len(all_trades), 2):
            trades = all_trades[x: x + 2]
            if len(trades) < 2:
                break

            if trades[0].offset == Offset.OPEN and trades[1].offset == Offset.CLOSE:
                aday = trades[0].datetime
                if interval == Interval.WEEKLY:
                    aday -= timedelta(days=aday.weekday())
                entry_days.append(
                    "    array.push(entry_days, timestamp({}, {}, {}, 9, 30))".format(aday.year, aday.month, aday.day))

                while aday <= trades[1].datetime:
                    hold_days.append(
                        "    array.push(hold_days, timestamp({}, {}, {}, 9, 30))".format(aday.year, aday.month,
                                                                                         aday.day))
                    aday += timedelta(days=1)
                    if interval == Interval.WEEKLY:
                        aday += timedelta(days=6)

                end_day = trades[1].datetime
                if interval == Interval.WEEKLY:
                    end_day -= timedelta(days=aday.weekday())
                    end_day += timedelta(days=7)
                exit_days.append(
                    "    array.push(exit_days, timestamp({}, {}, {}, 9, 30))".format(end_day.year, end_day.month,
                                                                                     end_day.day))

        template = """
// This source code is subject to the terms of the Mozilla Public License 2.0 at https://mozilla.org/MPL/2.0/
// © wukong2020

//@version=5
indicator("策略复盘可视化", overlay=true, max_lines_count=500)

var int[] hold_days = na
if na(hold_days)
    hold_days := array.new<int>({})
{}
    array.sort(hold_days)
    
//entry_days
int[] entry_days = na
if na(entry_days)
    entry_days := array.new<int>({})
{}
    array.sort(entry_days)
    
//end_days
var int[] exit_days = na
if na(exit_days)
    exit_days := array.new<int>({})
{}
    array.sort(exit_days)

var line lastLine = na

if not na(entry_days) and array.binary_search(entry_days, time) >= 0
    lastLine := line.new(bar_index, close, bar_index, close, color=color.purple, width=2, style=line.style_arrow_right)
    lastLine

if not na(hold_days) and array.binary_search(hold_days, time) >= 0
    line.set_xy2(lastLine, bar_index, open)
    lastLine := lastLine
    lastLine
        """.format(len(hold_days), "\n".join(hold_days), len(entry_days), "\n".join(entry_days), len(exit_days),
                   "\n".join(exit_days))

        os.chdir(TEMP_DIR)  # Change working directory
        fp = open(f"pine/strategy_review-{self.symbol_name}-{datetime.now().isoformat()}.txt", "w")
        fp.write(template)
        fp.close()

    def send_order(self, order_type: OrderType, direction: Direction, offset: Offset, volume: float,
                   price: float = None, trigger_price: float = None, **kwargs) -> list:
        """
        替代CtaTemplate的send_order
        :param order_type:
        :param direction:
        :param offset:
        :param volume:
        :param price:
        :param trigger_price:
        :param kwargs:
        :return:
        """
        if not self.trading:
            return []

        if order_type in (OrderType.STOP, OrderType.STOP_LOSS, OrderType.STOP_WIN):
            vt_orderid = self.om.send_stop_order(order_type, direction, offset, volume, price, trigger_price)
        else:
            vt_orderid = self.om.send_limit_order(order_type, direction, offset, price, volume)
        return [vt_orderid]

    def buy_high(self, trigger_price: float, volume: float, price: float = None) -> list:
        """
        定价止损订单，价格向上触发trigger_price，以price的价格下单买入
        """
        return self.send_order(OrderType.STOP_LOSS, Direction.LONG, Offset.OPEN, volume, price, trigger_price)

    def buy_low(self, trigger_price: float, volume: float, price: float = None) -> list:
        """
        定价止盈订单，价格向下触发trigger_price，以price的价格下单买入
        """
        return self.send_order(OrderType.STOP_WIN, Direction.LONG, Offset.OPEN, volume, price, trigger_price, )

    def sell_high(self, trigger_price: float, volume: float, price: float = None) -> list:
        """
        定价止盈订单，价格向上触发trigger_price，以price的价格下单卖出
        """
        return self.send_order(OrderType.STOP_WIN, Direction.SHORT, Offset.CLOSE, volume, price, trigger_price, )

    def sell_low(self, trigger_price: float, volume: float, price: float = None) -> list:
        """
        定价止损订单，价格向下触发trigger_price，以price的价格下单卖出
        """
        return self.send_order(OrderType.STOP_LOSS, Direction.SHORT, Offset.CLOSE, volume, price, trigger_price, )

    def buy_market(self, volume: float, price: float = None) -> list:
        """
        市价购买
        :param volume:
        :param price:
        :return:
        """
        return self.send_order(OrderType.MARKET, Direction.LONG, Offset.OPEN, volume, price,)

    def log_parameters(self):
        logger.info("Strategy:")
        logger.info(f"stop_loss_rate: {self.stop_loss_rate}, unit_size: {self.unit_size}, price_tick: {self.price_tick}")
        logger.info(f"ta: {self.ta}")
        for sd_type, detectors in self.detectors.items():
            logger.info(f"detector_type: {sd_type}")
            for detector in detectors:
                logger.info(detector.to_string())
