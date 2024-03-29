import logging
import os
from datetime import timedelta, datetime
from typing import Any, Dict, List

from ex_vnpy.manager.position_manager import PositionManager
from ex_vnpy.manager.order_manager import OrderManager
from ex_vnpy.signal import SignalDetector, DetectorType, Signal
from ex_vnpy.trade_plan import TradePlanData, TradePlan
from vnpy.trader.constant import Interval, OrderType, Direction, Offset
from vnpy.trader.utility import virtual, TEMP_DIR
from vnpy_ctastrategy import CtaTemplate

from ex_vnpy.manager.source_manager import SourceManager


class ExStrategyTemplate(CtaTemplate):
    detectors: Dict[DetectorType, List[SignalDetector]] = {}
    sm: SourceManager = None  # 数据管理器
    om: OrderManager = None  # 订单管理器
    pm: PositionManager = None  # 仓位控制器
    stoploss_rate: float = 0.08

    fix_capital = 100000  # 资金总量
    price_tick: float = 0.01
    unit_size: int = 100  # 每一手多少股
    commission_rate = 0.0002  # 手续费率
    slippage = 0.005  # 交易滑点
    risk_free = 0
    annual_days = 240
    entry_order_type = 'LIMIT'

    ta = []
    stoploss_ind = {}
    stoploss_settings = {}

    trade_plans: List[TradePlan] = []    # 当前交易计划
    all_trade_plans: List[TradePlan] = []  # 所有交易计划

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
        self.pm = PositionManager(stoploss_rate=self.stoploss_rate,
                                  fix_capital=self.fix_capital,
                                  price_tick=self.price_tick,
                                  unit_size=self.unit_size,
                                  commission_rate=self.commission_rate,
                                  slippage=self.slippage,
                                  stoploss_ind=self.stoploss_ind)
        self.today = None
        self.trade_plans = []
        self.all_trade_plans = []

    def set_source_manager(self, source: SourceManager):
        self.sm = source

    def set_order_manager(self, om: OrderManager):
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
        :param is_protected: 保护性订单，表示必须执行，一旦价格暴涨暴跌，以市价成交
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

        if order_type in (OrderType.STOP, OrderType.STP, OrderType.MIT, OrderType.LIT, OrderType.STL):
            vt_orderid = self.om.send_stop_order(order_type, direction, offset, volume, price, trigger_price)
        else:
            vt_orderid = self.om.send_limit_order(order_type, direction, offset, price, volume)
        return [vt_orderid]

    def buy_high(self, trigger_price: float, volume: float, price: float = None, is_market: bool = True) -> list:
        """
        定价止损订单，价格向上触发trigger_price，以price的价格下单买入
        """
        order_type = OrderType.STP if is_market else OrderType.STL
        return self.send_order(order_type, Direction.LONG, Offset.OPEN, volume, price, trigger_price)

    def buy_low(self, trigger_price: float, volume: float, price: float = None, is_market: bool = True) -> list:
        """
        定价止盈订单，价格向下触发trigger_price，以price的价格下单买入
        """
        order_type = OrderType.MIT if is_market else OrderType.LIT
        return self.send_order(order_type, Direction.LONG, Offset.OPEN, volume, price, trigger_price)

    def sell_high(self, trigger_price: float, volume: float, price: float = None, is_market: bool = True) -> list:
        """
        定价止盈订单，价格向上触发trigger_price，以price的价格下单卖出
        """
        order_type = OrderType.MIT if is_market else OrderType.LIT
        return self.send_order(order_type, Direction.SHORT, Offset.CLOSE, volume, price, trigger_price)

    def sell_low(self, trigger_price: float, volume: float, price: float = None, is_market: bool = False) -> list:
        """
        定价止损订单，价格向下触发trigger_price，以price的价格下单卖出
        """
        order_type = OrderType.STP if is_market else OrderType.STL
        return self.send_order(order_type, Direction.SHORT, Offset.CLOSE, volume, price, trigger_price)

    def buy_market(self, volume: float, price: float = None) -> list:
        """
        市价购买
        :param volume:
        :param price:
        :return:
        """
        return self.send_order(OrderType.MARKET, Direction.LONG, Offset.OPEN, volume, price, )

    def log_parameters(self):
        detector_strs = []
        for sd_type, detectors in self.detectors.items():
            for detector in detectors:
                detector_strs.append(detector.to_string())
        d_content = "\n".join(detector_strs)

        content = f"""
Strategy
1) stoploss_rate: {self.stoploss_rate}, unit_size: {self.unit_size}, price_tick: {self.price_tick}, commission_rate: {self.commission_rate}, slippage: {self.slippage}
2) detectors:
{d_content}"""
        return content

    def update_stoploss_settings(self, sl_settings: dict):
        self.update_setting(sl_settings)
        self.pm.update_stoploss_settings(sl_settings)

    def update_trade_settings(self, tr: dict):
        self.update_setting(tr)
        self.pm.update_settings(fix_capital=tr["fix_capital"],
                                price_tick=tr["price_tick"],
                                unit_size=tr["unit_size"],
                                commission_rate=tr["commission_rate"],
                                slippage=tr["slippage"])

    def get_all_trade_plans(self):
        return self.all_trade_plans

    def get_all_trade_plan_data(self) -> List[TradePlanData]:
        tps = []
        for tp in self.all_trade_plans:
            tpd = tp.extract_data()
            tps.append(tpd)
        return tps

    def import_trade_plan(self, tp: TradePlan):
        self.all_trade_plans.append(tp)

    def clear_data(self):
        self.trade_plans.clear()
        self.all_trade_plans.clear()

