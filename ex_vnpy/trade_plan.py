import logging
from datetime import datetime
from enum import Enum
from typing import List

from ex_vnpy.signal import SignalDetector
from ex_vnpy.source_manager import SourceManager
from vnpy.trader.object import TradeData, OrderData
from vnpy.trader.constant import Direction, Offset
from vnpy_ctastrategy import StopOrder
from vnpy_ctastrategy.base import StopOrderStatus

logger = logging.getLogger("TradePlan")


class PlanStatus(Enum):
    PLAN = 0                   # "计划中", 初始状态
    WAITING = 1                # "等待中", 入场条件监控中
    OPEN = 2                   # "建仓中", 入场条件已触发，等待限价成交中
    HOLD = 3                   # "持仓中", 已完成入场
    EXIT = 4                   # "退出中", 止损订单已触发
    CLOSE = 5                  # "已结束", 止损订单已成交, 已清仓
    CANCEL = 6                 # "已撤销"


class TradePlan:
    """
    每一次入场都是提前计划好的，包括入场价格、止损价格、止盈价格（这些信息包含在信号中）。
    入场成功之后，相关的仓位信息保存在本结构中，包括入场订单、止损订单。一个股票可以同时多次入场（由不同的信号驱动）
    """

    direction: Direction = Direction.LONG
    status: PlanStatus = PlanStatus.PLAN
    plan_date: datetime = None
    entry_date: datetime = None                   # 入场日期
    stoploss_date: datetime = None                # 止损日期
    stoploss_price_date: datetime = None          # 止损价格更新的日期，用来控制止损价格，只允许同一周内下降，跨周不允许下降
    entry_trigger_price: float = 0
    entry_buy_price: float = 0
    stoploss_price: float = 0
    volume: float = 0
    strength: float = 0
    detectors: List[SignalDetector] = []
    stoploss_rate: float = 0.08

    entry_trigger_order_id: str = ""        # 入场触发的StopOrder订单id
    entry_order_id: str = ""                # 入场的LimitOrder订单id
    exit_trigger_order_id: str = ""         # 退场的StopOrder订单id
    exit_order_id: str = ""                 # 退场的LimitOrder订单id
    stoploss_order: StopOrder = None
    # TODO: 增加stoploss order执行的limit order id

    def __init__(self, entry_trigger_price, entry_buy_price, sl_price, volume, plan_date, strength, stoploss_rate = 0.08, direction: Direction = Direction.LONG):
        self.entry_trigger_price = entry_trigger_price
        self.entry_buy_price = entry_buy_price

        self.stoploss_price = sl_price
        self.stoploss_rate = stoploss_rate
        self.stoploss_price_date = plan_date
        self.stoploss_order: StopOrder = None

        self.volume = volume
        self.plan_date = plan_date
        self.strength = strength
        self.direction = direction

    def set_entry_trigger_order(self, order_ids: List[str]):
        if len(order_ids) <= 0:
            return

        self.entry_trigger_order_id = order_ids[0]
        self.status = PlanStatus.WAITING
        logger.debug(f"[TP][SetOrder][EntryTrigger] trigger_order_id: {self.entry_trigger_order_id}, plan_date: {self.plan_date.strftime('%Y-%m-%d')}, status: {self.status}")

    def set_entry_order(self, vt_orderids):
        if len(vt_orderids) <= 0:
            return

        self.entry_order_id = vt_orderids[0]
        self.status = PlanStatus.OPEN
        logger.debug(f"[TP][SetOrder][Entry] limit_order_id: {self.entry_order_id}, plan_date: {self.plan_date.strftime('%Y-%m-%d')}, status: {self.status}")

    def set_exit_trigger_order(self, vt_orderid: str, sl_order: StopOrder):
        self.exit_trigger_order_id = vt_orderid
        self.stoploss_order = sl_order
        logger.debug(f"[TP][SetOrder][ExitTrigger] trigger_order_id: {self.exit_trigger_order_id}, plan_date: {self.plan_date.strftime('%Y-%m-%d')}, status: {self.status}")

    def set_exit_order(self, vt_orderids):
        if len(vt_orderids) <= 0:
            return

        self.exit_order_id = vt_orderids[0]
        self.status = PlanStatus.EXIT
        logger.debug(f"[TP][SetOrder][Exit] limit_order_id: {self.exit_order_id}, plan_date: {self.plan_date.strftime('%Y-%m-%d')}, status: {self.status}")

    @property
    def is_stoploss_order_active(self):
        return self.exit_trigger_order_id != "" and self.stoploss_order is not None and self.stoploss_order.status == StopOrderStatus.WAITING

    def is_price_high_enough(self, high_price, base_price, factor) -> float:
        return high_price >= base_price * (1 + factor * self.stoploss_rate)

    def accept_drawback_price(self, high_price, base_price, factor) -> float:
        return base_price + (high_price - base_price) * factor

    def update_stoploss_price(self, sm: SourceManager):
        sl_prices = [detector.stoploss_price(sm, self) for detector in self.detectors]
        valid_sl_prices = [x for x in sl_prices if x is not None]

        # 如果detector有止损设置，则采用该设置；如果没有，则采用通用策略
        new_sl_price = 0
        if len(valid_sl_prices) > 0:
            # 取所有策略的最低止损价格
            new_sl_price = min(valid_sl_prices)
        elif self.stoploss_rate > 0:
            # 根据价格走势，动态调整止损位
            new_sl_price = self.get_dynamic_stoploss_price(sm)

        # 止损价格只能上升，不能下降
        _, lw, _ = self.stoploss_price_date.isocalendar()
        _, tw, _ = sm.today.isocalendar()
        # 对于当周的数据，由于周线未定型，允许向下调整止损价；对于非当周数据，只允许向上调整
        if (lw == tw and self.entry_buy_price > self.stoploss_price) or new_sl_price > self.stoploss_price:
            logger.debug(f"[TP][SLPriceUpdate] date: {sm.last_date.strftime('%Y-%m-%d')},   stoploss_price: {self.stoploss_price:.2f} -> {new_sl_price:.2f}")
            self.stoploss_price = new_sl_price
            self.stoploss_price_date = sm.today

    def set_stoploss_price(self, new_stoploss_price: float):
        self.stoploss_price = new_stoploss_price

    def set_detectors(self, detectors: List[SignalDetector]):
        self.detectors = detectors

    def get_dynamic_stoploss_price(self, sm: SourceManager):
        """
        # 明确当前关键止损位，确保不亏钱；随着股价变动，调整止损价格、仓位
        # 不断提高调整止损点位（止盈）
        """

        bar = sm.latest_week_bar
        recent_low = sm.recent_week_low(11)
        last_pivot_low = sm.last_bottom_low_w  # 当上一周刚出现最低的pivot的时候，有可能还没有识别出底分型
        low = min(recent_low, last_pivot_low) if last_pivot_low is not None else recent_low

        # 已经从最低点涨上去了2*stop_loss_rate，止损位提高到最低位上涨以来38%的位置
        low_back_price = self.stoploss_price
        if self.is_price_high_enough(bar.high, low, 5):     # 价格超出low以上5个止损位
            # 当周线上涨比较多的时候，要保留日线上最近一次上涨以来 0.618 的收益
            low_back_price = self.accept_drawback_price(bar.high, low, 0.618)
        elif self.is_price_high_enough(bar.high, low, 2):     # 价格超出low以上2个止损位
            # 当周线上涨不多的时候，保留周线上最近一次上涨以来 0.382的收益
            low_back_price = self.accept_drawback_price(bar.high, low, 0.382)

        # 已经从入场位置涨上去了2*stop_loss_rate，止损位提高到61%的涨幅位置
        enter_back_price = self.stoploss_price
        if self.is_price_high_enough(bar.high, self.entry_buy_price, 3):     # 价格超出入场价以上2个止损位
            enter_back_price = self.accept_drawback_price(bar.high, self.entry_buy_price, 0.618)

        # 两个同时满足，不要太快提高止损位
        stoploss_price = min(low_back_price, enter_back_price)
        # 取中间位置止损
        # stoploss_price = (low_back_price + enter_back_price) / 2

        # 最近1个月超速上涨，一旦回落，马上落袋
        last_month_high = sm.recent_week_high(4)
        last_month_low = sm.recent_week_low(4)
        if self.is_price_high_enough(last_month_high, last_month_low, 5):
            stoploss_price = self.accept_drawback_price(last_month_high, last_month_low, 0.618)

        return stoploss_price

