import logging
from math import floor
from typing import List, Dict

from ex_vnpy.signal import Signal
from ex_vnpy.trade_plan import TradePlan
from ex_vnpy.manager.source_manager import SourceManager
from vnpy.trader.constant import Direction, Offset
from vnpy.trader.object import OrderData, TradeData
from vnpy_ctastrategy import StopOrder
import numpy as np

logger = logging.getLogger("PositionManager")

class PositionInfo(object):
    """
    当前的仓位信息
    """

    symbol: str = ''
    enter_price: float = 0
    stoploss_price: float = 0

    enter_order_id: OrderData
    stoploss_order: StopOrder


class PositionManager(object):

    unit_size: int = 100         # 每一手的仓位
    commission_rate: float = 0.0002
    slippage: float = 0.005
    stoploss_rate: float = 0.08  # 最大止损比例
    price_tick: float = 0.01
    fix_capital: float = 100000
    current_capital: float = 100000

    total_volume: float = 0
    cost_price: float = 0       # 成本价
    last_price: float = 0       # 最后入场价
    stoploss_ind: Dict = None   # 止损指标
    stoploss_settings: Dict = None   # 止损指标

    def __init__(self, stoploss_rate=0.08, fix_capital=100000, price_tick: float = 0.01, unit_size: int = 100, commission_rate: float = 0.0002, slippage: float = 0.005, stoploss_ind: Dict = None):
        self.stoploss_rate = stoploss_rate
        self.update_settings(fix_capital, price_tick, unit_size, commission_rate, slippage)

        self.positions = []
        self.stoploss_ind = stoploss_ind

    def update_settings(self, fix_capital=100000, price_tick: float = 0.01, unit_size: int = 100, commission_rate: float = 0.0002, slippage: float = 0.005):
        self.fix_capital = fix_capital
        self.price_tick = price_tick
        self.unit_size = unit_size
        self.commission_rate = commission_rate
        self.slippage = slippage

        self.current_capital = fix_capital
        self.total_volume = 0

    def update_stoploss_settings(self, sl_settings: dict):
        self.stoploss_settings = sl_settings

    def is_capital_enough(self, direction: Direction, price, volume):
        is_enough = True
        if direction == Direction.LONG:
            is_enough = self.current_capital >= price * volume * self.unit_size
        return is_enough

    def has_capital(self, price) -> bool:
        """
        现金至少要能够购买1手
        :param price:
        :return:
        """
        return self.current_capital > price * self.unit_size * 1.2

    def is_price_high_enough(self, high_price, base_price, factor) -> float:
        return high_price >= base_price * (1 + factor * self.stoploss_rate)

    def update_position(self, trade: TradeData):
        """
        当有新的订单成交，需要更新当前仓位、持仓成本
        如果是新的开仓，需要设置止损订单
        :param trade:
        :return:
        """
        # 计算滑点、手续费
        slippage = trade.volume * self.unit_size * self.slippage
        turnover: float = trade.volume * self.unit_size * trade.price
        commission = turnover * self.commission_rate

        if trade.offset == Offset.OPEN and trade.direction == Direction.LONG:
            if trade.volume > 0:
                new_cost_price = (self.total_volume * self.cost_price + trade.price * trade.volume) / (
                        self.total_volume + trade.volume)
                self.total_volume += trade.volume
                self.cost_price = new_cost_price
                self.last_price = trade.price
                self.current_capital -= trade.price * trade.volume * self.unit_size
                self.current_capital -= slippage + commission
                logger.info(f"[PM][OpenPosition] date: {trade.datetime.strftime('%Y-%m-%d')}, total_volume: {self.total_volume:.2f}, current_capital: {self.current_capital:.2f}, total_value: {self.current_capital + self.total_volume * trade.price * self.unit_size:.2f}, slippage: {slippage:.2f}, commission: {commission:.2f}")
        elif trade.offset == Offset.CLOSE and trade.direction == Direction.SHORT:
            if self.total_volume - trade.volume <= 0:
                self.total_volume = 0
                self.cost_price = 0
                self.last_price = 0
                self.current_capital += trade.price * trade.volume * self.unit_size
                self.current_capital -= slippage + commission
            else:
                new_cost_price = (self.total_volume * self.cost_price - trade.price * trade.volume) / (
                        self.total_volume - trade.volume)
                self.total_volume -= trade.volume
                self.cost_price = new_cost_price
                self.current_capital += trade.price * trade.volume * self.unit_size
                self.current_capital -= slippage + commission
            logger.info(f"[PM][ClosePosition] date: {trade.datetime.strftime('%Y-%m-%d')}, total_volume: {self.total_volume:.2f}, current_capital: {self.current_capital:.2f}, slippage: {slippage:.2f}, commission: {commission:.2f}")

    @property
    def do_stop_loss(self):
        return self.stoploss_rate > 0

    def make_trade_plan(self, sm: SourceManager, signals: List[Signal], total_signal_strength: float, full_strength: float, stoploss_rate: float):
        # 触发价格、入场价格取信号的均值，确保综合了两种信号的影响
        # TODO: 都取均值，效果比较均衡； trigger取low，buy取max，效果就会比较极端，或者是两者之长，或者是两者之短
        trigger_prices = [s.trigger_price for s in signals]
        trigger_price = np.mean(trigger_prices)

        buy_prices = [s.buy_price for s in signals]
        buy_price = np.mean(buy_prices)

        # 如果触发价格 低于 买入价格，需要将买入价格调低到触发价
        if trigger_price < buy_price:
            buy_price = trigger_price

        # 如果current_capital处于赚钱的位置，仓位也需要适当增加
        plan_capital = max(self.fix_capital, self.current_capital) * (total_signal_strength / full_strength)
        available_capital = min(self.current_capital, plan_capital)
        max_volume = floor(available_capital * 0.999 / (self.unit_size * buy_price))

        detectors = [s.detector for s in signals]
        tp = TradePlan(trigger_price, buy_price, max_volume, sm.today, total_signal_strength, detectors=detectors, stoploss_rate=stoploss_rate, stoploss_settings=self.stoploss_settings, price_tick=self.price_tick)
        sl_price = tp.init_stoploss_price(sm, signals)

        logger.info(f"[PM][NewTP] date: {sm.last_date.strftime('%Y-%m-%d')}, trigger_price: {trigger_price:.2f}, buy_price: {buy_price:.2f}, stoploss_price: {sl_price:.2f}, volume: {max_volume}, plan_capital: {available_capital:.2f}, strength: {total_signal_strength}")

        return tp
