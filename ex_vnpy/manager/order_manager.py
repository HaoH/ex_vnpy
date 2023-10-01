import logging
from datetime import datetime, timedelta
from typing import Any

from vnpy.trader.object import OrderData
from vnpy.trader.constant import Direction, Offset, OrderType, Status
from vnpy.trader.utility import round_to
from vnpy_ctastrategy import StopOrder, CtaTemplate
from vnpy_ctastrategy.base import STOPORDER_PREFIX, StopOrderStatus

logger = logging.getLogger("OrderManager")


class OrderManager(object):

    def __init__(self, strategy: CtaTemplate, engine: Any):
        """Constructor"""
        self.orders = {}
        self.strategy = strategy

        from vnpy_ctastrategy.backtesting import BacktestingEngine
        if isinstance(engine, BacktestingEngine):
            self.stop_orders = engine.stop_orders
            self.active_stop_orders = engine.active_stop_orders
            self.limit_orders = engine.limit_orders
            self.active_limit_orders = engine.active_limit_orders
            self.trades = engine.trades
            self.engine = engine
            self.stop_order_count = engine.stop_order_count
            self.limit_order_count = engine.limit_order_count
            self.vt_symbol = engine.vt_symbol
            self.symbol = engine.symbol
            self.exchange = engine.exchange
            self.gateway_name = engine.gateway_name
            self.datetime = engine.datetime
            self.price_tick = engine.price_tick

    def active_stop_order(self, stop_order_id) -> StopOrder:
        if stop_order_id in self.active_stop_orders.keys():
            return self.active_stop_orders[stop_order_id]
        return None

    def get_limit_order(self, vt_order_id) -> OrderData:
        if vt_order_id in self.limit_orders.keys():
            return self.limit_orders[vt_order_id]
        return None
    def active_limit_order(self, vt_order_id) -> OrderData:
        if vt_order_id in self.active_limit_orders.keys():
            return self.active_limit_orders[vt_order_id]
        return None

    def stop_order(self, stop_order_id) -> StopOrder:
        if stop_order_id in self.stop_orders.keys():
            return self.stop_orders[stop_order_id]
        return None

    def cancel_stop_order(self, stop_order: StopOrder, is_trading: bool = True):
        if not is_trading or len(self.active_stop_orders) <= 0:
            return

        stop_order_ids = list(self.active_stop_orders.keys())
        for stop_order_id in stop_order_ids:
            if stop_order_id == stop_order.stop_orderid:
                order = self.active_stop_orders[stop_order_id]
                if order.order_type == stop_order.order_type and order.direction == stop_order.direction:
                    self.active_stop_orders.pop(stop_order_id)
                    order.status = StopOrderStatus.CANCELLED
                    order.trigger_datetime = self.datetime
                break

    def cancel_stop_orders_except(self, is_trading: bool, stop_order: StopOrder):
        """
        取消同类型的stop_order
        :param is_trading:
        :param stop_order:
        :return:
        """
        if not is_trading or len(self.active_stop_orders) <= 0:
            return

        stop_order_ids = list(self.active_stop_orders.keys())
        for stop_order_id in stop_order_ids:
            if stop_order_id != stop_order.stop_orderid:
                order = self.active_stop_orders[stop_order_id]
                if order.order_type == stop_order.order_type and order.direction == stop_order.direction:
                    self.active_stop_orders.pop(stop_order_id)
                    order.status = StopOrderStatus.CANCELLED
                    order.trigger_datetime = self.datetime

    def cancel_limit_order(self, order: OrderData, is_trading: bool = True):
        if not is_trading or len(self.active_limit_orders) <= 0:
            return

        vt_orderids = list(self.active_limit_orders.keys())
        for vt_orderid in vt_orderids:
            if vt_orderid == order.vt_orderid:
                order: OrderData = self.active_limit_orders.pop(vt_orderid)
                order.status = Status.CANCELLED
                self.strategy.on_order(order)
                break

    def cancel_limit_orders(self, is_trading: bool):
        if not is_trading or len(self.active_limit_orders) <= 0:
            return

        vt_orderids = list(self.active_limit_orders.keys())
        for vt_orderid in vt_orderids:
            if vt_orderid in list(self.active_limit_orders.keys()):
                order: OrderData = self.active_limit_orders.pop(vt_orderid)
                order.status = Status.CANCELLED
                self.strategy.on_order(order)

    def send_stop_order(self,
                        order_type: OrderType,
                        direction: Direction,
                        offset: Offset,
                        volume: float,
                        price: float = None,
                        trigger_price: float = None) -> str:
        """

        :param is_protected:
        :param trigger_price:
        :param order_type:
        :param direction:
        :param offset:
        :param price:
        :param volume:
        :return:
        """

        price: float = round_to(price, self.price_tick)
        self.stop_order_count += 1

        stop_order: StopOrder = StopOrder(
            vt_symbol=self.vt_symbol,
            order_type=order_type,
            direction=direction,
            offset=offset,
            trigger_price=trigger_price,
            price=price,
            volume=volume,
            datetime=self.datetime,
            stop_orderid=f"{STOPORDER_PREFIX}.{self.stop_order_count}",
            strategy_name=self.strategy.strategy_name
        )

        self.active_stop_orders[stop_order.stop_orderid] = stop_order
        self.stop_orders[stop_order.stop_orderid] = stop_order

        return stop_order.stop_orderid

    def send_limit_order(self, order_type: OrderType, direction: Direction, offset: Offset, price: float, volume: float, trigger_stop_orderid: str = "") -> str:
        price: float = round_to(price, self.price_tick)
        self.limit_order_count += 1

        order = OrderData(
            symbol=self.symbol,
            exchange=self.exchange,
            type=order_type,
            orderid=str(self.limit_order_count),
            direction=direction,
            offset=offset,
            price=price,
            volume=volume,
            status=Status.SUBMITTING,
            gateway_name=self.gateway_name,
            datetime=self.datetime,
            trigger_stop_orderid=trigger_stop_orderid
        )

        self.active_limit_orders[order.vt_orderid] = order
        self.limit_orders[order.vt_orderid] = order

        return order.vt_orderid

    def is_signal_duplicated(self, today: datetime, trigger_price: float):
        """
        连续出现相同信号，但是价格却在不断抬高，应该过滤掉
        :param today:
        :param trigger_price:
        :return:
        """
        if self.stop_order_count <= 0:
            return

        stop_order_id = f"{STOPORDER_PREFIX}.{self.stop_order_count}"
        last_stop_order = self.stop_orders[stop_order_id]
        if last_stop_order.datetime + timedelta(days=14) >= today and last_stop_order.trigger_price <= trigger_price:
            logger.info("[OM][Signal_Duplicated] date: {}, last_trigger_price: {:.2f}, new_trigger_price: {:.2f} (dropped)".format(today.strftime("%Y-%m-%d"), last_stop_order.trigger_price, trigger_price))
            return True
        return False

    def is_limit_signal_duplicated(self, today: datetime, buy_price: float):
        """
        连续出现相同信号，但是价格却在不断抬高，应该过滤掉
        :param today:
        :param buy_price:
        :return:
        """
        if self.limit_order_count<= 0:
            return

        vt_orderid: str = f"{self.gateway_name}.{self.limit_order_count}"
        last_order = self.limit_orders[vt_orderid]
        if last_order.direction == Direction.LONG and last_order.datetime + timedelta(days=14) >= today and last_order.price >= buy_price:
            logger.info("[OM][Signal_Duplicated] date: {}, last_market_price: {:.2f}, new_market_price: {:.2f} (dropped)".format(today.strftime("%Y-%m-%d"), last_order.price, buy_price))
            return True
        return False

    def is_signal_duplicated_uni(self, today: datetime, last_day_high: float, trigger_price: float, buy_price: float):
        last_order = None
        if trigger_price <= last_day_high and self.limit_order_count > 0:  # 限价单/市价单
            vt_orderid: str = f"{self.gateway_name}.{self.limit_order_count}"
            last_order = self.limit_orders[vt_orderid]
        elif trigger_price >= last_day_high and self.stop_order_count > 0:   # 触发单
            stop_order_id: str = f"{STOPORDER_PREFIX}.{self.stop_order_count}"
            last_order = self.stop_orders[stop_order_id]

        if last_order and last_order.direction == Direction.LONG and last_order.datetime + timedelta(days=14) >= today:
            if isinstance(last_order, StopOrder) and last_order.trigger_price <= trigger_price:
                logger.info("[OM][Signal_Duplicated] date: {}, last_trigger_price: {:.2f}, new_trigger_price: {:.2f} (dropped)".format(today.strftime("%Y-%m-%d"), last_order.trigger_price, trigger_price))
                return True
            elif isinstance(last_order, OrderData) and last_order.price >= buy_price:
                logger.info("[OM][Signal_Duplicated] date: {}, last_market_price: {:.2f}, new_market_price: {:.2f} (dropped)".format(today.strftime("%Y-%m-%d"), last_order.price, buy_price))
                return True
        return False
