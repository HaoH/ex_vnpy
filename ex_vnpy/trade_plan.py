import logging
from dataclasses import dataclass, fields
from datetime import datetime
from enum import Enum
from math import floor
from typing import List, Dict, Tuple

from pandas import Series

from ex_vnpy.manager.source_manager import SourceManager
from ex_vnpy.signal import SignalDetector, Signal
from vnpy.trader.constant import Direction
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

class StoplossReason(Enum):
    """
    止损价格更新原因
    """
    Empty = -1
    Init = 0
    Detector = 1
    Dynamic = 2
    LowTwo = 3
    LowFive = 4
    Enter_Two = 5
    Impulse = 6
    Ema = 7
    LargeUp = 8
    LargeVolume = 9
    Engine = 10         # 表示Engine执行的时候调整，一般可能是市价止损出现了跳水现象
    LostSpeed = 11
    LevelLargeUp = 12   # 入场X天之后的大幅上涨
    LargeRange = 13     # 上影线较长


@dataclass
class StoplossRecord:
    stoploss_price: float = 0
    change_date: datetime = None
    change_reason: StoplossReason = StoplossReason.Init


@dataclass
class TradePlanData:
    """
    用于记录每一笔交易的信息
    """
    # symbol: str
    # exchange: Exchange

    entry_trigger_order_id: str = ""        # 入场触发的StopOrder订单id
    entry_order_id: str = ""                # 入场的LimitOrder订单id
    exit_trigger_order_id: str = ""         # 退场的StopOrder订单id
    exit_order_id: str = ""                 # 退场的LimitOrder订单id

    direction: Direction = Direction.LONG
    status: PlanStatus = PlanStatus.PLAN
    plan_date: datetime = None
    entry_trigger_date: datetime = None           # 入场触发日期
    entry_date: datetime = None                   # 入场日期
    stoploss_trigger_date: datetime = None        # 止损触发日期
    stoploss_date: datetime = None                # 止损日期

    entry_trigger_price: float = 0
    entry_buy_price: float = 0
    stoploss_price: float = 0
    volume: float = 0
    strength: float = 0
    stoploss_rate: float = 0

    stoploss_records: List = None


class TradePlan:
    """
    每一次入场都是提前计划好的，包括入场价格、止损价格、止盈价格（这些信息包含在信号中）。
    入场成功之后，相关的仓位信息保存在本结构中，包括入场订单、止损订单。一个股票可以同时多次入场（由不同的信号驱动）
    """

    direction: Direction = Direction.LONG
    status: PlanStatus = PlanStatus.PLAN
    plan_date: datetime = None
    entry_trigger_date: datetime = None           # 入场订单触发日期
    entry_date: datetime = None                   # 入场日期
    stoploss_trigger_date: datetime = None        # 止损订单触发日期
    stoploss_date: datetime = None                # 止损日期
    stoploss_price_date: datetime = None          # 止损价格更新的日期，用来控制止损价格，只允许同一周内下降，跨周不允许下降
    entry_trigger_price: float = 0
    entry_buy_price: float = 0
    stoploss_price: float = 0
    volume: float = 0
    strength: float = 0

    detectors: List[SignalDetector] = []
    stoploss_rate: float = 0.08
    stoploss_ind: Dict = None

    entry_trigger_order_id: str = ""        # 入场触发的StopOrder订单id
    entry_order_id: str = ""                # 入场的LimitOrder订单id
    exit_trigger_order_id: str = ""         # 退场的StopOrder订单id
    exit_order_id: str = ""                 # 退场的LimitOrder订单id
    stoploss_order: StopOrder = None

    # 新增一种类型，止损数据，把止损价格变动原因也放进来
    stoploss_records: List[StoplossRecord] = []

    def __init__(self, entry_trigger_price, entry_buy_price, volume, plan_date, strength, **kwargs):
        """
        stoploss_price，可以通过kwargs参数传递
        """
        self.entry_trigger_price = entry_trigger_price
        self.entry_buy_price = entry_buy_price

        # self.stoploss_price = stoploss_price
        self.stoploss_price_date = plan_date
        self.stoploss_order: StopOrder = None

        self.volume = volume
        self.plan_date = plan_date
        self.strength = strength

        self.detectors = []
        self.stoploss_records: List[StoplossRecord] = []

        for key, value in kwargs.items():
            if hasattr(self, key):
                if key == 'stoploss_records':
                    for record in value:
                        self.stoploss_records.append(StoplossRecord(**record))
                else:
                    setattr(self, key, value)

    def set_entry_trigger_order(self, order_ids: List[str]):
        if len(order_ids) <= 0:
            return

        self.entry_trigger_order_id = order_ids[0]
        self.status = PlanStatus.WAITING
        logger.debug(f"[TP][SetOrder][EntryTrigger] trigger_order_id: {self.entry_trigger_order_id}, plan_date: {self.plan_date.strftime('%Y-%m-%d')}, status: {self.status}, entry_trigger_price: {self.entry_trigger_price:.2f}")

    def set_entry_order(self, vt_orderids):
        if len(vt_orderids) <= 0:
            return

        self.entry_order_id = vt_orderids[0]
        self.status = PlanStatus.OPEN
        logger.debug(f"[TP][SetOrder][Entry] limit_order_id: {self.entry_order_id}, plan_date: {self.plan_date.strftime('%Y-%m-%d')}, status: {self.status}, entry_price: {self.entry_buy_price:.2f}")

    def set_exit_trigger_order(self, vt_orderid: str, sl_order: StopOrder):
        self.exit_trigger_order_id = vt_orderid
        self.stoploss_order = sl_order
        logger.debug(f"[TP][SetOrder][ExitTrigger] trigger_order_id: {self.exit_trigger_order_id}, plan_date: {self.plan_date.strftime('%Y-%m-%d')}, status: {self.status}, stoploss_price: {self.stoploss_price:.2f}")

        if self.stoploss_price > 0:
            self.stoploss_records.append(StoplossRecord(self.stoploss_price, self.entry_date, StoplossReason.Init))

    def set_exit_order(self, vt_orderids):
        if len(vt_orderids) <= 0:
            return

        self.exit_order_id = vt_orderids[0]
        self.status = PlanStatus.EXIT
        logger.debug(f"[TP][SetOrder][Exit] limit_order_id: {self.exit_order_id}, plan_date: {self.plan_date.strftime('%Y-%m-%d')}, status: {self.status}, stoploss_price: {self.stoploss_price:.2f}")

    def set_stoploss_price(self, new_stoploss_price: float):
        self.stoploss_price = new_stoploss_price

    def set_detectors(self, detectors: List[SignalDetector]):
        self.detectors = detectors

    @property
    def do_stop_loss(self):
        return self.stoploss_rate > 0

    @property
    def is_stoploss_order_active(self):
        return self.exit_trigger_order_id != "" and self.stoploss_order is not None and self.stoploss_order.status == StopOrderStatus.WAITING

    def is_price_high_enough(self, high_price, base_price, factor) -> float:
        return high_price >= base_price * (1 + factor * self.stoploss_rate)

    def accept_drawback_price(self, high_price, base_price, factor) -> float:
        return base_price + (high_price - base_price) * factor

    def init_stoploss_price(self, sm: SourceManager, signals: List[Signal]) -> float:
        sl_prices = [s.sl_price for s in signals]
        valid_sl_prices = [x for x in sl_prices if x is not None]

        # 如果detector有止损设置，则采用该设置；如果没有，则采用通用策略
        stoploss_price = 0
        if len(valid_sl_prices) > 0:
            stoploss_price = min(valid_sl_prices)

            logger.debug(f"[PM][SLPriceInit] date: {sm.last_date.strftime('%Y-%m-%d')}, min_stoploss_prices: {stoploss_price:.2f}, valid_sl_prices: {len(valid_sl_prices)}")

        elif self.do_stop_loss:
            stoploss_price = self.entry_buy_price * (1 - self.stoploss_rate)

            # 首次建仓，如果前低位置比固定止损比例8%要低，只要幅度在8%的50%以内，可以增大止损
            recent_low = sm.recent_week_low(11)
            last_pivot_low = sm.last_bottom_low_w  # 当上一周刚出现最低的pivot的时候，有可能还没有识别出底分型
            low = min(recent_low, last_pivot_low) if last_pivot_low is not None else recent_low

            if 0 < (stoploss_price - low) / stoploss_price <= 0.5:
                stoploss_price = low
                logger.debug(f"[PM][SLPriceAdjust] date: {sm.last_date.strftime('%Y-%m-%d')}, stoploss_price: {stoploss_price:.2f}")

        self.stoploss_price = stoploss_price
        return stoploss_price

    def update_stoploss_price(self, sm: SourceManager):
        sl_prices = [detector.stoploss_price(sm, self) for detector in self.detectors]
        valid_sl_prices = [x for x in sl_prices if x is not None]

        # 如果detector有止损设置，则采用该设置；如果没有，则采用通用策略
        reason = StoplossReason.Empty
        new_sl_price = 0

        if len(valid_sl_prices) > 0:
            # 取所有策略的最低止损价格
            new_sl_price = min(valid_sl_prices)
            reason = StoplossReason.Detector
            logger.debug(f"[TP][NewSL][{reason.name}] date: {sm.last_date.strftime('%Y-%m-%d')}, stoploss: {self.stoploss_price:.2f} -> {new_sl_price:.2f}, change_reason: {reason.name}")
        elif self.stoploss_rate > 0:
            # 根据价格走势，动态调整止损位
            dynamic_sl_price, dynamic_reason = self.get_dynamic_stoploss_price(sm)
            new_sl_price = dynamic_sl_price
            reason = dynamic_reason
            logger.debug(f"[TP][NewSL][{dynamic_reason.name}] date: {sm.last_date.strftime('%Y-%m-%d')}, stoploss: {self.stoploss_price:.2f} -> {new_sl_price:.2f}, change_reason: {reason.name}")

        # 计算止损策略的止损价
        ind_change_price, ind_reason = self.get_all_stoploss_prices(sm)
        if ind_change_price > new_sl_price:
            logger.debug(f"[TP][NewSL][Ind][{ind_reason.name}] date: {sm.last_date.strftime('%Y-%m-%d')}, stoploss: {self.stoploss_price:.2f} -> {ind_change_price:.2f}, change_reason: {ind_reason.name}")
            new_sl_price = ind_change_price
            reason = ind_reason

        # 止损价格只能上升，不能下降
        _, lw, _ = self.stoploss_price_date.isocalendar()
        _, tw, _ = sm.today.isocalendar()
        # 对于当周的数据，由于周线未定型，允许向下调整止损价；对于非当周数据，只允许向上调整
        if (lw == tw and self.entry_buy_price > self.stoploss_price) or new_sl_price > self.stoploss_price:
            logger.debug(f"[TP][SLPriceUpdate] date: {sm.last_date.strftime('%Y-%m-%d')},   stoploss_price: {self.stoploss_price:.2f} -> {new_sl_price:.2f}")
            if self.stoploss_price != new_sl_price:
                self.stoploss_records.append(StoplossRecord(new_sl_price, sm.today, reason))

            self.stoploss_price = new_sl_price
            self.stoploss_price_date = sm.today

    def adjust_stoploss_price(self, stoploss_price: float, stoploss_price_date: datetime, reason: StoplossReason):
        """
        用于手动调整止损价格，比如说，engine在成交时遇到跳水情况，实际止损价格跟预计不一样
        """
        self.stoploss_price = stoploss_price
        self.stoploss_price_date = stoploss_price_date
        self.stoploss_records.append(StoplossRecord(stoploss_price, stoploss_price_date, reason))

    def get_dynamic_stoploss_price(self, sm: SourceManager) -> Tuple[float, StoplossReason]:
        """
        # 明确当前关键止损位，确保不亏钱；随着股价变动，调整止损价格、仓位
        # 不断提高调整止损点位（止盈）
        """
        bar = sm.latest_week_bar
        recent_low = sm.recent_week_low(11, last_contained=False)
        last_pivot_low = sm.last_bottom_low_w  # 当上一周刚出现最低的pivot的时候，有可能还没有识别出底分型
        low = min(recent_low, last_pivot_low) if last_pivot_low is not None else recent_low
        reason = StoplossReason.Dynamic

        # 已经从最低点涨上去了2*stop_loss_rate，止损位提高到最低位上涨以来38%的位置
        low_back_price = self.stoploss_price
        if self.is_price_high_enough(bar.high, low, 5):     # 价格超出low以上5个止损位
            # 当周线上涨比较多的时候，要保留日线上最近一次上涨以来 0.618 的收益
            low_back_price = self.accept_drawback_price(bar.high, low, 0.618)
            reason = StoplossReason.LowFive
        elif self.is_price_high_enough(bar.high, low, 2):     # 价格超出low以上2个止损位
            # 当周线上涨不多的时候，保留周线上最近一次上涨以来 0.382的收益
            low_back_price = self.accept_drawback_price(bar.high, low, 0.382)
            reason = StoplossReason.LowTwo

        # 兜底用的止损价
        stoploss_price = low_back_price

        # 最近1个月超速上涨，一旦回落，马上落袋; 在大牛股趋势上，容易造成过早离场
        # last_month_high = sm.recent_week_high(4)
        # last_month_low = sm.recent_week_low(4)
        # if self.is_price_high_enough(last_month_high, last_month_low, 5):
        #     stoploss_price = self.accept_drawback_price(last_month_high, last_month_low, 0.618)
        return stoploss_price, reason

    def get_all_stoploss_prices(self, sm: SourceManager) -> Tuple[float, StoplossReason]:
        # 根据指标的变化，调整止损位
        ind_change_price = 0
        ind_reason = StoplossReason.Impulse
        if self.stoploss_ind:
            if "enabled" in self.stoploss_ind and self.stoploss_ind['enabled']:
                # 兼容老的数据格式（仅支持一个stoploss_ind）
                ind_change_price, ind_reason = self.get_stoploss_price_by_strategy(sm, self.stoploss_ind["type"], self.stoploss_ind)
            else:
                for ind_type, ind_setting in self.stoploss_ind.items():
                    if ind_setting["enabled"]:
                        a_ind_change_price, a_ind_reason = self.get_stoploss_price_by_strategy(sm, ind_type, ind_setting)
                        if a_ind_change_price > ind_change_price:
                            ind_change_price = a_ind_change_price
                            ind_reason = a_ind_reason

        return ind_change_price, ind_reason

    def get_stoploss_price_by_strategy(self, sm: SourceManager, stoploss_type: str, ind_setting: dict):
        """
        根据不同的止损策略计算止损价
        """
        def has_large_drop(today_bar: Series, yesterday_bar: Series, atr: list, factor: int) -> bool:
            """
            出现大幅度drop：
            1）实体柱在上升，且上影线长度超过 前一日的atr * factor
            2）昨日出现大幅drop，当日股价还在攀升
            3）出现大阴柱，跌幅超过 前一日的 atr * factor
            :return:
            """
            today_max_oc = max(today_bar["open"], today_bar["close"])
            up_shadow_line = (today_bar["high"] - today_max_oc)

            yesterday_max_oc = max(yesterday_bar["open"], yesterday_bar["close"])
            last_up_shadow_line = (yesterday_bar["high"] - yesterday_max_oc)

            down_solid_body = (today_bar["open"] - today_bar["close"])
            if (today_max_oc >= yesterday_bar["close"] and up_shadow_line >= atr[-2] * factor) or \
                    (today_max_oc >= yesterday_max_oc and last_up_shadow_line >= atr[-3] * factor) or \
                    down_solid_body >= atr[-2] * factor:
                        return True
            return False

        a_ind_change_price = self.stoploss_price
        a_ind_reason = StoplossReason.Empty
        if stoploss_type == "impulse":
            # impulse 指标连续2周转红，第三周出场
            ind_values = sm.get_indicator_value(ind_setting["name"], ind_setting["signals"])
            if ind_values and len(ind_values) > 3 and sum(ind_values[-3:-1]) <= -2:
                a_ind_change_price = sm.recent_week_low(2, last_contained=False)
                a_ind_reason = StoplossReason.Impulse
        elif stoploss_type == "ema":
            # 价格低于ema10，则止损
            ind_values = sm.get_indicator_value(ind_setting["name"], ind_setting["signals"])
            if ind_values and len(ind_values) > 2:
                a_ind_change_price = ind_values[-2] * 0.98    # ema10以下2%位置
                a_ind_reason = StoplossReason.Ema
        elif stoploss_type == "ema_v":
            # 日线柱成交量3倍于最近3个月成交量加权平均，止损位放在该日线柱下方一个price_tick位置
            ind_values = sm.get_indicator_value(ind_setting["name"], ind_setting["signals"])
            if ind_values and len(ind_values) > 2:
                bar = sm.latest_daily_bar
                if bar["volume"] >= ind_values[-1] * ind_setting['factor']:
                    a_ind_change_price = bar["low"] - 0.01
                    a_ind_reason = StoplossReason.LargeVolume
        elif stoploss_type == "large_up":
            # 日线柱出现3%波动，止损位放在该日线柱下方一个price_tick位置
            # TODO: 可以考虑换成ATR的9分位
            # ind_values = sm.get_indicator_value(ind_setting["name"], ind_setting["signals"])

            bar = sm.latest_daily_bar
            hl_range = (bar["high"] - bar["low"]) / bar["close"]
            if sm.is_up and hl_range >= ind_setting["wave_percent"]:
                a_ind_change_price = bar["low"] - 0.01
                # TODO: 可以增加一些空间，避免频繁止损
                # a_ind_change_price = bar["low"] * 0.99 - 0.01
                a_ind_reason = StoplossReason.LargeUp
        elif stoploss_type == "entry_low_speed":
            def find_real_test_days(max_test_days: int):
                real_test_days = 0
                for ix in range(2, max_test_days+1):
                    if sm.daily_df.iloc[-1 * ix]["datetime"] == self.entry_date:
                        real_test_days = ix
                        break
                return real_test_days

            def is_speed_low(input: list, drop_days: int):
                before = input[0]
                valid_days = 0
                for i in range(1, len(input)):
                    current = input[i]
                    if current < before:
                        valid_days += 1
                    else:
                        valid_days = 0
                    if valid_days >= drop_days:
                        return True
                    before = current
                return False

            # TODO: 周线转变的时候入场，如果离日线突破时间超过1周，将豁免3日试炼
            # 入场的前N天，根据macd hist的变动进行止损
            ind_values = sm.get_indicator_value(ind_setting["name"], ind_setting["signals"])
            test_days, drop_days = ind_setting["test_days"]
            real_test_days = find_real_test_days(test_days)
            last_ind_values = ind_values[-1 * real_test_days:]
            if None not in last_ind_values and is_speed_low(last_ind_values, drop_days):
                a_ind_change_price = sm.daily_df.iloc[-1]["low"] * 0.99 - 0.01
                a_ind_reason = StoplossReason.LostSpeed
        elif stoploss_type == "mid_large_up":
            # 入场企稳之后，出现较大幅度的上涨
            # TODO: 考虑周线上涨幅度
            # test_days = self.stoploss_ind["entry_low_speed"]["test_days"][0]
            # if sm.daily_df.index[-1 * test_days] >= self.entry_date: # 已经经过了入场试炼
            bar = sm.latest_daily_bar   # 当日
            last_bar = sm.last_bar      # 昨日
            ind_values = sm.get_indicator_value(ind_setting["name"], ind_setting["signals"])

            # 上影线的策略优先级更高
            # TODO: 调整large up 和large range的标准, 上影线达到1倍ATR，则为largerange，而不是固定比例
            if not has_large_drop(bar, last_bar, ind_values, ind_setting["factor"]):
                close_up = bar["close"] - last_bar["close"]
                atr_y = ind_values[-2]
                if close_up > 0 and bar["close"] >= bar["open"]:
                    if close_up >= atr_y * 0.7:
                        a_ind_reason = StoplossReason.LevelLargeUp
                        a_ind_change_price = bar["low"] * 0.99 - 0.01
                    elif close_up >= atr_y * 1:
                        a_ind_change_price = min(bar["open"], bar["close"]) - 0.01
                        a_ind_reason = StoplossReason.LevelLargeUp
                    elif close_up >= atr_y * 1.3:
                        a_ind_change_price = min(bar["open"], bar["close"]) - 0.01
                        a_ind_reason = StoplossReason.LevelLargeUp

        elif stoploss_type == "mid_large_drop":
            # 出现明显的下调时候，止损价提高到当日实体柱的位置
            # large_range策略不区分入场还是非入场

            # test_days = self.stoploss_ind["entry_low_speed"]["test_days"][0]
            # if sm.daily_df.index[-1 * test_days] >= self.entry_date: # 已经经过了入场试炼

            bar = sm.latest_daily_bar   # 当日
            last_bar = sm.last_bar      # 昨日
            ind_values = sm.get_indicator_value(ind_setting["name"], ind_setting["signals"])
            if has_large_drop(bar, last_bar, ind_values, ind_setting["factor"]):
                # a_ind_change_price = bar["low"] * 0.98 - 0.01
                a_ind_change_price = min(bar["close"], bar["open"]) * 0.998 - 0.01
                a_ind_reason = StoplossReason.LargeRange

        return a_ind_change_price, a_ind_reason

    def extract_data(self):
        tpd_values = {}
        tpd_fields = [f.name for f in fields(TradePlanData)]
        for attr_name in tpd_fields:
            if attr_name == "stoploss_records":
                sr = []
                for record in self.stoploss_records:
                    sr.append(record.__dict__)
                tpd_values[attr_name] = sr
            else:
                tpd_values[attr_name] = getattr(self, attr_name)

        return TradePlanData(**tpd_values)

    @classmethod
    def init_from_trade_plan_data(cls, trade_plan_data: dict) -> 'TradePlan':
        return TradePlan(**trade_plan_data)

