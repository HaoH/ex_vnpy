from ex_vnpy.source_manager import SourceManager
from vnpy.trader.object import TradeData
from vnpy.trader.constant import Direction, Offset


class Position:
    direction: Direction = Direction.LONG
    volume: float = 0.0  # 仓位
    cost_price: float = 0.0  # 成本价
    last_price: float = 0.0  # 最后一次入场的价格
    stop_loss_price: float = 0.0  # 止损价格
    stop_loss_rate: float = 0.08  # 最大止损比例
    price_tick: float = 0.01

    def __init__(self, stop_loss_rate=0.08, volume: float = 0, price: float = 0, price_tick: float = 0.01, direction: Direction = Direction.LONG):
        self.stop_loss_rate = stop_loss_rate
        self.volume = volume
        self.cost_price = price
        self.last_price = price
        self.price_tick = price_tick
        self.direction = direction
        self.stop_loss_price = price * (1 - stop_loss_rate)

    @property
    def is_active(self) -> bool:
        return self.volume > 0

    def update_position(self, trade: TradeData):
        """
        当有新的订单成交，需要更新当前仓位、持仓成本
        如果是新的开仓，需要设置止损订单
        :param trade:
        :return:
        """
        if trade.offset == Offset.OPEN and trade.direction == Direction.LONG:
            if trade.volume > 0:
                new_cost_price = (self.volume * self.cost_price + trade.price * trade.volume) / (
                        self.volume + trade.volume)
                self.volume += trade.volume
                self.cost_price = new_cost_price
                self.last_price = trade.price
                self.stop_loss_price = trade.price * (1 - self.stop_loss_rate)
        elif trade.offset == Offset.CLOSE and trade.direction == Direction.SHORT:
            if self.volume - trade.volume <= 0:
                self.volume = 0
                self.cost_price = 0
                self.last_price = 0
            else:
                if trade.volume > 0:
                    new_cost_price = (self.volume * self.cost_price - trade.price * trade.volume) / (
                            self.volume - trade.volume)
                    self.volume -= trade.volume
                    self.cost_price = new_cost_price

    def update_stop_loss_price(self, sm: SourceManager, first: bool = False):
        """
        # 明确当前关键止损位，确保不亏钱；随着股价变动，调整止损价格、仓位
        # 不断提高调整止损点位（止盈）
        # TODO: 除了考虑黄金分割比例, 考虑均线系统
        :param first: 入场时设计止损位需要综合考虑，不仅仅按照百分比
        :param sm:
        :return:
        """
        if self.volume == 0:
            return

        bar = sm.latest_week_bar
        recent_low = sm.recent_week_low(3)
        last_pivot_low = sm.last_bottom_low_w  # 当上一周刚出现最低的pivot的时候，有可能还没有识别出底分型
        low = min(recent_low, last_pivot_low)

        # 首次建仓，如果前低位置比固定止损比例8%要低，只要幅度在8%的50%以内，可以增大止损
        if first and self.last_price != 0:
            if (self.stop_loss_price - low) / (self.last_price * self.stop_loss_rate) <= 0.5:
                price_before_adjust = self.stop_loss_price
                self.stop_loss_price = low - self.price_tick
                print("[SL_Price_Adjust] date: {}, from {:.2f} to {:.2f}".format(sm.last_date.strftime("%Y-%m-%d"), price_before_adjust, self.stop_loss_price))

        else:

            # 已经从最低点涨上去了2*stop_loss_rate，止损位提高到最低位上涨以来38%的位置
            low_back_price = self.stop_loss_price
            if self.is_price_high_enough(bar.high, low, 5):     # 价格超出low以上5个止损位
                # 当周线上涨比较多的时候，要保留日线上最近一次上涨以来 0.618 的收益
                low_back_price = self.accept_drawback_price(bar.high, low, 0.618)
            elif self.is_price_high_enough(bar.high, low, 2):     # 价格超出low以上2个止损位
                # 当周线上涨不多的时候，保留周线上最近一次上涨以来 0.382的收益
                low_back_price = self.accept_drawback_price(bar.high, low, 0.382)

            # 已经从入场位置涨上去了2*stop_loss_rate，止损位提高到61%的涨幅位置
            enter_back_price = self.stop_loss_price
            if self.is_price_high_enough(bar.high, self.last_price, 3):     # 价格超出入场价以上2个止损位
                enter_back_price = self.accept_drawback_price(bar.high, self.last_price, 0.618)

            stop_loss_price = min(low_back_price, enter_back_price)  # 两个同时满足，不要太快提高止损位
            # stop_loss_price = (low_back_price + enter_back_price) / 2  # 两个同时满足，不要太快提高止损位

            # 最近1个月超速上涨，一旦回落，马上落袋
            last_month_high = sm.recent_week_high(4)
            last_month_low = sm.recent_week_low(4)
            if self.is_price_high_enough(last_month_high, last_month_low, 5):
                stop_loss_price = self.accept_drawback_price(last_month_high, last_month_low, 0.618)

            # 更新止损价
            if stop_loss_price > self.stop_loss_price:
                print("[SL_Price_Update] date: {}, from {:.2f} to {:.2f}".format(sm.last_date.strftime("%Y-%m-%d"), self.stop_loss_price, stop_loss_price))
                self.stop_loss_price = stop_loss_price

        return self.stop_loss_price

    def is_price_high_enough(self, high_price, base_price, factor) -> float:
        return high_price >= base_price * (1 + factor * self.stop_loss_rate)

    def accept_drawback_price(self, high_price, base_price, factor) -> float:
        return base_price + (high_price - base_price) * factor
