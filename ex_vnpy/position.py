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

    def __init__(self, stop_loss_rate=0.08, volume: float = 0, price: float = 0, direction: Direction = Direction.LONG):
        self.stop_loss_rate = stop_loss_rate
        self.volume = volume
        self.cost_price = price
        self.last_price = price
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
        low = sm.recent_week_low(12)

        # 首次建仓，如果前低位置比固定止损比例8%要低，只要幅度在8%的50%以内，可以增大止损
        if first and self.last_price != 0:
            last_low_price = sm.recent_week_low(12)     # 最好取最近的pivot低点
            if (self.stop_loss_price - last_low_price) / (self.last_price * self.stop_loss_rate) <= 0.5:
                print("[SL_Price_Adjust] date: {}, from {:.2f} to {:.2f}".format(sm.last_date.strftime("%Y-%m-%d"), self.stop_loss_price, last_low_price))
                self.stop_loss_price = last_low_price


        # 已经从最低点涨上去了2*stop_loss_rate，止损位提高到最低位上涨以来38%的位置
        low_back_price = self.stop_loss_price
        if bar.high >= low * (1 + 2 * self.stop_loss_rate):
            low_back_price = low + (bar.high - low) * 0.382

        # 已经从入场位置涨上去了2*stop_loss_rate，止损位提高到61%的涨幅位置
        enter_back_price = self.stop_loss_price
        if bar.high >= self.last_price * (1 + 2 * self.stop_loss_rate):
            enter_back_price = self.last_price + (bar.high - self.last_price) * 0.618

        stop_loss_price = min(low_back_price, enter_back_price)  # 两个同时满足，不要太快提高止损位
        # stop_loss_price = (low_back_price + enter_back_price) / 2  # 两个同时满足，不要太快提高止损位

        # 更新止损价
        if stop_loss_price > self.stop_loss_price:
            print("[SL_Price_Update] date: {}, from {:.2f} to {:.2f}".format(sm.last_date.strftime("%Y-%m-%d"), self.stop_loss_price, stop_loss_price))
            self.stop_loss_price = stop_loss_price

        return self.stop_loss_price
