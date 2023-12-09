from dataclasses import dataclass
from enum import Enum, EnumMeta
from typing import List, Any

import numpy as np
from talipp.indicators.Indicator import Indicator
from talipp.ohlcv import OHLCV
import talib as ta


@dataclass
class PatternVal:
    Doji: int = 0
    Doji2: int = 0
    Spinning: int = 0
    Candle: int = 0
    Revert: int = 0


class DojiPattern(Enum):
    Doji = 0                # 十字
    # DojiStar = 1            # 十字星
    # GravestoneDoji = 2      # 墓碑十字星丄
    # LongLeggedDoji = 3      # 长脚十字星，(OC相同居当日价格中部，上下影线长)
    # RickshawMan = 4         # 黄包车夫线（跟长腿十字星类似，价格正好在当日中点）
    # DragonflyDoji = 5       # 蜻蜓十字星丅
    # Takuri = 6              # 探水杆（下影线极长的蜻蜓十字星）

class Doji2Pattern(Enum):
    HaramiCross = 5      # 十字孕线（类似母子线，第二日是十字星）

class SpinningPattern(Enum):
    SpinningTop = 0      # 纺锤，实体短
    # ShortLine = 1        # 短蜡烛, 实体短，上下影线短
    # ShootingStar = 2     # 流星线, 上影线长
    # InvertedHammer = 3      # 倒锤头, 上影线长
    # Hammer = 4              # 锤头, 下影线长
    # HangingMan = 5          # 上吊线, 下影线长
    # SeparatingLines = 6     # 分离线


class CandlePattern(Enum):
    LongLine = 0         # 长蜡烛, 实体长
    # Marubozu = 1         # 光头光脚/缺影线
    # ClosingMarubozu = 2  # 收盘缺影线，无上影线


class RevertPattern(Enum):
    # DarkCloudCover = 0   # 乌云压顶，第一日长阳，第二日开盘价高于前一日最高价，收盘价处于前一日实体中部以下
    # Engulfing = 1        # 吞噬模式，分多头吞噬和空头吞噬（多头吞噬，第一日为阴线，第二日阳线，第一日的开盘价和收盘价在第二日开盘价收盘价之内，但不能完全相同）
    # BeltHold = 2         # 捉腰带线，第一日阴线，第二日开盘价为最低价，阳线，收盘价接近最高价
    # Thrusting = 3
    # Piercing = 4         # 刺透模式，第一日阴线，第二日阳线，第二日开盘价低于前一日最低价，收盘价高于前一日中部（实体上部）
    Harami = 5           # 母子线，分多头母子与空头母子（多头母子，第一日k线长阴，第二日开盘价收盘价在第一日价格振幅之内阳线）
    # HaramiCross = 6      # 十字孕线（类似母子线，第二日是十字星）
    # HomingPigeon = 7     # 家鸽（类似母子线，两日k线颜色相同）


def pattern_value(pattern: Enum, total_value: int) -> int:
    """
    高12位表示正模式(对应talib的100)，低12位表示反模式(对应talib的-100)
    每种pattern分别在高低段占有1bit字段的位置，位置号就是enum值
    :param pattern:
    :param total_value:
    :return:
    """
    return (total_value & (0x1001 << pattern.value)) >> pattern.value


def pattern_values(pattern_meta: EnumMeta, total_value: int) -> List[Enum]:
    patterns = []
    for pattern in pattern_meta:
        if pattern_value(pattern, total_value) >= 1:
            patterns.append(pattern)
    return patterns


def pattern_bin(total_value: int) -> str:
    low_bin_str = bin(total_value & 0xFFF)
    high_bin_str = bin(total_value >> 12)
    return f"{high_bin_str}:{low_bin_str}"


class PRI(Indicator):
    """
    Pattern Recognize Indicator

    """

    def __init__(self, pattern_type: List[str], input_values: List[OHLCV] = None):
        super().__init__()

        self.pattern_type = pattern_type
        self.open = np.array([])
        self.high = np.array([])
        self.close = np.array([])
        self.low = np.array([])

        self.enums = {
            'Doji': DojiPattern,
            'Doji2': Doji2Pattern,
            'Spinning': SpinningPattern,
            'Candle': CandlePattern,
            'Revert': RevertPattern
        }

        self.initialize(input_values)

    def add_input_value(self, value: Any) -> None:
        if type(value) == OHLCV:
            self.open = np.append(self.open, value.open)
            self.high = np.append(self.high, value.high)
            self.close = np.append(self.close, value.close)
            self.low = np.append(self.low, value.low)
        super().add_input_value(value)

    def remove_input_value(self) -> None:
        if len(self.open) > 0:
            self.open = self.open[:-1]
            self.high = self.high[:-1]
            self.close = self.close[:-1]
            self.low = self.low[:-1]
        super().remove_input_value()

    def _calculate_new_value(self) -> Any:
        if len(self.input_values) < 13:
            return None

        assert (len(self.high) == len(self.input_values))

        patterns = {}
        for pattern in self.pattern_type:
            p_value: int = 0
            pattern_enum = self.enums[pattern]
            for i, pattern_member in enumerate(pattern_enum):
                pattern_name = f"CDL{pattern_member.name.upper()}"
                pattern_result = getattr(ta, pattern_name)(self.open, self.high, self.low, self.close)
                if pattern_result[-1] > 0:
                    p_value |= 0x1000 << pattern_member.value
                elif pattern_result[-1] < 0:
                    p_value |= 0x0001 << pattern_member.value
            patterns[pattern] = p_value

        return PatternVal(**patterns)
