from typing import List, Any
from dataclasses import dataclass

from talipp.indicators.Indicator import Indicator
from talipp.indicators.ATR import ATR
from talipp.ohlcv import OHLCV


@dataclass
class ASXVal:
    asx: float = None
    plus_si: float = None
    minus_si: float = None


class ASX(Indicator):
    """
    Average Spine Index

    Output: a list of ASXVal
    """
    def __init__(self, period_si: int, period_asx: int, input_values: List[OHLCV] = None):
        super(ASX, self).__init__()

        self.period_si = period_si
        self.period_asx = period_asx

        self.atr = ATR(period_si)
        self.add_sub_indicator(self.atr)

        # plus spine movement
        self.psm = []
        # minus spine movement
        self.msm = []

        self.add_managed_sequence(self.psm)
        self.add_managed_sequence(self.msm)

        # smoothed plus spine movement
        self.spsm = []
        # smoothed minus spine movement
        self.smsm = []

        self.add_managed_sequence(self.spsm)
        self.add_managed_sequence(self.smsm)

        # plus spine index
        self.psi = []
        # minus spine index
        self.msi = []

        self.add_managed_sequence(self.psi)
        self.add_managed_sequence(self.msi)

        # spine index
        self.sx = []
        self.add_managed_sequence(self.sx)

        self.initialize(input_values)

    def _calculate_new_value(self) -> Any:
        if len(self.input_values) < 2:
            return None

        current_input = self.input_values[-1]
        prev_input = self.input_values[-2]

        up_spine = current_input.high - max(current_input.open, current_input.close, prev_input.high)
        down_spine = min(prev_input.low, current_input.open, current_input.close) - current_input.low

        if up_spine > down_spine and current_input.high - prev_input.high > 0:
            self.psm.append(up_spine)
        # else:
        #     self.psm.append(0)

        if down_spine > up_spine and prev_input.low - current_input.low > 0:
            self.msm.append(down_spine)
        # else:
        #     self.msm.append(0)

        if len(self.psm) < self.period_si:
            return None
        elif len(self.psm) >= self.period_si:
            if not self.spsm:
                self.spsm.append(sum(self.psm[-self.period_si:]) / float(self.period_si))
            else:
                self.spsm.append((self.spsm[-1] * (self.period_si - 1) + self.psm[-1]) / float(self.period_si))

        if len(self.msm) < self.period_si:
            return None
        elif len(self.msm) >= self.period_si:
            if not self.smsm:
                self.smsm.append(sum(self.msm[-self.period_si:]) / float(self.period_si))
            else:
                self.smsm.append((self.smsm[-1] * (self.period_si - 1) + self.msm[-1]) / float(self.period_si))

        self.psi.append(100.0 * self.spsm[-1] / float(self.atr[-1]))
        self.msi.append(100.0 * self.smsm[-1] / float(self.atr[-1]))

        self.sx.append(100.0 * float(abs(self.psi[-1] - self.msi[-1])) / (self.psi[-1] + self.msi[-1]))

        asx = None
        if len(self.sx) == self.period_asx:
            asx = sum(self.sx) / float(self.period_asx)
        elif len(self.sx) > self.period_asx:
            asx = (self.output_values[-1].asx * (self.period_asx - 1) + self.sx[-1]) / float(self.period_asx)

        return ASXVal(asx, self.psi[-1], self.msi[-1])
