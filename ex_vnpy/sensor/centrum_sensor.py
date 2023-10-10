import logging
from datetime import datetime, timedelta
from typing import Any, Dict

import numpy as np
from pandas import DataFrame, Series
from vnpy.trader.constant import Interval

logger = logging.getLogger("CentrumDetector")


class CentrumSensor(object):
    """
    缠论中枢探测
    LastPivot(L)： 最后一个正式的pivot，由CandidatePivot转正而来
    CandidatePivot(C)：备选的pivot，符合分型条件、跟LastPivot分型相反。必须要等到出现新的CandidatePivot，上一个CandidatePivot才能转正
    BackupPivot(B)： 在寻找new CandidatePivot的过程中，发现的比Last Pivot更合适的Pivot，将在下一个new CandidatePivot确定的时候，取代原来的LastPivot
    IgnorePivot(I): 在寻找new CandidatePivot的过程中，发现的跟当前CandidatePivot相同分型，但是应该忽略的pivot（底分型价格高于当前candidate，顶分型价格低于当前candidate）

    e.g.下图所示，发现B比L高，同时B和C1之间bar不满足数量要求，B可能更适合做L；继续探测，出现C2，此时B转正为L，原来的L取消资格
                     ^B(替代L)
                   /  \      ^
      ^L-----    /     \   /  \/----
    /        \/         \/     I
             C1         C2

    """

    def __init__(self, valid_bars: int = 5, enableContain = True, setting=None):
        super().__init__()
        self.name = 'Centrum'
        self.valid_bars: int = valid_bars  # 分型有效间距，笔
        self.enableContain: bool = enableContain    # contain处理是否有效

        self.last_pivot_index: Any = None  # 上一个确定的pivot的index(日期)
        self.last_candidate_pivot_index: Any = None  # 上一个备选的pivot的index(日期)
        self.last_backup_pivot_index: Any = None  # 上一个确定的pivot的备选的pivot的index(日期)
        # self.last_bar_index: Any = None   # 上一个探测的bar的index(日期)

        self.last_candidate_pivot_bars: int = 0  # 上一个pivot以来，有多少没有包含关系的bar
        self.last_backup_pivot_bars: int = 0  # 上一个确定的pivot的备选的pivot以来，有多少没有包含关系的bar

        self.last_pivot_high: float = 0.0  # 上一个确定的Pivot的high
        self.last_pivot_low: float = 0.0  # 上一个确定的Pivot的low
        self.last_pivot_type: int = 0  # 上一个确定的Pivot的type，1表示顶分型，-1表示底分型

        self.last_candidate_pivot_high: float = 0.0  # 最新的Candidate Pivot的high
        self.last_candidate_pivot_low: float = 0.0  # 最新的Candidate Pivot的low
        self.last_candidate_pivot_type: int = 0  # 最新的Candidate Pivot的type, 1 & -1

        self.last_backup_pivot_high: float = 0.0  # 最近的backup pivot的high
        self.last_backup_pivot_low: float = 0.0  # 最近的backup pivot的low
        self.last_backup_pivot_type: int = 0  # 最近的backup pivot的type, 1 & -1

        self.last_bar_high: float = 0.0  # 上一个有效的bar的high （去除包含关系的新生成的bar）
        self.last_bar_low: float = 0.0  # 上一个有效的bar的low （去除包含关系的新生成的bar）
        self.last_before_bar_high: float = 0.0  # 上上个有效的bar的high
        self.last_before_bar_low: float = 0.0  # 上上个有效的bar的low

        # pivot,high,low, 存储顶底分型的high/low, pivot值的含义
        # 1/-1 表示确定的顶底分型
        # 2/-2 表示candidate pivot 顶底分型
        # 3/-3 表示历史的candidate pivot 顶底分型
        # 4/-4 表示backup pivot 顶底分型
        # 5/-5 表示ignore pivot 顶底分型
        self.pivot_df: DataFrame = None
        self.inited: bool = False

        self.source_df: DataFrame = None
        self.backup_point: Dict = {}        # 用来备份当前的所有状态

    def init_sensor(self, source_df: DataFrame) -> bool:
        if source_df is None or len(source_df) < 2:
            # logger.debug("[Centrum]Init detector Error! source_df is too short, less than 2")
            self.inited = False
            return False

        self.source_df = source_df
        self.pivot_df = DataFrame(columns=['pivot', 'high', 'low'], index=source_df.index)
        self.last_bar_high = source_df.high.iloc[0]
        self.last_bar_low = source_df.low.iloc[0]
        # self.last_bar_index = source_df.index.iloc[0]

        for i in range(1, len(source_df)):      # 从第二个bar开始，尤其是处理周线数据时，做好初始化工作
            self.detect_next_pivot(source_df.iloc[:i+1])

        self.inited = True
        return True

    def update_bar(self, source_df: DataFrame):
        if not self.inited:
            self.init_sensor(source_df)
            return

        source_len = len(source_df)
        pivot_len = len(self.pivot_df)
        if pivot_len < source_len:
            for x in range(pivot_len, source_len):
                self.pivot_df.loc[source_df.index[x]] = Series(data=[0, None, None], index=['pivot', 'high', 'low'])
            self.backup_point = self.backup_current_stats()
            self.detect_next_pivot(source_df)
        else:
            self.restore_to_last_backup_point(self.backup_point)
            self.detect_next_pivot(source_df)

    def detect_next_pivot(self, source_df: DataFrame):
        last_high = self.last_bar_high
        last_low = self.last_bar_low
        high = current_high = source_df.high.iloc[-1]
        low = current_low = source_df.low.iloc[-1]

        yesterday_index = source_df.index[-2] if len(source_df) > 1 else source_df.index[-1]
        is_contain = False

        # TODO: 包含关系需要考虑实体柱的位置，如果后一个实体柱完全处于前一个的影线区域，则不算做包含？ 2018-11-27   603501
        # 处理包含关系
        if self.enableContain and ((high >= last_high and low <= last_low) or (high <= last_high and low >= last_low)):
            # 短期上升
            if self.last_before_bar_high < last_high:
                current_high = max(high, last_high)
                current_low = max(low, last_low)
            else:
                current_high = min(high, last_high)
                current_low = min(low, last_low)
            is_contain = True

        else:
            # 更新candidate pivot到最新的bar的计数
            self.last_candidate_pivot_bars = self.last_candidate_pivot_bars + 1
            # 如果存在backup pivot，则同步更新backup pivot到最新的bar的计数
            if self.last_backup_pivot_index is not None:
                self.last_backup_pivot_bars = self.last_backup_pivot_bars + 1

            new_candidate_pivot_type = 0
            # 确定新的candidate pivot
            if not (self.last_before_bar_high == 0.0 and self.last_before_bar_low == 0.0):
                # 新的顶分型成立
                if last_high > current_high and last_high > self.last_before_bar_high and last_low >= current_low and last_low >= self.last_before_bar_low:
                    new_candidate_pivot_type = 1

                # 新的底分型成立
                if last_high <= current_high and last_high <= self.last_before_bar_high and last_low < current_low and last_low < self.last_before_bar_low:
                    new_candidate_pivot_type = -1

            if new_candidate_pivot_type != 0:
                # 初始化
                if self.last_candidate_pivot_index is None:
                    self.update_candidate_pivot(yesterday_index, last_high, last_low, new_candidate_pivot_type)
                    self.pivot_df.loc[self.last_candidate_pivot_index] = Series(data=[self.last_candidate_pivot_type * 3, self.last_candidate_pivot_high, self.last_candidate_pivot_low], index=['pivot', 'high', 'low'])

                # 连续相同的相同分型，选择顶分型的高位、底分型的低位
                if self.last_candidate_pivot_type == new_candidate_pivot_type:
                    if (new_candidate_pivot_type == 1 and last_high > self.last_candidate_pivot_high) or (
                            new_candidate_pivot_type == -1 and last_low < self.last_candidate_pivot_low):
                        self.pivot_df.loc[self.last_candidate_pivot_index, 'pivot'] = self.last_candidate_pivot_type * 3
                        self.update_candidate_pivot(yesterday_index, last_high, last_low, new_candidate_pivot_type)
                        self.pivot_df.loc[self.last_candidate_pivot_index] = Series(data=[self.last_candidate_pivot_type * 2, self.last_candidate_pivot_high, self.last_candidate_pivot_low], index=['pivot', 'high', 'low'])

                        if self.last_backup_pivot_index is not None and self.last_backup_pivot_bars >= self.valid_bars:
                            # if self.pLastBackupPivotType == self.pLastPivotType
                            # self.pivot_df.loc[self.last_pivot_index] = Series(data=[0, None, None], index=['pivot', 'high', 'low'])
                            self.pivot_df.loc[self.last_pivot_index, 'pivot'] = self.last_pivot_type * 3
                            self.reset_last_pivot_using_backup()
                            self.pivot_df.loc[self.last_pivot_index] = Series(data=[self.last_backup_pivot_type, self.last_backup_pivot_high, self.last_backup_pivot_low], index=['pivot', 'high', 'low'])
                    else:
                        # 当前是IgnorePivot，仅做记录，5/-5
                        self.pivot_df.loc[yesterday_index] = Series(data=[new_candidate_pivot_type * 5, last_high, last_low], index=['pivot', 'high', 'low'])

                # 连续不同的两个分型，需要看是否符合bar的数量要求
                elif self.last_candidate_pivot_type + new_candidate_pivot_type == 0:
                    if self.last_candidate_pivot_bars >= self.valid_bars:
                        # self.pivot_s.loc[self.last_candidate_pivot_index] = self.last_candidate_pivot_type
                        # 更新新的CandidatePivot作为LastPivot
                        self.update_last_pivot_using_candidate()
                        self.pivot_df.loc[self.last_pivot_index] = Series(data=[self.last_pivot_type, self.last_pivot_high, self.last_pivot_low], index=['pivot', 'high', 'low'])

                        self.update_candidate_pivot(yesterday_index, last_high, last_low, new_candidate_pivot_type)
                        self.pivot_df.loc[self.last_candidate_pivot_index] = Series(data=[self.last_candidate_pivot_type * 2, self.last_candidate_pivot_high, self.last_candidate_pivot_low], index=['pivot', 'high', 'low'])
                    else:
                        # 当前是IgnorePivot，仅做记录，5/-5
                        self.pivot_df.loc[yesterday_index] = Series(data=[new_candidate_pivot_type * 5, last_high, last_low], index=['pivot', 'high', 'low'])

                # 候选分型尚未成立，新的分型跟上一个确定分型相同，却有更低的低点或者更高的高点，则更新上一个确定分型
                elif self.last_pivot_type == new_candidate_pivot_type and self.last_candidate_pivot_bars < self.valid_bars:
                    if (new_candidate_pivot_type == 1 and last_high > self.last_pivot_high) or (
                            new_candidate_pivot_type == -1 and last_low < self.last_pivot_low):
                        self.update_last_backup_pivot(yesterday_index, last_high, last_low, new_candidate_pivot_type)
                        # 当前是backup pivot, 仅做记录, 4/-4
                        self.pivot_df.loc[self.last_backup_pivot_index] = Series(data=[self.last_backup_pivot_type * 4, self.last_backup_pivot_high, self.last_backup_pivot_low], index=['pivot', 'high', 'low'])

        if not is_contain:
            self.last_before_bar_high = last_high
            self.last_before_bar_low = last_low
            self.last_bar_high = current_high
            self.last_bar_low = current_low
        else:
            self.last_bar_high = current_high
            self.last_bar_low = current_low

    def update_candidate_pivot(self, index, hi, lo, tp):
        """
        当探测到新的candidate pivot时，更新CP的信息
        :param index:
        :param hi:
        :param lo:
        :param tp:
        :return:
        """
        self.last_candidate_pivot_index = index
        self.last_candidate_pivot_high = hi
        self.last_candidate_pivot_low = lo
        self.last_candidate_pivot_type = tp
        self.last_candidate_pivot_bars = 1  # 重置有效bar计数

    def update_last_pivot_using_candidate(self):
        self.last_pivot_index = self.last_candidate_pivot_index
        self.last_pivot_high = self.last_candidate_pivot_high
        self.last_pivot_low = self.last_candidate_pivot_low
        self.last_pivot_type = self.last_candidate_pivot_type
        self.last_backup_pivot_index = None
        self.last_backup_pivot_bars = 1

    def update_last_backup_pivot(self, index, hi, lo, tp):
        self.last_backup_pivot_index = index
        self.last_backup_pivot_high = hi
        self.last_backup_pivot_low = lo
        self.last_backup_pivot_type = tp
        self.last_backup_pivot_bars = 1  # 重置有效bar计数

    def reset_last_pivot_using_backup(self):
        self.last_pivot_index = self.last_backup_pivot_index
        self.last_pivot_type = self.last_backup_pivot_type
        self.last_pivot_high = self.last_backup_pivot_high
        self.last_pivot_low = self.last_backup_pivot_low
        self.last_backup_pivot_index = None
        self.last_backup_pivot_bars = 1

    @property
    def last_bottom_date(self):
        bottoms = self.pivot_df[self.pivot_df["pivot"].isin([-1, -2])]
        return bottoms.index[-1] if len(bottoms) > 0 else None

    @property
    def last_top_date(self):
        ups = self.pivot_df[self.pivot_df["pivot"].isin([1, 2])]
        return ups.index[-1] if len(ups) > 0 else None

    def backup_current_stats(self):
        backup_point = {}
        self_dict = vars(self)
        for name, value in self_dict.items():
            if name not in ['name', 'valid_bars', 'pivot_df', 'backup_point']:
                backup_point[name] = value

        if self.last_backup_pivot_index is not None:
            backup_point['last_candidate_pivot_index_value_s'] = self.pivot_df.loc[self.last_candidate_pivot_index]
        if self.last_pivot_index is not None:
            backup_point['last_pivot_index_value_s'] = self.pivot_df.loc[self.last_pivot_index]
        return backup_point

    def restore_to_last_backup_point(self, backup_point: Dict):
        if self.last_candidate_pivot_index is not None and 'last_candidate_pivot_index' in backup_point.keys() and self.last_candidate_pivot_index != backup_point['last_candidate_pivot_index']:
            self.pivot_df.loc[self.last_candidate_pivot_index, 'pivot'] = 0
        if self.last_pivot_index is not None and 'last_pivot_index' in backup_point.keys() and self.last_pivot_index != backup_point['last_pivot_index']:
            self.pivot_df.loc[self.last_pivot_index, 'pivot'] *= 2

        if 'last_candidate_pivot_index_value_s' in backup_point.keys():
            last_candidate_pivot_index = backup_point['last_candidate_pivot_index']
            self.pivot_df.loc[last_candidate_pivot_index] = backup_point['last_candidate_pivot_index_value_s']
        if 'last_pivot_index_value_s' in backup_point.keys():
            last_pivot_index = backup_point['last_pivot_index']
            self.pivot_df.loc[last_pivot_index] = backup_point['last_pivot_index_value_s']

        for name, value in backup_point.items():
            if name not in ['last_candidate_pivot_index_value_s', 'last_pivot_index_value_s']:
                setattr(self, name, value)


    def latest_pivot_df(self, last_signal_days, today):
        if not self.inited:
            return None

        new_pivot_df = self.pivot_df.copy()
        # 对于最新出现的backup pivot，需要纳入到背离范围
        if self.last_backup_pivot_index is not None and self.last_backup_pivot_index + timedelta(days=last_signal_days) >= today:
            new_pivot_df.loc[self.last_backup_pivot_index] = Series(data=[self.last_backup_pivot_type * 3, self.last_backup_pivot_high, self.last_backup_pivot_low], index=['pivot', 'high', 'low'])

        return new_pivot_df
