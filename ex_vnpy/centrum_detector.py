from datetime import datetime
from typing import Any
from pandas import DataFrame, Series
from vnpy.trader.constant import Interval


class CentrumDetector(object):
    """
    缠论中枢探测
    LastPivot(L)： 最后一个正式的pivot，由CandidatePivot转正而来
    CandidatePivot(C)：备选的pivot，符合分型条件、跟LastPivot分型相反。必须要等到出现新的CandidatePivot，上一个CandidatePivot才能转正
    BackupPivot(B)： 在寻找new CandidatePivot的过程中，发现的比Last Pivot更合适的Pivot，将在下一个new CandidatePivot确定的时候，取代原来的LastPivot

    e.g.下图所示，发现B比L高，可能更适合做L；继续探测，出现new C，此时B转正为L，原来的L取消资格
                     ^B
      ^L-----      /   \
    /        \/C---     \/new C

    """

    def __init__(self, valid_bars: int = 5, setting=None):
        super().__init__()
        self.name = 'Centrum'
        self.valid_bars: int = valid_bars  # 分型有效间距，笔

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

        self.pivot_s: Series = None

    def init_detector(self, source_df: DataFrame) -> bool:
        if len(source_df) < 2:
            print("Init detector Error! source_df is too short, less than 2")
            return False

        self.pivot_s = Series(0, index=source_df.index)
        self.last_bar_high = source_df.high.iloc[0]
        self.last_bar_low = source_df.low.iloc[0]
        # self.last_bar_index = source_df.index.iloc[0]

        for i in range(1, len(source_df)):      # 从第二个bar开始，尤其是处理周线数据时，做好初始化工作
            self.detect_next_pivot(source_df.iloc[:i+1])

        return True
        # ph_s = Series(True, index=source_df.index)
        # pl_s = Series(True, index=source_df.index)
        #
        # ph_s = self.find_pivot(source_df.high, self.valid_bars, ph_s, True)
        # pl_s = self.find_pivot(source_df.low, self.valid_bars, pl_s, False)
        # self.pivot_s.loc[ph_s == True] = 1
        # self.pivot_s.loc[pl_s == True] = -1
        #
        # self.pivot_s = self.merge_pivot(source_df)
        #
        # pivots_index = self.pivot_s[self.pivot_s != 0].index
        # self.last_pivot_index = pivots_index[-2]  # 倒数第二个是确定的
        # # self.pLastCandidatePivotIndex = pivots_index[-1]
        #
        # passed_df = source_df[source_df.index <= pivots_index[-1]]
        # self.last_bar_high = passed_df.high.iloc[-1]
        # self.last_bar_low = passed_df.low.iloc[-1]
        # self.last_before_bar_high = passed_df.high.iloc[-2]
        # self.last_before_bar_low = passed_df.low.iloc[-2]
        #
        # for i in range(len(passed_df) + 1, len(source_df) + 1):
        #     self.detect_next_pivot(source_df.iloc[:i])

    def merge_pivot(self, source_df):
        pivots_index = source_df[self.pivot_s != 0].index
        if len(pivots_index) <= 0:
            return

        last_bars = 0
        last_pivot_date = pivots_index[0]
        p_last_pivot_type = self.pivot_s.loc[last_pivot_date]
        new_pivot = Series(0, index=source_df.index)
        new_pivot.loc[last_pivot_date] = p_last_pivot_type
        for index, row in source_df.iterrows():
            if index <= last_pivot_date:
                continue

            if self.pivot_s.loc[index] + p_last_pivot_type == 0:
                if last_bars >= self.valid_bars:
                    last_pivot_date = index
                    new_pivot.loc[last_pivot_date] = self.pivot_s.loc[index]
                    p_last_pivot_type = self.pivot_s.loc[index]
                    last_bars = 0
            elif self.pivot_s.loc[index] == p_last_pivot_type == 1:
                if row.high >= source_df.high.loc[last_pivot_date]:
                    new_pivot.loc[last_pivot_date] = 0
                    new_pivot.loc[index] = 1
                    last_pivot_date = index
                    last_bars = 0
                else:
                    last_bars += 1
            elif self.pivot_s.loc[index] == p_last_pivot_type == -1:
                if row.low <= source_df.low.loc[last_pivot_date]:
                    new_pivot.loc[last_pivot_date] = 0
                    new_pivot.loc[index] = -1
                    last_pivot_date = index
                    last_bars = 0
                else:
                    last_bars += 1
            else:
                last_bars += 1
        return new_pivot

    @staticmethod
    def find_pivot(source_s, pivot_size, ps, is_high):
        for direction in ("left", "right"):
            for i in range(1, pivot_size):
                next_col_s = source_s.shift((1 if direction == "left" else -1) * i)
                if direction == "right":
                    if is_high:
                        ps = ps & ((source_s >= next_col_s) | (next_col_s.isna() & source_s))
                    else:
                        ps = ps & ((source_s <= next_col_s) | (next_col_s.isna() & source_s))
                else:
                    if is_high:
                        ps = ps & (source_s >= next_col_s)
                    else:
                        ps = ps & (source_s <= next_col_s)
        return ps

    def new_bar(self, source_df: DataFrame):
        for x in range(len(self.pivot_s), len(source_df)):
            self.pivot_s.loc[source_df.index[x]] = 0
        self.detect_next_pivot(source_df)

    def detect_next_pivot(self, source_df: DataFrame):
        last_high = self.last_bar_high
        last_low = self.last_bar_low
        high = current_high = source_df.high.iloc[-1]
        low = current_low = source_df.low.iloc[-1]

        yesterday_index = source_df.index[-2] if len(source_df) > 1 else source_df.index[-1]
        is_contain = False

        # 处理包含关系
        if (high >= last_high and low <= last_low) or (high <= last_high and low >= last_low):
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
            if self.last_before_bar_high != 0.0 and self.last_before_bar_low != 0.0:
                # 新的顶分型成立
                if last_high > current_high and last_high > self.last_before_bar_high and last_low > current_low and last_low > self.last_before_bar_low:
                    new_candidate_pivot_type = 1

                # 新的底分型成立
                if last_high < current_high and last_high < self.last_before_bar_high and last_low < current_low and last_low < self.last_before_bar_low:
                    new_candidate_pivot_type = -1

            if new_candidate_pivot_type != 0:
                # 初始化
                if self.last_candidate_pivot_index is None:
                    self.update_candidate_pivot(yesterday_index, last_high, last_low, new_candidate_pivot_type)

                # 连续相同的相同分型，选择顶分型的高位、底分型的低位
                if self.last_candidate_pivot_type == new_candidate_pivot_type:
                    if (new_candidate_pivot_type == 1 and last_high > self.last_candidate_pivot_high) or (
                            new_candidate_pivot_type == -1 and last_low < self.last_candidate_pivot_low):
                        self.update_candidate_pivot(yesterday_index, last_high, last_low, new_candidate_pivot_type)
                        if self.last_backup_pivot_index is not None and self.last_backup_pivot_bars >= self.valid_bars:
                            # if self.pLastBackupPivotType == self.pLastPivotType
                            self.pivot_s.loc[self.last_pivot_index] = 0
                            self.reset_last_pivot_using_backup()
                            self.pivot_s.loc[self.last_pivot_index] = self.last_backup_pivot_type

                # 连续不同的两个分型，需要看是否符合bar的数量要求
                elif self.last_candidate_pivot_type + new_candidate_pivot_type == 0 and self.last_candidate_pivot_bars >= self.valid_bars:
                    self.pivot_s.loc[self.last_candidate_pivot_index] = self.last_candidate_pivot_type       # 更新新的CandidatePivot作为LastPivot
                    self.update_last_pivot_using_candidate()
                    self.update_candidate_pivot(yesterday_index, last_high, last_low, new_candidate_pivot_type)
                    # self.pLastCandidateLine = line(na)
                    # set_last_candidate_line(line(na))

                # 候选分型尚未成立，新的分型跟上一个确定分型相同，却有更低的低点或者更高的高点，则更新上一个确定分型
                elif self.last_pivot_type == new_candidate_pivot_type and self.last_candidate_pivot_bars < self.valid_bars:
                    if (new_candidate_pivot_type == 1 and last_high > self.last_pivot_high) or (
                            new_candidate_pivot_type == -1 and last_low < self.last_pivot_low):
                        self.update_last_backup_pivot(yesterday_index, last_high, last_low, new_candidate_pivot_type)

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

    def on_detect_finish(self):
        if self.last_candidate_pivot_index is not None:
            self.pivot_s.loc[self.last_candidate_pivot_index] = self.last_candidate_pivot_type
        self.pivot_s.fillna(0, inplace=True)

    @property
    def last_bottom_date(self):
        if self.last_candidate_pivot_index is not None and self.last_candidate_pivot_type == -1:
            return self.last_candidate_pivot_index

        bottoms = self.pivot_s[self.pivot_s == -1]
        if len(bottoms) > 0:
            return bottoms.index[-1]
        return None

    @property
    def last_top_date(self):
        if self.last_candidate_pivot_index is not None and self.last_candidate_pivot_type == 1:
            return self.last_candidate_pivot_index

        ups = self.pivot_s[self.pivot_s == 1]
        if len(ups) > 0:
            return ups.index[-1]
        return None