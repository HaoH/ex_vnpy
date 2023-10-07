import logging
from datetime import datetime
from pandas import Series
from ex_vnpy.manager.source_manager import SourceManager


logger = logging.getLogger("Utility")

def is_trend_up(ind_values: list, up_days: int) -> bool:
    if ind_values is None or len(ind_values) < up_days:
        return False

    last_values = ind_values[-1 * up_days:]
    before = ind_values[0]
    for i in range(1, len(ind_values)):
        current = last_values[i]
        if current < before:
            return False
    return True

def has_long_shadow_up(today_bar: Series, atr: list, factor: int) -> bool:
    shadow_up = today_bar["high"] - max(today_bar["open"], today_bar["close"])
    if shadow_up >= atr[-2] * factor:
        logger.debug(f"[Utility][LongShadowUp] date: {today_bar['datetime'].strftime('%Y-%m-%d')}, shadow_up: {shadow_up:.2f}, atr_y: {atr[-2]:.2f}, factor: {factor}, shadow_up/atr_y: {shadow_up/atr[-2]:.2f}")
        return True
    return False

def has_large_drop(today_bar: Series, yesterday_bar: Series, atr: list, drop_factor: int, log: bool = True) -> bool:
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
    if (today_max_oc >= yesterday_bar["close"] and up_shadow_line >= atr[-2] * drop_factor) or \
            (today_max_oc >= yesterday_max_oc and last_up_shadow_line >= atr[-3] * drop_factor) or \
            down_solid_body >= atr[-2] * drop_factor:
        if log:
            logger.debug(f"[Utility][LargeDrop] date: {today_bar['datetime'].strftime('%Y-%m-%d')}, up_shadow_line: {up_shadow_line:.2f}, atr_y: {atr[-2]:.2f}, up_shadow_line/atr_y: {up_shadow_line/atr[-2]:.2f}, drop_factor: {drop_factor}; last_up_shadow_line: {last_up_shadow_line:.2f}, atr_yy: {atr[-3]:.2f}")
        return True
    return False

def find_real_test_days(sm: SourceManager, entry_date: datetime, max_test_days: int):
    real_test_days = 0
    for ix in range(2, max_test_days+1):
        if sm.daily_df.iloc[-1 * ix]["datetime"] == entry_date:
            # real_test_days = ix
            real_test_days = ix
            break
    return real_test_days

def is_speed_low(input: list, drop_days: int):
    if None in input:
        return False

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
