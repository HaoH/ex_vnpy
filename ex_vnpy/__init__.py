from functools import lru_cache

from ex_vnpy.object import BasicStockData


@lru_cache(maxsize=999)
def load_symbol_meta(symbol: str) -> BasicStockData:
    """"""
    from vnpy.trader.database import get_database, BaseDatabase
    database: BaseDatabase = get_database()
    basic_data = database.get_basic_info_by_symbol(symbol)

    return basic_data

