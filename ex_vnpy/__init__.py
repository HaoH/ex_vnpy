from functools import lru_cache

from ex_vnpy.object import BasicSymbolData

__version__ = "1.0.0"


@lru_cache(maxsize=999)
def load_symbol_meta(symbol: str, symbol_type: str = "CS") -> BasicSymbolData:
    """"""
    from vnpy.trader.database import BaseDatabase, get_database

    database: BaseDatabase = get_database()
    basic_data = database.get_basic_info_by_symbols([symbol], symbol_type=symbol_type)

    return basic_data[0]
