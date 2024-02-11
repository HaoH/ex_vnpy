from functools import lru_cache

from ex_vnpy.object import BasicSymbolData


@lru_cache(maxsize=999)
def load_symbol_meta(symbol: str, symbol_type: str = "CS") -> BasicSymbolData:
    """"""
    from vnpy.trader.database import get_database, BaseDatabase
    database: BaseDatabase = get_database()
    basic_data = database.get_basic_info_by_symbols([symbol], symbol_type=symbol_type)

    return basic_data[0]

