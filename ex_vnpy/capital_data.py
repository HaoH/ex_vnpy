from dataclasses import dataclass
from itertools import zip_longest
from typing import Optional, List, Dict


@dataclass
class CapitalData:
    order_count_buy_XL: Optional[int]
    order_count_buy_L: Optional[int]
    order_count_buy_M: Optional[int]
    order_count_buy_S: Optional[int]
    order_count_sell_XL: Optional[int]
    order_count_sell_L: Optional[int]
    order_count_sell_M: Optional[int]
    order_count_sell_S: Optional[int]
    order_volume_buy_XL: Optional[int]
    order_volume_buy_L: Optional[int]
    order_volume_buy_M: Optional[int]
    order_volume_buy_S: Optional[int]
    order_volume_sell_XL: Optional[int]
    order_volume_sell_L: Optional[int]
    order_volume_sell_M: Optional[int]
    order_volume_sell_S: Optional[int]
    volume_buy_XL: Optional[int]
    volume_buy_L: Optional[int]
    volume_buy_M: Optional[int]
    volume_buy_S: Optional[int]
    volume_sell_XL: Optional[int]
    volume_sell_L: Optional[int]
    volume_sell_M: Optional[int]
    volume_sell_S: Optional[int]
    turnover_buy_XL: Optional[float]
    turnover_buy_L: Optional[float]
    turnover_buy_M: Optional[float]
    turnover_buy_S: Optional[float]
    turnover_sell_XL: Optional[float]
    turnover_sell_L: Optional[float]
    turnover_sell_M: Optional[float]
    turnover_sell_S: Optional[float]


class CapitalDataFactory:
    @staticmethod
    def from_matrix(values: List[List]) -> List[CapitalData]:
        """
        Converts lists representing CapitalData values into lists of CapitalData objects. Expected dimension of input CapitalData list
        is 4 (without volume and timestamp), 5 (with volume and without timestamp) or 6 (with volume and timestamp).

        Unlike from_matrix2 in this method each input sublist represents an CapitalData tuple.

        Example: [[1,2,3,4,5], [6,7,8,9,0]] -> [CapitalData(1,2,3,4,5), CapitalData(6,7,8,9,0)]
        Example: [[1,2,3,4], [6,7,8,9]] -> [CapitalData(1,2,3,4), CapitalData(6,7,8,9)]
        Example: [[1,2,3,4,5,6], [7,8,9,10,11,12]] -> [CapitalData(1,2,3,4,5,6), CapitalData(7,8,9,10,11,12)]
        """
        return [CapitalData(x[0], x[1], x[2], x[3],
                          x[4], x[5], x[6], x[7],
                          x[8], x[9], x[10], x[11],
                          x[12], x[13], x[14], x[15],
                          x[16], x[17], x[18], x[19],
                          x[20], x[21], x[22], x[23],
                          x[24], x[25], x[26], x[27],
                          x[28], x[29], x[30], x[31]) for x in values]

    @staticmethod
    def from_matrix2(values: List[List[float]]) -> List[CapitalData]:
        """
        Converts lists representing O, H, L, C, V and T(ime) values into lists of CapitalData objects.

        Unlike from_matrix in this method each input sublist represents all opens, highs, ...

        Example: [[1,2], [3,4], [5,6], [7,8]] -> [CapitalData(1,3,5,7), CapitalData(2,4,6,8)]
        Example: [[1,2], [3,4], [5,6], [7,8], [9,0]] -> [CapitalData(1,3,5,7,9), CapitalData(2,4,6,8,0)]
        Example: [[1,2], [3,4], [5,6], [7,8], [9,0], [11, 12]] -> [CapitalData(1,3,5,7,9,11), CapitalData(2,4,6,8,0,12)]
        """
        return CapitalDataFactory.from_matrix(
            list(map(list,
                     zip_longest(values[0], values[1], values[2], values[3],
                                 values[4], values[5], values[6], values[7],
                                 values[8], values[9], values[10], values[11],
                                 values[12], values[13], values[14], values[15],
                                 values[16], values[17], values[18], values[19],
                                 values[20], values[21], values[22], values[23],
                                 values[24], values[25], values[26], values[27],
                                 values[28], values[29], values[30], values[31]))))

    @staticmethod
    def from_dict(values: Dict[str, List[float]]) -> List[CapitalData]:
        """
        Converts a dict with keys 'open', 'high', 'low', 'close', 'volume' and 'time' where each key
        contains a list of simple values into a list of CapitalData objects. If some key is missing, corresponding values
        in CapitalData will be None

        Example: {'open': [1,2], 'close': [3,4]} -> [CapitalData(1, None, None, 3, None, None), CapitalData(2, None, None, 4, None, None)]
        """
        return CapitalDataFactory.from_matrix2([
            values['order_count_buy_XL'] if 'order_count_buy_XL' in values else [],
            values['order_count_buy_L'] if 'order_count_buy_L' in values else [],
            values['order_count_buy_M'] if 'order_count_buy_M' in values else [],
            values['order_count_buy_S'] if 'order_count_buy_S' in values else [],
            values['order_count_sell_XL'] if 'order_count_sell_XL' in values else [],
            values['order_count_sell_L'] if 'order_count_sell_L' in values else [],
            values['order_count_sell_M'] if 'order_count_sell_M' in values else [],
            values['order_count_sell_S'] if 'order_count_sell_S' in values else [],
            values['order_volume_buy_XL'] if 'order_volume_buy_XL' in values else [],
            values['order_volume_buy_L'] if 'order_volume_buy_L' in values else [],
            values['order_volume_buy_M'] if 'order_volume_buy_M' in values else [],
            values['order_volume_buy_S'] if 'order_volume_buy_S' in values else [],
            values['order_volume_sell_XL'] if 'order_volume_sell_XL' in values else [],
            values['order_volume_sell_L'] if 'order_volume_sell_L' in values else [],
            values['order_volume_sell_M'] if 'order_volume_sell_M' in values else [],
            values['order_volume_sell_S'] if 'order_volume_sell_S' in values else [],
            values['volume_buy_XL'] if 'volume_buy_XL' in values else [],
            values['volume_buy_L'] if 'volume_buy_L' in values else [],
            values['volume_buy_M'] if 'volume_buy_M' in values else [],
            values['volume_buy_S'] if 'volume_buy_S' in values else [],
            values['volume_sell_XL'] if 'volume_sell_XL' in values else [],
            values['volume_sell_L'] if 'volume_sell_L' in values else [],
            values['volume_sell_M'] if 'volume_sell_M' in values else [],
            values['volume_sell_S'] if 'volume_sell_S' in values else [],
            values['turnover_buy_XL'] if 'turnover_buy_XL' in values else [],
            values['turnover_buy_L'] if 'turnover_buy_L' in values else [],
            values['turnover_buy_M'] if 'turnover_buy_M' in values else [],
            values['turnover_buy_S'] if 'turnover_buy_S' in values else [],
            values['turnover_sell_XL'] if 'turnover_sell_XL' in values else [],
            values['turnover_sell_L'] if 'turnover_sell_L' in values else [],
            values['turnover_sell_M'] if 'turnover_sell_M' in values else [],
            values['turnover_sell_S'] if 'turnover_sell_S' in values else []
        ])
