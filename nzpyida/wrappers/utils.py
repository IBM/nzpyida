#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Copyright (c) 2023, IBM Corp.
# All rights reserved.
#
# Distributed under the terms of the BSD Simplified License.
#
# The full license is in the LICENSE file, distributed with this software.
#----------------------------------------------------------------------------- 
from typing import Dict, Tuple, Any
from time import time
import random
from nzpyida.frame import IdaDataFrame


def map_to_props(data: Dict[str, Any]) -> str:
    """
    Maps the given dictionary of attributes into coma separated list of key/value
    pairs. Skips these pairs with value None.
    """
    ret = []
    for k, v in data.items():
        if v is not None:
            ret.append(f"{k}={v}")
    return ",".join(ret)

def materialize_df(df: IdaDataFrame) -> Tuple[str, bool]:
    """
    Creates a view associated to the given data frame in the database.
    If the data frame requires no view, simply returns a table assiciated with it.
    The second items of the returned tuple shows if what is returned is a temporaty view
    (that should be dropped by the caller) or exisintg table.
    """

    if df.internal_state.views:
        temp_view_name = make_temp_table_name()
        df.internal_state._create_view(viewname=temp_view_name)
        return temp_view_name, True
    else:
        return df.tablename, False

def make_temp_table_name(prefix: str='DATA_FRAME_') -> str:
    """
    Generate temp table name.
    """
    return f'{prefix}{random.randint(0, 100000)}_{int(time())}'
