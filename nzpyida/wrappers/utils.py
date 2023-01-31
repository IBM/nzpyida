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
from nzpyida.wrappers.auto_delete_context import AutoDeleteContext


def map_to_props(data: Dict[str, Any]) -> str:
    """
    Maps the given dictionary of attributes into coma separated list of key/value
    pairs. Skips these pairs with value None.

    Parameters:
    -----------
    data : dict
        the input data

    Returns:
    --------
    str
        the output string
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

    Parameters:
    -----------
    df : IdaDataFrame
        the input data frame

    Returns:
    --------
    str
        the name of db object (view or table) available representing the data frame

    bool
        should the object be dropped by the caller
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

    Parameters:
    -----------
    prefix : str, optional
        name prefix

    Returns:
    --------
    str
        generated table name with the given prefix
    """

    return f'{prefix}{random.randint(0, 100000)}_{int(time())}'

def get_auto_delete_context(out_table_attr_name: str) -> AutoDeleteContext:
    """
    Returns an auto delete context manager assigned to the current thread.
    If nothing is assigned, raise an exception with appropriate error message.
    The 'out_table_attr_name' is included in that message.

    Parameters:
    -----------
    out_table_attr_name : str
        the name of function parameter with the output table name (used for
        a message in the exception only)

    Returns:
    --------
    AutoDeleteContext
        current thread AutoDeleteContext

    Raises:
    -------
    RuntimeError
        If there is no AutoDeleteContext attached to the current thread.
    """
    
    if AutoDeleteContext.current() is None:
        raise RuntimeError('This code needs to run inside of AutoDeleteContext context manager or '
        f'you need to set an output table name in {out_table_attr_name} function attribute')
    return AutoDeleteContext.current()
