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
import pandas as pd
import random
import re
from nzpyida.frame import IdaDataFrame
from nzpyida.analytics.auto_delete_context import AutoDeleteContext


def map_to_props(data: Dict[str, Any]) -> str:
    """
    Maps the given dictionary of attributes into coma separated list of key/value
    pairs. Skips these pairs with value None.

    Parameters
    ----------
    data : dict
        the input data

    Returns
    -------
    str
        the output string
    """

    ret = []
    for k, v in data.items():
        if isinstance(v, list):
            if len(v):
                ret.append(f"{k}={';'.join(map(str, v))}")
        elif v is not None:
            ret.append(f"{k}={v}")
    return ",".join(ret)

def materialize_df(df: IdaDataFrame) -> Tuple[str, bool]:
    """
    Creates a view associated to the given data frame in the database.
    If the data frame requires no view, simply returns a table assiciated with it.
    The second items of the returned tuple shows if what is returned is a temporaty view
    (that should be dropped by the caller) or exisintg table.

    Parameters
    ----------
    df : IdaDataFrame
        the input data frame

    Returns
    -------
    str
        the name of db object (view or table) available representing the data frame

    bool
        should the object be dropped by the caller
    """

    if df.internal_state.views:
        temp_view_name = make_temp_table_name()
        query = f'CREATE VIEW {temp_view_name} AS ({df.internal_state.get_state()})'
        df.ida_query(query, autocommit = True)
        return temp_view_name, True
    else:
        return df.tablename, False

def make_temp_table_name(prefix: str='DATA_FRAME_') -> str:
    """
    Generate temp table name.

    Parameters
    ----------
    prefix : str, optional
        name prefix

    Returns
    -------
    str
        generated table name with the given prefix
    """

    return f'{prefix}{random.randint(0, 100000)}_{int(time())}'

def get_auto_delete_context(out_table_attr_name: str) -> AutoDeleteContext:
    """
    Returns an auto delete context manager assigned to the current thread.
    If nothing is assigned, raise an exception with appropriate error message.
    The 'out_table_attr_name' is included in that message.

    Parameters
    ----------
    out_table_attr_name : str
        the name of function parameter with the output table name (used for
        a message in the exception only)

    Returns
    -------
    AutoDeleteContext
        current thread AutoDeleteContext

    Raises
    ------
    RuntimeError
        If there is no AutoDeleteContext attached to the current thread.
    """

    if AutoDeleteContext.current() is None:
        raise RuntimeError('This code needs to run inside of AutoDeleteContext context manager or '
        f'you need to set an output table name in {out_table_attr_name} function attribute')
    return AutoDeleteContext.current()

def call_proc_df_in_out(proc: str, in_df: IdaDataFrame, params: dict,
    out_table: str=None, copy_indexer=False) -> Tuple[IdaDataFrame, str]:
    """
    Generic function for data processing.
    """
    if not isinstance(in_df, IdaDataFrame):
        raise TypeError("Argument in_df should be an IdaDataFrame")

    if out_table and in_df._idadb.exists_table_or_view(out_table):
            in_df._idadb.drop_table(out_table)

    temp_view_name, need_delete = materialize_df(in_df)

    auto_delete_context = None
    if not out_table:
        auto_delete_context = get_auto_delete_context('out_table')
        out_table = make_temp_table_name()

    params['intable'] = temp_view_name
    params['outtable'] = out_table

    params_s = map_to_props(params)

    try:
        out_query = in_df.ida_query(f'call NZA..{proc}(\'{params_s}\')')
    finally:
        if need_delete and in_df._idadb.exists_table_or_view(temp_view_name):
            in_df._idadb.drop_view(temp_view_name)

    if not in_df._idadb.exists_table_or_view(out_table):
        # stored procedure call was successful by did not produce a table
        return None, out_query

    if auto_delete_context:
        auto_delete_context.add_table_to_delete(out_table)

    out_df = IdaDataFrame(in_df._idadb, out_table)
    if copy_indexer and in_df.indexer:
        out_df.indexer = in_df.indexer
    return out_df, out_query

def out_str_to_df(out_str: str):
    clean_str = re.sub('[^a-zA-Z0-9" ]', '', out_str)
    clean_str_split =clean_str.split(' ')
    out_dict = {}
    for i in range(0, len(clean_str_split), 2):
        try:
            value = int(clean_str_split[i+1])
        except ValueError:
            value = clean_str_split[i+1]
        out_dict[clean_str_split[i].upper()] = [value]
    return  pd.DataFrame.from_dict(out_dict)

def q(txt):
    """
    Quotes the given object. It supports the following types:
    str - it will be quited, it it is not already quoted. Also strings in format A:B got quoting only for A part.
    list - each item is quoted separately and the list is returned.
    everything else - txt without modifications is returned

    Parameters
    ----------
    txt
        an object to quote

    Returns
    -------
    Quited copy of the object
    """
    
    if isinstance(txt, str) and (not txt.startswith('"') or not txt.endswith('"')):
        if ':' in txt:
            ix = txt.index(':')
            return f'"{txt[:ix]}":{txt[ix+1:]}'
        return f'"{txt}"'
    elif isinstance(txt, list):
        return [q(x) for x in txt]
    else:
        return txt
