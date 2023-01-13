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
from nzpyida.frame import IdaDataFrame
from nzpyida.wrappers.utils import map_to_props, materialize_df, make_temp_table_name
from nzpyida.wrappers.utils import get_auto_delete_context


def __prepare(proc: str, in_df: IdaDataFrame, params: dict, out_table: str=None) -> IdaDataFrame:
    """
    Generic function for data processing.
    """

    temp_view_name, need_delete = materialize_df(in_df)

    auto_delete_context = None
    if not out_table:
        auto_delete_context = get_auto_delete_context('out_table')
        out_table = make_temp_table_name()

    params['intable'] = temp_view_name
    params['outtable'] = out_table

    params_s = map_to_props(params)

    try:
        in_df.ida_query(f'call NZA..{proc}(\'{params_s}\')')
    finally:
        if need_delete:
            in_df._idadb.drop_view(temp_view_name)

    if auto_delete_context:
        auto_delete_context.add_table_to_delete(out_table)

    return IdaDataFrame(in_df._idadb, out_table)


def std_norm(in_df: IdaDataFrame, id_column: str, in_column: str, by: str=None,
    out_table: str=None) -> IdaDataFrame:
    """
    Normalize and stardardize columns of the input data frame and returns that in a new data frame.
    """

    params = {
        'id': id_column,
        'incolumn': in_column,
        'by': by
    }
    return __prepare(proc='STD_NORM', in_df=in_df, params=params, out_table=out_table)


def impute_data(in_df: IdaDataFrame, in_column: str=None, method: str=None, 
    numeric_value: float=-1, nominal_value: str='missing', out_table: str=None) -> IdaDataFrame:
    """
    Replaces missing values in the input data frame and returns the result in a new data frame.
    """

    params = {
        'incolumn': in_column,
        'method': method,
        'numericvalue': numeric_value,
        'nominalvalue': nominal_value
    }
    return __prepare(proc='IMPUTE_DATA', in_df=in_df, params=params, out_table=out_table)


def random_sample(in_df: IdaDataFrame, size: int, fraction: float=None,
    by: str=None, out_signature: str=None, rand_seed: int=None, out_table: str=None) -> IdaDataFrame:
    """
    Creates a random sample of a data frame a fixed size or a fixed probability and returns the result
    in a new data frame.
    """

    params = {
        'size': size,
        'fraction': fraction,
        'by': by,
        'outsignature': out_signature,
        'randseed': rand_seed
    }
    return __prepare(proc='RANDOM_SAMPLE', in_df=in_df, params=params, out_table=out_table)


