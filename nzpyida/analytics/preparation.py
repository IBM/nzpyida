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
from nzpyida.analytics.utils import map_to_props, materialize_df, make_temp_table_name
from nzpyida.analytics.utils import get_auto_delete_context


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

    Parameters:
    -----------
    in_df : IdaDataFrame
        the input data frame

    id_column : str
        the input table column identifying a unique instance id

    in_column : str
        the input table columns to consider, separated by a semi-colon (;).
        Each column name may be followed by :L to leave it unchanged, by :S to standardize
        its values, by :N to normalize its values or by :U to make it of unit length.
        Additionally, two columns may be indicated, separated by a slash (/), followed
        by :C to make the columns be a row unit vector or by :V to divide the column
        values by the length of the longest row vector.

    by : str, optional
        the input table column which splits the data into groups for which the operation
        is to be performed

    out_table : str, optional
        the output table with the modified data

    Returns:
    --------
    IdaDataFrame
        the data frame with requested transformations
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

    Parameters:
    -----------
    in_df : IdaDataFrame
        the input data frame

    in_column : str, optional
        the input table column where missing values have to be replaced.
        If not specified, all input data columns are considered.

    method : str, optional
        the data imputation method. Allowed values are: mean, median, freq (most frequent value),
        replace. If not specified, the method is median for the numeric columns and freq for
        the nominal columns. The methods mean and median cannot be used with nominal columns.

    numeric_value : float, optional
        the numeric replacement value when method=replace

    nominal_value : str, optional
        the nominal replacement value when method=replace

    out_table : str, optional
        the output table with the modified data

    Returns:
    --------
    IdaDataFrame
        the data frame with requested transformations
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
    Creates a random sample of a data frame a fixed size or a fixed probability and
    returns the result in a new data frame.

    Parameters:
    -----------
    in_df : IdaDataFrame
        the input data frame

    size : int
        the number of rows in the sample (alias of size). If specified,
        the parameter <fraction> must not be specified. Only one of both parameters <num>
        and <size> must be specified.

    fraction : float
        the probability of each row to be in the sample. If specified, the parameters <num>
        and <size> must not be specified. Otherwise, one of both parameters <num> or <size>
        must be specified.

    by : str
        the column used to stratify the input table. If indicated, stratified sampling is
        done: it ensures that each value of the column is represented in the sample in
        about the same percentage as in the original input table.

    out_signature : str
        the input table columns to keep in the sample, separated by a semi-colon (;).
        If not specified, all columns are kept in the output table.

    rand_seed : int
        the seed of the random function

    out_table : str
        the output table with the modified data

    Returns:
    --------
    IdaDataFrame
        the data frame with requested transformations
    """

    params = {
        'size': size,
        'fraction': fraction,
        'by': by,
        'outsignature': out_signature,
        'randseed': rand_seed
    }
    return __prepare(proc='RANDOM_SAMPLE', in_df=in_df, params=params, out_table=out_table)
