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
"""
Exploratory data analysis - data distribution functions.
"""

from typing import List
from nzpyida.frame import IdaDataFrame
from nzpyida.analytics.utils import call_proc_df_in_out


def moments(in_df: IdaDataFrame, in_column: str, by_column: str=None,
    out_table: str=None) -> IdaDataFrame:
    """
    Calculates the moments of a numeric input column: mean, variance,
    stand-ard deviation, skewness and (excess) kurtosis as well as the
    count of cases, the minimum and the maximum.

    Parameters:
    -----------
    in_df : IdaDataFrame
        the input data frame

    in_column : str
        the numeric input table column

    by : str
        the input table column which splits the data into groups for which the
        operation is to be per-formed

    out_table : str
        the output table to write the moments into

    Returns:
    --------
    IdaDataFrame
        the data frame with requested data
    """

    params = {
        'incolumn': in_column,
        'by': by_column
    }
    return call_proc_df_in_out(proc='MOMENTS', in_df=in_df, params=params, out_table=out_table)

def quantile(in_df: IdaDataFrame, in_column: str, quantiles: List[int],
    out_table: str=None) -> IdaDataFrame:
    """
    Calculates quantile limit(s) for a numeric column.

    Parameters:
    -----------
    in_df : IdaDataFrame
        the input data frame

    in_column : str
        the numeric input table column

    quantiles : List[int]
        the list of quantiles to be calculated.
        Quantiles are values between 0 and 1 indicating the percentage of sorted
        values to be considered in each quantile.

    out_table : str
        the output table to write the moments into

    Returns:
    --------
    IdaDataFrame
        the data frame with requested data
    """

    params = {
        'incolumn': in_column,
        'quantiles': quantiles
    }
    return call_proc_df_in_out(proc='QUANTILE', in_df=in_df, params=params, out_table=out_table)

def outliers(in_df: IdaDataFrame, in_column: str, multiplier: float=1.5,
    out_table: str=None) -> IdaDataFrame:
    """
    Detect outliers of a numeric column.

    Parameters:
    -----------
    in_df : IdaDataFrame
        the input data frame

    in_column : str
        the numeric input table column

    multiplier : float
        the value of the IQR multiplier

    out_table : str
        the output table to write the moments into

    Returns:
    --------
    IdaDataFrame
        the data frame with requested data
    """

    params = {
        'incolumn': in_column,
        'multiplier': multiplier
    }
    return call_proc_df_in_out(proc='OUTLIERS', in_df=in_df, params=params, out_table=out_table)

def unitable(in_df: IdaDataFrame, in_column: str, out_table: str=None) -> IdaDataFrame:
    """
    Creates a univariate frequency table for one column of the input table.

    Parameters:
    -----------
    in_df : IdaDataFrame
        the input data frame

    in_column : str
        the numeric input table column

    out_table : str
        the output table to write the moments into

    Returns:
    --------
    IdaDataFrame
        the data frame with requested data
    """

    params = {
        'incolumn': in_column
    }
    return call_proc_df_in_out(proc='UNITABLE', in_df=in_df, params=params, out_table=out_table)

def bitable(in_df: IdaDataFrame, in_column: List[str], freq: bool=False, cum: bool=False, 
    out_table: str=None) -> IdaDataFrame:
    """
    Creates a bivariate frequency table for two columns of the input table.

    Parameters:
    -----------
    in_df : IdaDataFrame
        the input data frame

    in_column : List[str]
        the list of numeric input table columns, must be two (if there are more, they
        will be ignored)

    freq : bool
        flag indicating whether frequencies should be attached to the output table

    cum : bool
        flag indicating whether cumulative frequencies should be attached to the output 
        table (setting this flag automatically sets freq flag as frequencies have to be 
        calculated prior to cumulative frequencies)

    out_table : str
        the output table to write the moments into

    Returns:
    --------
    IdaDataFrame
        the data frame with requested data
    """

    params = {
        'incolumn': in_column,
        'freq': freq,
        'cum': cum
    }
    return call_proc_df_in_out(proc='BITABLE', in_df=in_df, params=params, out_table=out_table)

def histogram(in_df: IdaDataFrame, in_column: str, nbreaks: int=None, right: bool=True,
    btable: str=None, bcolumn: str=None, density: bool=False, midpoints: bool=False,
    freq: bool=False, cum: bool=False, out_table: str=None) -> IdaDataFrame:
    """
    Creates histograms. The number of bins and the bins themselves
    can be spe-cified or are automatically calculated.

    Parameters:
    -----------
    in_df : IdaDataFrame
        the input data frame

    in_column : str
        the input table column to build the histogram onto

    nbreaks : int
        the number of bins for the histogram.
        If not specified, the number of bins is calculated auto-matically.

    right : bool
        the flag indicating whether the histogram bins should be
        right-closed (true) or right-open (false)

    btable : str
        the input table with breaks for the histogram.
        If not specified, the bins are calculated automat-ically, using the
        parameter <nbreaks> if specified.

    bcolumn : str
        the <btable> column containing the breaks for the histogram.
        This column must be specified if the parameter <btable> is specified.

    density : bool
        flag indicating whether densities should be attached to the output table

    midpoints : bool
        flag indicating whether the midpoints of the bins should be attached to the output table

    freq : bool
        flag indicating whether frequencies should be attached to the output table

    cum : bool
        flag indicating whether cumulative frequencies should be attached to the output table 
        (setting this flag automatically sets freq flag as frequencies have to be calculated 
        prior to cumulative frequencies)

    out_table : str
        the output table to write the moments into

    Returns:
    --------
    IdaDataFrame
        the data frame with requested data
    """

    params = {
        'incolumn': in_column,
        'nbreaks': nbreaks,
        'right': right,
        'btable': btable,
        'bcolumn': bcolumn,
        'density': density,
        'midpoints': midpoints,
        'freq': freq,
        'cum': cum
    }
    return call_proc_df_in_out(proc='HIST', in_df=in_df, params=params, out_table=out_table)
