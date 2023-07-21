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
This module consists of algorithms used to detect and quantify relationships between attributes.
"""

from nzpyida.analytics.utils import call_proc_df_in_out, make_temp_table_name, out_str_to_df
from nzpyida.frame import IdaDataFrame
from typing import List
from nzpyida.analytics.utils import q
import pandas as pd


def corr(in_df: IdaDataFrame, in_column: List[str], by_column: str=None, out_table: str=None):
    """
    This stored procedure calculates the correlation between two numeric input columns, 
    either in the whole input table or within the groups defined in the column specified by parameter <by>. 
    Correlation is a measure saying how easily it is to predict one column from the other. 
    It takes a value between -1 (inversely correlated) and 1 (correlated), 
    0 means that the two columns are independent.

    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : List[str]
        the two numeric input table columns, separated by a semicolon (;). 
        Optionally, a third numeric column can be specified for weights followed by :objweight.
    
    by_column : str, optional
        the input table column which splits the data into groups for which the operation is to be performed. 
        If specified, an output table must be indicated.
    
    out_table : str, optional
        the output table to write the correlations into. 
        This parameter must be specified if parameter by is specified.

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column)
    }
    out_df = call_proc_df_in_out(proc="CORR", in_df=in_df, params=params, out_table=out_table)[0]
    temp_table = make_temp_table_name()
    query = f"""create table {temp_table} as
            select case when CORRELATION is null then 0 else CORRELATION
            end as correlation {', "' + by_column + '"' if by_column else ''}
            from {out_table};
            drop table {out_table};
            alter table {temp_table} rename to {out_table};
            """
    in_df._idadb.ida_query(query)
    return out_df


def cov(in_df: IdaDataFrame, in_column: List[str], by_column: str=None, out_table: str=None):
    """
    This function calculates the covariance of two numeric input columns, 
    either in the whole input table or within the groups defined in the column specified by parameter <by>. 
    Covariance is a measure say-ing how easily it is to predict one column from the other. 
    It takes a negative value when both columns are inversely correlated, 
    a positive value when they are correlated, 0 means that the two columns are inde-pendent.

    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : List[str]
        the two numeric input table column, separated by a semicolon

    by_column : str, optional
        the input table column which splits the data into groups for which the operation is to be performed. 
        If specified, an output table must be indicated.
    
    out_table : str, optional
        the output table to write the covariances into. 
        This parameter must be specified if parameter <by_column> is specified.

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column)
    }
    return call_proc_df_in_out(proc="COV", in_df=in_df, params=params, out_table=out_table)[0]


def covariance_matrix(in_df: IdaDataFrame, in_column: List[str], out_table: str=None, by_column: str=None):
    """
    This function calculates the matrix of covariances between 
    pairs of numeric input columns divided into two sets X and Y, either in the whole 
    input table or within the groups defined in the column specified by parameter <by_column>. 
    Covariance is a measure saying how easily it is to predict one column from the other. 
    It takes a negative value when both columns are inversely correlated, 
    a positive value when they are correlated, 0 means that the two columns are independent.

    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : List[str], optional
        the numeric input table columns to calculate covariances for, separated by a semi- colon (;). 
        Each column is followed by :X or :Y to indicate it belongs to set X or set Y. 
        If neither :X nor :Y is specified for any of the input columns, 
        the matrix contains the cov-ariances between all pairs of input columns.

    out_table : str, optional
        the output table to write the covariances

    by_column : str, optional
        the input table column which splits the data into groups for which the operation is to be performed

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column)
    }
    return call_proc_df_in_out(proc="COVARIANCEMATRIX", in_df=in_df, params=params, out_table=out_table)[0]


def spearman_corr(in_df: IdaDataFrame, in_column: List[str], by_column: str=None, out_table: str=None):
    """
    This function calculates the Spearman rank correlation 
    between two columns whose values are ordered in their respective domain, either in the whole 
    input table or within the groups defined in the column specified by parameter <by_column>. 
    If both columns are of type double, or int, or date, or time, the order is obviously guaranteed. 
    In case of character columns, it is assumed that the order of their values is lexicographic. 
    The Spearman rank correlation is a non-parametric measure of dependence between two variables. 
    It takes a value between -1 (inversely correlated) and 1 (correlated), 
    0 means that the two columns are independent.

    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : List[str]
        the two ordinal input table columns, separated by a semicolon (;). 
        Optionally, a third numeric column can be specified for weights followed by :objweight.
    
    by_column : str, optional
        the input table column which splits the data into groups for which the operation is to be performed. 
        If specified, an output table must be indicated.
    
    out_table : str, optional
        the output table to write the Spearman rank correlations into. 
        This parameter must be specified if parameter <by_column> is specified.

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column)
    }
    temp_table = make_temp_table_name()
    if by_column:
        out_df =  call_proc_df_in_out(proc="SPEARMAN_CORR", in_df=in_df, params=params, out_table=out_table)[0]
        query = f"""create table {temp_table} as
            select {'"' + by_column + '", '} case when RHO is null then 0 else RHO
            end as RHO, N
            from {out_table};
            drop table {out_table};
            alter table {temp_table} rename to {out_table};
            """
    else:
        output = call_proc_df_in_out(proc="SPEARMAN_CORR", in_df=in_df, params=params, out_table=out_table)[1]
        out_df = in_df._idadb.as_idadataframe(pd.DataFrame(output), tablename=out_table)
        query = f"""create table {temp_table} as
            select case when SPEARMAN_CORR is null then 0 else SPEARMAN_CORR
            end as SPEARMAN_CORR
            from {out_table};
            drop table {out_table};
            alter table {temp_table} rename to {out_table};
            """
    in_df._idadb.ida_query(query)
    return out_df


def mutual_info(in_df: IdaDataFrame, in_column: List[str], by_column: str=None, out_table: str=None):
    """
    This function calculates the mutual information of two input columns, 
    either in the whole input table or within the groups defined in the column specified by parameter <by>. 
    Mutual Information is a measure saying how easily it is to predict the value of one column 
    from the value of the other column. It takes a positive value, the lower the better predictability.
    
    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : List[str]
        the two input table columns separated by a semicolon
    
    by_column : str, optional
        the input table column which splits the data into groups for which the operation is to be performed. 
        If specified, an output table must be indicated.
    
    out_table : str, optional
        the output table to write the mutual information into. 
        This parameter must be specified if parameter <by_column> is specified.

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column)
    }
    return call_proc_df_in_out(proc="MUTUALINFO", in_df=in_df, params=params, out_table=out_table)[0]


def chisq(in_df: IdaDataFrame, in_column: List[str], out_table: str=None, by_column: str=None):
    """
    This function calculates the Chi-square value between two input columns, either 
    in the whole input table or within the groups defined in the column specified by parameter <by_column>. 
    Chi-square is a measure saying how easily it is to predict one column from the other. 
    The function then returns probability that the two columns are independent.

    Additionally to the Chi-square statistics, this stored procedure returns the degree of freedom 
    of the input variables and a percentage between 0 and 1.
    If the percentage ranges from 0 to 0.05, the columns are definitely independent,
    If the percentage ranges between 0.05 and 0.95, they are to be treated as independent, though one is not sure,
    If the percentage ranges between 0.95 and 1, then a predictive mutual dependency is definitely there.
        
    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : List[str]
        the two input table columns separated by a semicolon
    
    out_table : str, optional
        the output table to write the Chi-square values

    by_column : str, optional
        he input table column which splits the data into groups for which the operation is to be performed. 
        If specified, an output table must be indicated.

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column)
    }
    return call_proc_df_in_out(proc="CHISQ_TEST", in_df=in_df, params=params, out_table=out_table)[0]


def t_me_test(in_df: IdaDataFrame, in_column: str, mean_value: float, by_column: str=None, out_table: str=None):
    """
    This function calculates the t-Student statistics of an input column with the expected mean, 
    either in the whole input table or within the groups defined in the column specified by parameter <by>. 
    This t-Student test is a measure saying whether or not the column has the given mean.
    
    If the percentage ranges from 0 to 0.05, the expected mean value is significantly too high,
    If the percentage ranges between 0.05 and 0.95, the expected mean value matches the mean value of the column,
    If the percentage ranges between 0.95 and 1, the expected mean value is significantly too low.

    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : str
        the numeric input table
    
    mean_value : float
        the expected mean value of the input column

    by_column : str, optional
        the input table column which splits the data into groups for which the operation is to be performed.
    
    out_table : str, optional
        the output table to write the t-Student statistics

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column),
        'mean': mean_value
    }
    return call_proc_df_in_out(proc="T_ME_TEST", in_df=in_df, params=params, out_table=out_table)[0]


def t_umd_test(in_df: IdaDataFrame, in_column: str, class_column: str, out_table: str=None, by_column: str=None):
    """
    This function calculates the t-Student statistics of a column whose values are split into two classes, 
    either in the whole input table or within the groups defined in the column specified by parameter <by>. 
    This t-Student test is a measure saying whether or not the two classes have the same mean value.
    
    If the percentage ranges from 0 to 0.05, the second class has a significantly bigger mean value than the first class,
    If the percentage ranges between 0.05 and 0.95, the two classes have the same mean value,
    If the percentage ranges between 0.95 and 1, the second class has a significantly smaller mean value than the first class.
    
    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : str
        the numeric input table
    
    class_column : str
        the input table column which splits data into two classes. 
        The class column name is followed by two class values preceded by a colon (:), eg. 'column_name:class_1:sclass_2'

    out_table : str, optional
        the output table to write the t-Student statistics

    by_column : str, optional
        the input table column which splits the data into groups for which the operation is to be performed.

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column),
        'class': q(class_column)
    }
    return call_proc_df_in_out(proc="T_UMD_TEST", in_df=in_df, params=params, out_table=out_table)[0]

def t_pmd_test(in_df: IdaDataFrame, in_column: List[str], expected_diff: float, out_table: str=None, by_column: str=None):
    """
    This function calculates the t-Student statistics of two paired columns, either in the whole 
    input table or within the groups defined in the column specified by parameter <by>. 
    This t-Student test is a measure saying whether or not the two columns take different values, 
    on average distinct by <expected_diff>
    
    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : List[str]
        the two numeric input table columns, separated by a semicolon (;). 
        One column must be followed by :X, the other column by :Y.
    
    expected_diff : float
        the expected difference between the mean values of the input columns (Y - X)

    out_table : str, optional
        the output table to write the t-Student statistics

    by_column : str, optional
        the input table column which splits the data into groups for which the operation is to be performed.

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column),
        'expdiff': expected_diff
    }
    return call_proc_df_in_out(proc="T_PMD_TEST", in_df=in_df, params=params, out_table=out_table)[0]


def t_ls_test(in_df: IdaDataFrame, in_column: List[str], slope: float, by_column: str=None, out_table: str=None):
    """
    This function calculates the t-Student statistics between two input columns, either in the whole 
    input table or within the groups defined in the column specified by parameter <by>. This t-Student test 
    is a measure saying whether or not the two columns are linearly related with a given slope.
    
    If the percentage ranges from 0 to 0.05, the slope is too high,
    If the percentage ranges between 0.05 and 0.95, the indicated slope is acceptable, 
    the lower the percentage the more independent the columns are,
    If the percentage ranges between 0.95 and 1, the slope is too low.

    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : List[str]
        the two numeric input table columns, separated by a semicolon (;). 
        One column must be followed by :X, the other column by :Y.
    
    slope : float
        the expected direction of dependence

    by_column : str, optional
        the input table column which splits the data into groups for which the operation is to be performed.
    
    out_table : str, optional
        the output table to write the t-Student statistics

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column),
        'slope': slope
    }
    return call_proc_df_in_out(proc="T_LS_TEST", in_df=in_df, params=params, out_table=out_table)[0]

#error in documentation
def mww_test(in_df: IdaDataFrame, in_column: str, class_column: str, by_column: str=None, out_table: str=None):
    """
    This function executes the Mann-Whitney-Wilcoxon test on a column whose values are split into 
    two classes, either in the whole input table or within the groups defined in the column specified 
    by parameter <by>. The Mann-Whitney-Wilcoxon test is a statistical hypothesis test that determines 
    whether one of two samples of independent observations tends to have larger values than the other. 
    This is indicated by the component of the output called pp. If pp < 0.05, then one of the two classes 
    tends to have larger values than the other. The class which tends to have lower-ranked values is indicated.
    
    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : str
        the input table column. It does not need to be numerical, but must be ordered in its domain
    
    class_column : str
        the input table column which splits data into two classes. 

    by_column : str, optional
        the input table column which splits the data into groups for which the operation is to be performed.

    out_table : str, optional
        the output table to write the MWW statistics into

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column),
        'class': q(class_column)
    }
    if by_column:
        return call_proc_df_in_out(proc="MWW_TEST", in_df=in_df, params=params, out_table=out_table)[0]
    else:
        output_string = call_proc_df_in_out(proc="MWW_TEST", in_df=in_df, params=params, out_table=out_table)[1][0]
        return in_df._idadb.as_idadataframe(out_str_to_df(str(output_string)), tablename=out_table)


def wilcoxon_test(in_df: IdaDataFrame, in_column: List[str], by_column: str=None, out_table: str=None):
    """
    This function executes the Wilcoxon test on two numeric columns, either in the whole input 
    table or within the groups defined in the column specified by parameter <by>. The Wilcoxon test 
    compares in non-parametric manner two variables to state if they are different in their mean value. 
    This is indicated by the component of the output called pp. If pp < 0.05, then one of the two columns 
    tends to have larger values than the other. The class which tends to have lower-ranked values is indicated

    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : List[str]
        the two numeric input table columns, separated by a semicolon (;)
    
    by_column : str, optional
        the input table column which splits the data into groups for which the operation is to be performed. 

    out_table : str, optional
        the output table to write the Wilcoxon statistics into

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column)
    }
    if by_column:
        return call_proc_df_in_out(proc="WILCOXON_TEST", in_df=in_df, params=params, out_table=out_table)[0]
    else:
        output_string = call_proc_df_in_out(proc="WILCOXON_TEST", in_df=in_df, params=params, out_table=out_table)[1][0]
        return in_df._idadb.as_idadataframe(out_str_to_df(str(output_string)), tablename=out_table)


def canonical_corr(in_df: IdaDataFrame, in_column: List[str], by_column: str=None, out_table: str=None):
    """
    This function calculates the canonical correlation of two sets of input columns, either in the whole 
    input table or within the groups defined in the column specified by parameter <by>. 
    Canonical correlation is a measure saying how easily it is to predict one column set from the other. 
    It takes a value between -1 (in- versely correlated) and 1 (correlated), 
    0 means that the two sets of columns are independent.

    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : List[str] 
        the numeric input table columns to be correlated, separated by a semicolon (;). 
        Each column is followed by :X or :Y to indicate it belongs to set X or set Y
    
    by_column : str, optional
        the input table column which splits the data into groups for which the operation is to be performed. 

    out_table : str, optional
        the output table to write the correlations into. 

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column)
    }
    return call_proc_df_in_out(proc="CANONICAL_CORR", in_df=in_df, params=params, out_table=out_table)[0]


def anova_crd_test(in_df: IdaDataFrame, in_column: List[str], treatment_column: str, by_column: str=None, out_table: str=None):
    """
    This function analyzes the variance of one or several observations for different treatments. 
    It assumes that the input table contains one or several columns with numerical (double) 
    observation results of an experiment concerning treatments indicated by the treatment parameter.

    The One-way ANOVA considers independent samples (the treatments) while the Completely Randomized Design 
    considers equally sized "samples". The implementation covers both cases. For more information, 
    see 'Completely Randomized Design'.
    
    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : List[str]
        the input table observation columns, separated by a semi-colon (;) 
    
    treatment_column : str
        the input table column identifying a unique treatment

    by_column : str, optional
        the input table column which uniquely identifies a group on which to perform ANOVA
    
    out_table : str, optional
        the output table

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column),
        'treatment': q(treatment_column)
    }
    out_df = call_proc_df_in_out(proc="ANOVA_CRD_TEST", in_df=in_df, params=params, out_table=out_table)[0]
    temp_table = make_temp_table_name()
    usual_columns = ', '.join(['TOTNO', 'TOTSU', 'TOTMEAN', 'TOTSS', 'SSCTOT', 'SSCBETWEEN', 
                                      'DFBETWEEN', 'SSCWITHIN', 'DFWITHIN', 'F'])
    query = f"""create table {temp_table} as
            select {'INCOLUMN, ' if len(in_column)>1 else ''} 
            {'"' + by_column + '", ' if by_column else ''} 
            {usual_columns},
            case when p is null then 0 else p
            end as p 
            from {out_table};
            drop table {out_table};
            alter table {temp_table} rename to {out_table};
            """
    in_df._idadb.ida_query(query)
    return out_df


def anova_rbd_test(in_df: IdaDataFrame, in_column: str, treatment_column: str, block_column: str, by_column: str=None, out_table: str=None):
    """
    This function analyzes the variance of one or several observations for different blocks of treatments. 
    It assumes that the input table contains one or several columns with numerical (double) observation 
    results of an experiment concerning treatments indicated by the treatment parameter. 
    The treatments are performed repeatedly in various blocks, e.g. from different laboratories 
    where experiments are carried out. This means there are independent samples (treatments) 
    repeatedly drawn for each block. In particular, the number of observations 
    for the same treatment in different blocks should be the same.

    Generally, we would expect differences between blocks (we split into blocks just to reduce 
    the variance resulting) and are curious about the differences concerning treatments.

    The function returns, among others, the p-value of the F test, related to the block split and the group split of the data. 
    If the p-value is larger than 0.95 then it can be said that the hypothesis is incorrect or in this case the treatment or block split had no effect.

    blsscbetween - the sum of squares between the blocks of treatments (around the overal mean), 
    bldfbetween - the number of degrees of freedom between the blocks of treatments, 
    sscwithin - the sum of squares within the treatments (around the mean of each treatment), 
    dfwithin - the sum of the number of degrees of freedom within each treatment, 
    grsscbetween - the sum of squares between the groups (around the overal mean), 
    grdfbetween - the number of degrees of freedom between the groups,
    fbl - the value of the F-statistics for the blocks of treatments,
    pbl - the probability that the true F statistics is lower or equal to the F-statistics above (computed from the sample),
    fgr - the value of the F-statistics for the groups,
    pgr - the probability that the true F statistics is lower or equal to the F-statistics above (computed from the sample).

    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : str
        the input table observation columns, separated by a semi-colon (;) 
    
    treatment_column : str
        the input table column identifying a unique treatment

    block_column : str
        the input table column identifying a unique block of treatments
        
    by_column : str, optional
        the input table column which uniquely identifies a group on which to perform ANOVA
    
    out_table : str, optional
        the output table

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column),
        'treatment': q(treatment_column),
        'block': q(block_column)
    }
    return call_proc_df_in_out(proc="ANOVA_RBD_TEST", in_df=in_df, params=params, out_table=out_table)[0]


def manova_one_way_test(in_df: IdaDataFrame, in_column: str, factor1_column: str, id_column: str=None, by_column: str=None, 
                        table_type: str='trcv', out_table: str=None, timecheck: str=None):
    """
    This function performs one-way analysis of variance/covariance aiming to tell whether or not 
    the groups of data identified by factor1 have the same mean value in all dependent variables or not. 
    As an output 4 matrices (for each task) are produced that are stored in the output table 
    the first matrix describes the ground means of all dependent variables (row vector) 
    the second matrix describes the covariance table stat-istics and their p-values (row vector) 
    the third matrix lists the eigenvalues of the covariance table statistics (column vector) 
    the forth matrix lists the eigenvectors of the covariance table statistics (row of column vec-tors)

    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : str, optional
        the input table observation columns (dependent variables), separated by a semi-colon (;) 
        used for type='column' only, for type='trcv' these are the values for col-column greater equal 2 
    
    factor1_column : str, optional
        the input table column identifying a first factor (so-called treatment in RBD/CRD nomenclature) 
        used for type='column' only, for type='trcv' these are the values for col-column equal 1
    
    id_column : str,  optional
        the input table column which uniquely identifies records used for type='column' only, 
        for type='trcv' not needed due to the structure of the table

    by_column : str, optional
        the input table column which splits the input table into subtables, on each of them a separate MANOVA is run 
        If not specified, the whole input table is subject of a single MANOVA run. 
        used for type='column' only, for type='trcv' it is by default the column named id_task
    
    table_type : str, optional
        the input table form: either 'columns' or 'trcv' trcv stands for "id_task, row, column, value" 
        id_task must be positive integer >= 1. columns means the traditional table representation
    
    out_table : str, optional
        the output table
    
    timecheck : str, optional
        the output will be enriched with execution time of critical sections if set to yes 
        If not specified, the whole input table is subject of a single MANOVA run. 
        used for type='column' only for type='trcv' it is by default the column named id_task

    Returns
    -------
    IdaDataFrame
        the data frame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column),
        'factor1': q(factor1_column),
        'type': table_type,
        'id': q(id_column),
        '_timecheck': timecheck
    }
    return call_proc_df_in_out(proc="MANOVA_ONE_WAY_TEST", in_df=in_df, params=params, out_table=out_table)[0]


def manova_two_way_test(in_df: IdaDataFrame, in_column: str, factor1_column: str, factor2_column: str, table_type: str, 
                        id_column: str=None, by_column: str=None, out_table: str=None, timecheck: str=None):
    """
    This function performs Two-way analysis of variance/covariance aiming to tell whether or 
    not the groups of data identified by factor1 have the same mean value in all dependent variables or not.

    Parameters
    ----------
    in_df : IdaDataFrame
        the input data frame

    in_column : str
        the input table observation columns (dependent variables), separated by a semi-colon (;) 
        used for type='column' only, for type='trcv' these are the values for col-column greater equal 3 
    
    factor1_column : str
        the input table column identifying a first factor (so-called treatment in RBD/CRD nomenclature),
        used for type='column' only, for type='trcv' these are the values for col-column equal 1
    
    factor2_column: str
        the input table column identifying a second factor (so-called block in RBD nomenclature),
        used for type='column' only, for type='trcv' these are the values for col-column equal 2
    
    table_type : str
        the input table form: either 'columns' or 'trcv' trcv stands for "id_task, row, column, value" 
        id_task must be positive integer >= 1. columns means the traditional table representation
    
    id_column : str, optional
        the input table column which uniquely identifies records used for type='column' only,
        for type='trcv' not needed due to the structure of the table

    by_column : str, optional
        the input table column which splits the input table into subtables, on each of them a separate MANOVA is run 
        If not specified, the whole input table is subject of a single MANOVA run. 
        used for type='column' only, for type='trcv' it is by default the column named id_task

    out_table : str, optional
        the output table
    
    timecheck : str, optional
        the output will be enriched with execution time of critical sections if set to yes 
        If not specified, the whole input table is subject of a single MANOVA run. 
        used for type='column' only, for type='trcv' it is by default the column named id_task

    Returns
    -------
    IdaDataFrame
        the data fąrame with requested data
    """
    params = {
        'incolumn': q(in_column),
        'by': q(by_column),
        'factor1': q(factor1_column),
        'factor2': q(factor2_column),
        'type': table_type,
        'id': q(id_column),
        '_timecheck': timecheck
    }
    return call_proc_df_in_out(proc="MANOVA_TWO_WAY_TEST", in_df=in_df, params=params, out_table=out_table)[0]