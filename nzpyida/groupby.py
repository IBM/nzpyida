#!/usr/bin/env python
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
IdaDataFrameGroupBy
"""
import pandas as pd
import nzpyida

class IdaDataFrameGroupBy(object):
    """
    Class representing groupby object, that is object created from IdaDataFrame.
    It implements Pandas-like object for grouping.

    Examples
    ---------
    >>> ida_iris.groupby("CLASS")
    <nzpyida.groupby.IdaDataFrameGroupBy at 0x11c288e10>
        
    >>> df.groupby("CLASS").mean().head()
            AVG_SEPAL_LENGTH 	AVG_SEPAL_WIDTH 	AVG_PETAL_LENGTH 	AVG_PETAL_WIDTH 	CLASS
        0 	5.006 	            3.418 	            1.464 	            0.244 	            Iris-setosa
        1 	5.936 	            2.770 	            4.260 	            1.326 	            Iris-versicolor
        2 	6.588 	            2.974 	            5.552 	            2.026 	            Iris-virginica

    >>> mean_sepal_length = ida_iris[["SEPAL_LENGTH", "CLASS"]].groupby("CLASS").mean()
    >>> mean_sepal_length.head()
            AVG_SEPAL_LENGTH 	CLASS
        0 	5.006 	            Iris-setosa
        1 	5.936 	            Iris-versicolor
        2 	6.588 	            Iris-virginica
        
    >>> iris_groupby = ida_iris[(ida_iris["SEPAL_LENGTH"]>4) & (ida_iris["SEPAL_WIDTH"]<3)][
    >>>     ["SEPAL_WIDTH", "SEPAL_LENGTH", "CLASS"]].groupby("CLASS")
    >>> iris_groupby
    <nzpyida.groupby.IdaDataFrameGroupBy object at 0x119863190>
    >>> iris_groupby.count().head()
            COUNT_SEPAL_WIDTH 	COUNT_SEPAL_LENGTH 	CLASS
        0 	2 	                2 	                Iris-setosa
        1 	21 	                21 	                Iris-virginica
        2 	34 	                34 	                Iris-versicolor
    """
    def __init__(self, in_df, columns_to_aggregate, by_column) -> None:
        """
        Parameters
        -----------
        in_df : IdaDataFrame
            DataFrame is to be grouped

        columns_to_aggregate : List[str]
            List of columns that should be run by with aggregate functions

        by_column : str
            Column to group the in_df by 
        """
        self.in_df = in_df
        self.columns_to_aggregate = columns_to_aggregate
        self.by_column = by_column
        self.name = in_df.internal_state.get_state()
    
    def count(self):
        """
        Aggregates all columns with COUNT method. 
        Counts all non null values in column with respect to grouped classes

        Returns
        -------
        IdaDataFrame
            Object with grouped data
        """     
        self.aggregation_method = "COUNT"
        return self._execute_query()
    
    def sum(self):
        """
        Aggregates all columns with SUM method.
        Sums all the values in column with respect to grouped classes

        Returns
        -------
        IdaDataFrame
            Object with grouped data
        """   
        self.aggregation_method = "SUM"
        return self._execute_query()

    def min(self):
        """
        Aggregates all columns with MIN method.
        Selects the minimum value from column with respect to grouped classes

        Returns
        -------
        IdaDataFrame
            Object with grouped data
        """   
        self.aggregation_method = "MIN"
        return self._execute_query()
    
    def max(self):
        """
        Aggregates all columns with MAX method.
        Selects the maximum value from column with respect to grouped classes

        Returns
        -------
        IdaDataFrame
            Object with grouped data
        """   
        self.aggregation_method = "MAX"
        return self._execute_query()
    
    def mean(self):
        """
        Aggregates all columns with AVG method.
        Calculates mean of values in column with respect to grouped classes

        Returns
        -------
        IdaDataFrame
            Object with grouped data
        """   
        self.aggregation_method = "AVG"
        return self._execute_query()
    
    def _execute_query(self):
        select_string = f'{self.aggregation_method}(' + \
            f'), {self.aggregation_method}('.join(self.columns_to_aggregate) + \
                ')'
        for col in self.columns_to_aggregate:
            select_string = select_string.replace(
                f'{self.aggregation_method}({col})', 
                f'{self.aggregation_method}(\"{col}\") AS \"{self.aggregation_method}_{col}\"')
            
        groupby_string = f'GROUP BY \"{self.by_column}\"'
        query = 'SELECT ' + select_string + f', \"{self.by_column}\"' + \
            f' FROM ({self.name}) AS TEMP_GB ' + groupby_string

        idadf=nzpyida.IdaGeoDataFrame(self.in_df._idadb, self.in_df.tablename)
        idadf.internal_state._views.append(query)
        return idadf
        