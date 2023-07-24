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
    """
    def __init__(self, in_df, columns_to_aggregate: list, by_column: str) -> None:
        self.in_df = in_df
        self.columns_to_aggregate = columns_to_aggregate
        self.by_column = by_column
        self.name = in_df.internal_state.current_state
    
    def count(self):
        self.aggregation_method = "COUNT"
        return self._execute_query()
    
    def sum(self):
        self.aggregation_method = "SUM"
        return self._execute_query()

    def min(self):
        self.aggregation_method = "MIN"
        return self._execute_query()
    
    def max(self):
        self.aggregation_method = "MAX"
        return self._execute_query()
    
    def mean(self):
        self.aggregation_method = "AVG"
        return self._execute_query()
    
    def _execute_query(self):
        select_string = f"{self.aggregation_method}(" + \
            f"), {self.aggregation_method}(".join(self.columns_to_aggregate) + \
                ")"
        for col in self.columns_to_aggregate:
            select_string = select_string.replace(
                f"{self.aggregation_method}({col})", 
                f"{self.aggregation_method}({col}) AS {self.aggregation_method}_{col}")
            
        groupby_string = f"GROUP BY {self.by_column}"
        query = "SELECT " + select_string + f", {self.by_column}" + \
            f" FROM {self.name} " + groupby_string
        
        viewname=self.in_df._idadb._create_view_from_expression(query)
        idageodf=nzpyida.IdaGeoDataFrame(self.in_df._idadb, viewname)
        return idageodf
        