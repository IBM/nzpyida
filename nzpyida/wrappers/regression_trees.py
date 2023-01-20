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
from nzpyida.base import IdaDataBase
from nzpyida.wrappers.predictive_modeling_regression import PredictiveModelingRegression
from nzpyida.wrappers.utils import map_to_props, materialize_df, make_temp_table_name

class DecisionTreeRegressor(PredictiveModelingRegression):
    """
    Decision tree based regressor
    """
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
        self.fit_proc = 'GROW_REGTREE'
        self.predict_proc = 'PREDICT_REGTREE'
        self.target_column_in_output = "CLASS"
        self.id_column_in_output = 'ID'


    def fit(self, in_df: IdaDataFrame, id_column: str, target_column: str, in_column: str=None, 
            col_def_type: str=None, col_def_role: str=None, col_properties_table: str=None, eval_measure: str=None,
            min_improve: float=0.1, min_split: int=50, max_depth: int=10, statistics: str=None):
        
        params = {
            'id': id_column,
            'target': target_column,
            'incolumn': in_column,
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colpropertiestable': col_properties_table,
            'eval': eval_measure,
            'minimprove': min_improve,
            'minsplit': min_split,
            'maxdepth': max_depth,
            'statistics': statistics
        }

        self._fit(in_df=in_df, params=params)

    def predict(self, in_df: IdaDataFrame, out_table: str=None, id_column: str=None, target_column: str=None,
                variance: bool=False):
        
        params = {
            'id': id_column,
            'target': target_column,
            'var': variance
            }

        return self._predict(in_df=in_df, params=params, out_table=out_table)
