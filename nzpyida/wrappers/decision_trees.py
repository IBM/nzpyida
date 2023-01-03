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

from nzpyida.frame import IdaDataFrame
from nzpyida.wrappers.utils import map_to_props

class DecisionTreeClassifier:

    def __init__(self, model_name: str):
        self.model_name = model_name
        self.temp_out_dfs = []

    def fit(self, in_df, id: str, target: str, in_column: str=None,  
        col_def_type: str=None, col_def_role: str=None, col_properties_table: str=None, 
        weights: str=None, eval: str=None, min_improve: float=0.02, min_split: int=50, 
        max_depth: int=10, val_table: str=None, val_weights: str=None, qmeasure: str=None, 
        statistics: str=None):
        
        temp_view_name = in_df._idadb._get_valid_tablename()
        in_df.internal_state._create_view(viewname=temp_view_name)
        
        params = map_to_props({
            'model': self.model_name,
            'id': id,
            'target': target,
            'incolumn': in_column,
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colpropertiestable': col_properties_table,
            'weights': weights,
            'eval': eval,
            'minimprove': min_improve,
            'minsplit': min_split,
            'maxdepth': max_depth,
            'valtable': val_table,
            'valweights': val_weights,
            'qmeasure': qmeasure,
            'statistics': statistics,
            'intable': temp_view_name
        })
        
        try:
            in_df.ida_query('call NZA..DECTREE(\'{}\')'.format(params))
        finally:
            in_df.internal_state._delete_view(viewname=temp_view_name)

    def predict(self, in_df, out_table: str=None, id: str=None, target: str=None, 
        prob: bool=False, out_table_prob: str=None):
        
        temp_view_name = in_df._idadb._get_valid_tablename()
        in_df.internal_state._create_view(viewname=temp_view_name)

        using_temp_out_table = False
        if not out_table:
            using_temp_out_table = True
            out_table = in_df._idadb._get_valid_tablename()

        params = map_to_props({
            'model': self.model_name,
            'outtable': out_table, 
            'id': id, 
            'target': target, 
            'prob': prob,
            'outtableprob': out_table_prob,
            'intable': temp_view_name
        })

        try:
            in_df.ida_query('call NZA..PREDICT_DECTREE(\'{}\')'.format(params))
        finally:
            in_df.internal_state._delete_view(viewname=temp_view_name)

        out_df = IdaDataFrame(in_df._idadb, out_table)

        if using_temp_out_table:
            self.temp_out_dfs.append((out_df, out_table))

        return out_df

    def clean_up(self):
        for out_df, table_name in self.temp_out_dfs:
            out_df._idadb.drop_table(table_name)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.clean_up()

