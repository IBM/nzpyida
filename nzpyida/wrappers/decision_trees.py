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
from typing import Tuple
from nzpyida.frame import IdaDataFrame
from nzpyida.base import IdaDataBase
from nzpyida.wrappers.utils import map_to_props, materialize_df, make_temp_table_name
from nzpyida.wrappers.utils import get_auto_delete_context
from nzpyida.wrappers.classification import Classification


class DecisionTreeClassifier(Classification):
    """
    Decision tree based classifier.
    """

    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
        self.fit_proc = 'GROW_DECTREE'
        self.predict_proc = 'PREDICT_DECTREE'
        self.score_proc = 'CERROR'
        self.score_inv = True
        self.target_column_in_output = 'CLASS'
        self.id_column_in_output = 'ID'

    def fit(self, in_df: IdaDataFrame, id_column: str, target_column: str, in_column: str=None,
        col_def_type: str=None, col_def_role: str=None, col_properties_table: str=None,
        weights: str=None, eval_measure: str=None, min_improve: float=0.02, min_split: int=50,
        max_depth: int=10, val_table: str=None, val_weights: str=None, qmeasure: str=None,
        statistics: str=None):
        """
        Grows the decision tree and stores its model in the database.
        """
        
        params = {
            'id': id_column,
            'target': target_column,
            'incolumn': in_column,
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colpropertiestable': col_properties_table,
            'weights': weights,
            'eval': eval_measure,
            'minimprove': min_improve,
            'minsplit': min_split,
            'maxdepth': max_depth,
            'valtable': val_table,
            'valweights': val_weights,
            'qmeasure': qmeasure,
            'statistics': statistics
        }

        self._fit(in_df=in_df, params=params)

    def predict(self, in_df: IdaDataFrame, out_table: str=None, id_column: str=None, target_column: str=None, 
        prob: bool=False, out_table_prob: str=None) -> IdaDataFrame:
        """
        Makes predictions based on the decision tree model. The model must exist.
        """
        
        params = {
            'id': id_column,
            'target': target_column,
            'prob': prob,
            'outtableprob': out_table_prob
        }

        return self._predict(in_df=in_df, params=params, out_table=out_table)
