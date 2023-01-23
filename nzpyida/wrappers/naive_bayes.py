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
from nzpyida.wrappers.classification import Classification

class NaiveBayesClassifier(Classification):
    """
    Naive Bayes classifier
    """
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
        self.fit_proc = 'NAIVEBAYES'
        self.predict_proc = 'PREDICT_NAIVEBAYES'
        self.target_column_in_output = "CLASS"
        self.id_column_in_output = 'ID'

    def fit(self, in_df: IdaDataFrame, id_column: str, target_column: str, in_column: str=None,
            col_def_type: str=None, col_def_role: str=None, col_properties_table: str=None, 
            disc: str=None, bins: int=10):
        
        params = {
            'id': id_column,
            'target': target_column,
            'incolumn': in_column,
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colpropertiestable': col_properties_table,
            'disc': disc,
            'bins': bins
        }

        self._fit(in_df=in_df, params=params)
    
    def predict(self, in_df: IdaDataFrame, out_table: str=None, id_column: str=None, target_column: str=None,
                out_table_prob: str=None, mestimation: str=None):
        
        params = {
            'id': id_column,
            'target': target_column,
            'outtableProb': out_table_prob,
            'mestimation': mestimation
            }

        return self._predict(in_df=in_df, params=params, out_table=out_table)

