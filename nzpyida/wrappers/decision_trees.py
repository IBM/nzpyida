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
from nzpyida.wrappers.predictive_modeling import PredictiveModeling


class DecisionTreeClassifier(PredictiveModeling):
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

    def score(self, in_df: IdaDataFrame, id_column: str, target_column: str) -> float:
        """
        Scores the model. The model must exist.
        """

        return self._score(in_df=in_df, id_column=id_column, target_column=target_column)

    def conf_matrix(self, in_df: IdaDataFrame, id_column: str, target_column: str, 
        out_matrix_table: str=None) -> Tuple[IdaDataFrame, float, float]:
        """
        Makes a predition for a test data set given by the user and returns a confusion matrix,
        together with other stats (ACC and WACC).
        """
        out_table = make_temp_table_name()

        pred_view_needs_delete, true_view_needs_delete = False, False
        try:
            pred_df = self.predict(in_df=in_df, out_table=out_table, id_column=id_column)

            pred_view, pred_view_needs_delete = materialize_df(pred_df)
            true_view, true_view_needs_delete = materialize_df(in_df)

            auto_delete_context = None
            if not out_matrix_table:
                auto_delete_context = get_auto_delete_context('out_table')
                out_matrix_table = make_temp_table_name()

            params = map_to_props({
                'resulttable': pred_view,
                'intable': true_view,
                'resultid': 'ID',
                'id': id_column,
                'resulttarget': 'CLASS',
                'target': target_column,
                'matrixTable': out_matrix_table
            })
            pred_df.ida_query(f'call NZA..CONFUSION_MATRIX(\'{params}\')')

            if auto_delete_context:
                auto_delete_context.add_table_to_delete(out_matrix_table)

            out_df = IdaDataFrame(self.idadb, out_matrix_table)

            params = map_to_props({
                'matrixTable': out_matrix_table
            })

            res_acc = pred_df.ida_query(f'call NZA..CMATRIX_ACC(\'{params}\')')
            res_wacc = pred_df.ida_query(f'call NZA..CMATRIX_WACC(\'{params}\')')

            return out_df, res_acc[0], res_wacc[0]

        finally:
            self.idadb.drop_table(out_table)
            if pred_view_needs_delete:
                self.idadb.drop_view(pred_view)
            if true_view_needs_delete:
                self.idadb.drop_view(true_view)

    def __str__(self):
        params = map_to_props({'model': self.model_name})
        return self.idadb.ida_query(f'call NZA..PRINT_DECTREE(\'{params}\')')[0]