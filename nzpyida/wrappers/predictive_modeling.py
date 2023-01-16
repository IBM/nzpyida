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
from nzpyida.wrappers.utils import map_to_props, materialize_df, make_temp_table_name
from nzpyida.wrappers.utils import get_auto_delete_context
from nzpyida.wrappers.model_manager import ModelManager


class PredictiveModeling:
    """
    Generic class for predictove modeling algorithms.
    """

    def __init__(self, idadb: IdaDataBase, model_name: str):
        self.idadb = idadb
        self.model_name = model_name
        self.fit_proc = ''
        self.predict_proc = ''
        self.score_proc = ''
        self.score_inv = False
        self.target_column_in_output = None
        self.id_column_in_output = None

    def _fit(self, in_df: IdaDataFrame, params:dict):
        """
        Trains the model.
        """
        
        ModelManager(self.idadb).drop_model(self.model_name)

        temp_view_name, need_delete = materialize_df(in_df)

        params['model'] = self.model_name
        params['intable'] = temp_view_name
        params_s = map_to_props(params)
        
        try:
            self.idadb.ida_query(f'call NZA..{self.fit_proc}(\'{params_s}\')')
        finally:
            if need_delete:
                self.idadb.drop_view(temp_view_name)

    def _predict(self, in_df: IdaDataFrame, params:dict, out_table: str=None) -> IdaDataFrame:
        """
        Makes predictions based on the model. The model must exist.
        """
        
        temp_view_name, need_delete = materialize_df(in_df)

        auto_delete_context = None
        if not out_table:
            auto_delete_context = get_auto_delete_context('out_table')
            out_table = make_temp_table_name()

        params['model'] = self.model_name
        params['intable'] = temp_view_name
        params['outtable'] = out_table
        params_s = map_to_props(params)
        
        try:
            self.idadb.ida_query(f'call NZA..{self.predict_proc}(\'{params_s}\')')
        finally:
            if need_delete:
                self.idadb.drop_view(temp_view_name)

        out_df = IdaDataFrame(self.idadb, out_table)

        if auto_delete_context:
            auto_delete_context.add_table_to_delete(out_table)

        return out_df

    def _score(self, in_df: IdaDataFrame, id_column: str, target_column: str) -> float:
        """
        Scores the model. The model must exist.
        """

        out_table = make_temp_table_name()

        pred_view_needs_delete, true_view_needs_delete = False, False
        try:

            params = {
                'id': id_column
            }

            pred_df = self._predict(in_df=in_df, params=params, out_table=out_table)

            pred_view, pred_view_needs_delete = materialize_df(pred_df)
            true_view, true_view_needs_delete = materialize_df(in_df)

            params = map_to_props({
                'pred_table': pred_view,
                'true_table': true_view,
                'pred_id': id_column if self.id_column_in_output is None else self.id_column_in_output,
                'true_id': id_column,
                'pred_column': target_column if self.target_column_in_output is None else self.target_column_in_output,
                'true_column': target_column
            })

            res = pred_df.ida_query(f'call NZA..{self.score_proc}(\'{params}\')')
            return 1-res[0] if self.score_inv else res[0]
        finally:
            self.idadb.drop_table(out_table)
            if pred_view_needs_delete:
                self.idadb.drop_view(pred_view)
            if true_view_needs_delete:
                self.idadb.drop_view(true_view)
