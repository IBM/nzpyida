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
from nzpyida.wrappers.utils import map_to_props, materialize_df
from nzpyida.wrappers.model_manager import ModelManager

class DecisionTreeClassifier:
    """
    Decision tree based classifier.
    """

    def __init__(self, idadb: IdaDataBase, model_name: str):
        self.idadb = idadb
        self.model_name = model_name
        self.temp_out_tables = []

    def fit(self, in_df: IdaDataFrame, id: str, target: str, in_column: str=None,
        col_def_type: str=None, col_def_role: str=None, col_properties_table: str=None,
        weights: str=None, eval: str=None, min_improve: float=0.02, min_split: int=50,
        max_depth: int=10, val_table: str=None, val_weights: str=None, qmeasure: str=None,
        statistics: str=None):
        """
        Grows the decision tree and stores its model in the database.
        """
        
        ModelManager(self.idadb).drop_model(self.model_name)

        temp_view_name, need_delete = materialize_df(in_df)

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
            self.idadb.ida_query(f'call NZA..DECTREE(\'{params}\')')
        finally:
            if need_delete:
                self.idadb.drop_view(temp_view_name)

    def predict(self, in_df: IdaDataFrame, out_table: str=None, id: str=None, target: str=None, 
        prob: bool=False, out_table_prob: str=None) -> IdaDataFrame:
        """
        Make predictions based on the decision tree model. The model must exist.
        """
        
        temp_view_name, need_delete = materialize_df(in_df)

        using_temp_out_table = False
        if not out_table:
            using_temp_out_table = True
            out_table = self.idadb._get_valid_tablename()

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
            self.idadb.ida_query(f'call NZA..PREDICT_DECTREE(\'{params}\')')
        finally:
            if need_delete:
                self.idadb.drop_view(temp_view_name)

        out_df = IdaDataFrame(self.idadb, out_table)

        if using_temp_out_table:
            self.temp_out_tables.append(out_table)

        return out_df

    def score(self, in_df: IdaDataFrame, id: str=None, target: str=None) -> float:
        """
        Scores the decision tree model. The model must exist.
        """

        out_table = self.idadb._get_valid_tablename()
        
        pred_view_needs_delete, true_view_needs_delete = False, False
        try:
            pred_df = self.predict(in_df=in_df, out_table=out_table, id=id)

            pred_view, pred_view_needs_delete = materialize_df(pred_df)
            true_view, true_view_needs_delete = materialize_df(in_df)

            params = map_to_props({
                'pred_table': pred_view,
                'true_table': true_view,
                'pred_id': 'ID',
                'true_id': id,
                'pred_column': 'CLASS',
                'true_column': target
            })

            res = pred_df.ida_query(f'call NZA..CERROR(\'{params}\')')
            return 1-res[0]
        finally:
            self.idadb.drop_table(out_table)
            if pred_view_needs_delete:
                self.idadb.drop_view(pred_view)
            if true_view_needs_delete:
                self.idadb.drop_view(true_view)

    def clean_up(self):
        """
        Cleans up temporary tables created for predictions made using this model.
        """
        for table_name in self.temp_out_tables:
            self.idadb.drop_table(table_name)

    def __str__(self):
        params = map_to_props({'model': self.model_name})
        return self.idadb.ida_query(f'call NZA..PRINT_DECTREE(\'{params}\')')[0]

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.clean_up()
