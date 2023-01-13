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

class KMeans:
    """
    KMeans clustering.
    """

    def __init__(self, idadb: IdaDataBase, model_name: str):
        self.idadb = idadb
        self.model_name = model_name

    def fit(self, in_df: IdaDataFrame, id_column: str, target_column: str, in_column: str=None,
        col_def_type: str=None, col_def_role: str=None, col_properties_table: str=None, out_table: str=None,
        distance: str='norm_euclidean', k: int=3, max_iter: int=5, rand_seed: int=12345,
        id_based: bool=False, statistics: str=None, transform: str='L') -> IdaDataFrame:
        """
        Create a model for clustering based on provided data and store it in a database.
        """
        
        ModelManager(self.idadb).drop_model(self.model_name)

        temp_view_name, need_delete = materialize_df(in_df)

        auto_delete_context = None
        if not out_table:
            auto_delete_context = get_auto_delete_context('out_table')
            out_table = make_temp_table_name()

        params = map_to_props({
            'model': self.model_name,
            'id': id_column,
            'target': target_column,
            'incolumn': in_column,
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colpropertiestable': col_properties_table,
            'distance': distance,
            'k': k,
            'maxiter': max_iter,
            'randseed': rand_seed,
            'idbased': id_based,
            'statistics': statistics,
            'transform': transform,
            'intable': temp_view_name,
            'outtable': out_table
        })
        
        try:
            self.idadb.ida_query(f'call NZA..KMEANS(\'{params}\')')
        finally:
            if need_delete:
                self.idadb.drop_view(temp_view_name)

        if auto_delete_context:
            auto_delete_context.add_table_to_delete(out_table)

        return IdaDataFrame(self.idadb, out_table)

 
    def predict(self, in_df: IdaDataFrame, out_table: str=None, id_column: str=None) -> IdaDataFrame:
        """
        Make predictions based on this model. The model must exist.
        """
        
        temp_view_name, need_delete = materialize_df(in_df)

        auto_delete_context = None
        if not out_table:
            auto_delete_context = get_auto_delete_context('out_table')
            out_table = make_temp_table_name()

        params = map_to_props({
            'model': self.model_name,
            'outtable': out_table,
            'id': id_column,
            'intable': temp_view_name
        })

        try:
            self.idadb.ida_query(f'call NZA..PREDICT_KMEANS(\'{params}\')')
        finally:
            if need_delete:
                self.idadb.drop_view(temp_view_name)

        out_df = IdaDataFrame(self.idadb, out_table)

        if auto_delete_context:
            auto_delete_context.add_table_to_delete(out_table)

        return out_df

    def score(self, in_df: IdaDataFrame, id_column: str, target_column: str) -> float:
        """
        Scores the model. The model must exist.
        """

        out_table = make_temp_table_name()

        pred_view_needs_delete, true_view_needs_delete = False, False
        try:
            pred_df = self.predict(in_df=in_df, out_table=out_table, id_column=id_column)

            pred_view, pred_view_needs_delete = materialize_df(pred_df)
            true_view, true_view_needs_delete = materialize_df(in_df)

            params = map_to_props({
                'pred_table': pred_view,
                'true_table': true_view,
                'pred_id': 'ID',
                'true_id': id_column,
                'pred_column': 'CLUSTER_ID',
                'true_column': target_column
            })

            res = pred_df.ida_query(f'call NZA..MSE(\'{params}\')')
            return res[0]
        finally:
            self.idadb.drop_table(out_table)
            if pred_view_needs_delete:
                self.idadb.drop_view(pred_view)
            if true_view_needs_delete:
                self.idadb.drop_view(true_view)

    def __str__(self):
        params = map_to_props({'model': self.model_name})
        return self.idadb.ida_query(f'call NZA..PRINT_KMEANS(\'{params}\')')[0]