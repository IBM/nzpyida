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
from nzpyida.wrappers.utils import map_to_props, make_temp_table_name
from nzpyida.wrappers.utils import get_auto_delete_context
from nzpyida.wrappers.predictive_modeling import PredictiveModeling


class KMeans(PredictiveModeling):
    """
    KMeans clustering.
    """

    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
        self.fit_proc = 'KMEANS'
        self.predict_proc = 'PREDICT_KMEANS'
        self.score_proc = 'MSE'
        self.target_column_in_output = 'CLUSTER_ID'
        self.id_column_in_output = 'ID'

    def fit(self, in_df: IdaDataFrame, id_column: str, target_column: str, in_column: str=None,
        col_def_type: str=None, col_def_role: str=None, col_properties_table: str=None, out_table: str=None,
        distance: str='norm_euclidean', k: int=3, max_iter: int=5, rand_seed: int=12345,
        id_based: bool=False, statistics: str=None, transform: str='L') -> IdaDataFrame:
        """
        Creates a model for clustering based on provided data and store it in a database.
        """
        
        auto_delete_context = None
        if not out_table:
            auto_delete_context = get_auto_delete_context('out_table')
            out_table = make_temp_table_name()
        
        params = {
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
            'outtable': out_table
        }
        
        self._fit(in_df=in_df, params=params)

        if auto_delete_context:
            auto_delete_context.add_table_to_delete(out_table)

        return IdaDataFrame(self.idadb, out_table)

 
    def predict(self, in_df: IdaDataFrame, out_table: str=None, id_column: str=None) -> IdaDataFrame:
        """
        Makes predictions based on this model. The model must exist.
        """
        
        params = {
            'id': id_column
        }

        return self._predict(in_df=in_df, params=params, out_table=out_table)

    def score(self, in_df: IdaDataFrame, id_column: str, target_column: str) -> float:
        """
        Scores the model. The model must exist.
        """

        params = {
            'id': id_column
        }

        return self._score(in_df=in_df, predict_params=params, target_column=target_column)

    def __str__(self):
        params = map_to_props({'model': self.model_name})
        return self.idadb.ida_query(f'call NZA..PRINT_KMEANS(\'{params}\')')[0]