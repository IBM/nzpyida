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
"""
This module contains a class that is the base for all classification algorithms.
"""
from typing import Tuple
from nzpyida.frame import IdaDataFrame
from nzpyida.base import IdaDataBase
from nzpyida.analytics.utils import map_to_props, materialize_df, make_temp_table_name, \
call_proc_df_in_out
from nzpyida.analytics.utils import get_auto_delete_context
from nzpyida.analytics.predictive.predictive_modeling import PredictiveModeling
from nzpyida.analytics.model_manager import ModelManager
from nzpyida.analytics.utils import q


class Classification(PredictiveModeling):
    """
    Base class for classification algorithms.
    """

    def __init__(self, idadb: IdaDataBase, model_name: str):
        """
        Creates the classifier class.

        Parameters
        ----------

        idada : IdaDataBase
            database connector

        model_name : str
            model name - if it exists in the database, it will be used, otherwise
            it must be trained using fit() function before prediction or scoring is called.
        """

        super().__init__(idadb, model_name)
        self.target_column_in_output = idadb.to_def_case('CLASS')
        self.id_column_in_output = idadb.to_def_case('ID')
        self.score_proc = 'CERROR'
        self.score_inv = True
        self.type = None

    def predict(self, in_df: IdaDataFrame, out_table: str=None,
        id_column: str=None) -> IdaDataFrame:
        """
        Makes predictions based on this model. The model must exist.

        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame for predictions

        out_table : str, optional
            the output table where the predictions will be stored

        id_column : str, optional
            the input table column identifying a unique instance id
            Default: id column used to build the model
        """

        params = {
            'id': q(id_column)
        }

        return self._predict(in_df=in_df, params=params, out_table=out_table)

    def score(self, in_df: IdaDataFrame, target_column: str, id_column: str=None) -> float:
        """
        Scores the model. The model must exist.

        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame for scoring

        target_column : str
            the input table column representing the class

        id_column : str, optional
            the input table column identifying a unique instance id - if skipped, 
            the input data frame indexer must be set and will be used as an instance id

        Returns
        -------
        float
            the model score
        """

        params = {
            'id': q(id_column)
        }

        return self._score(in_df=in_df, predict_params=params, target_column=target_column)

    def conf_matrix(self, in_df: IdaDataFrame, target_column: str, id_column: str=None,
        out_matrix_table: str=None) -> Tuple[IdaDataFrame, float, float]:
        """
        Makes a predition for a test data set given by the user and returns a confusion matrix,
        together with other stats (ACC and WACC).

        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame for scoring

        target_column : str
            the input table column representing the class

        id_column : str, optional
            the input table column identifying a unique instance id - if skipped, 
            the input data frame indexer must be set and will be used as an instance id

        out_matrix_table : str, optional
            the output table where the confidence matrix will be stored

        Returns
        -------
        IdaDataFrame
            the confidence matrix data frame

        float
            classification accuracy (ACC)

        float
            weighted classification accuracy (WACC)
        """

        params = {
            'id': q(id_column),
            'target': q(target_column)
        }
        return self._conf_matrix(in_df, out_matrix_table, params)
        
    def _conf_matrix(self, in_df: IdaDataFrame, out_matrix_table: str=None,
                     params: dict={}) -> Tuple[IdaDataFrame, float, float]:

        if not isinstance(in_df, IdaDataFrame):
            raise TypeError("Argument in_df should be an IdaDataFrame")

        if not params.get('id'):
            if in_df.indexer:
                params['id'] = q(in_df.indexer)
            else:
                raise TypeError('Missing id column - either use id_column attribute or set '
                    'indexer column in the input data frame')

        out_table = make_temp_table_name()

        pred_view_needs_delete, true_view_needs_delete = False, False
        try:
            pred_df = self._predict(in_df=in_df, out_table=out_table, params=params)
            pred_view, pred_view_needs_delete = materialize_df(pred_df)
            true_view, true_view_needs_delete = materialize_df(in_df)

            auto_delete_context = None
            if not out_matrix_table:
                auto_delete_context = get_auto_delete_context('out_matrix_table')
                out_matrix_table = make_temp_table_name()

            params_s = map_to_props({
                'resulttable': pred_view,
                'intable': true_view,
                'resultid': self.idadb.to_def_case('ID'),
                'id': params['id'],
                'resulttarget': self.idadb.to_def_case('CLASS'),
                'target': params['target'],
                'matrixTable': out_matrix_table
            })
            self.idadb.ida_query(f'call NZA..CONFUSION_MATRIX(\'{params_s}\')')

            if auto_delete_context:
                auto_delete_context.add_table_to_delete(out_matrix_table)

            out_df = IdaDataFrame(self.idadb, out_matrix_table)

            params = map_to_props({
                'matrixTable': out_matrix_table
            })

            res_acc = self.idadb.ida_query(f'call NZA..CMATRIX_ACC(\'{params}\')')
            res_wacc = self.idadb.ida_query(f'call NZA..CMATRIX_WACC(\'{params}\')')

            return out_df, res_acc[0], res_wacc[0]

        finally:
            self.idadb.drop_table(out_table)
            if pred_view_needs_delete:
                self.idadb.drop_view(pred_view)
            if true_view_needs_delete:
                self.idadb.drop_view(true_view)
    
    def cross_validation(self, in_df: IdaDataFrame, target_column: str,  
                         id_column: str=None, out_table: str=None, folds: int=10, 
                         rand_seed: float=None) -> Tuple[IdaDataFrame, float]:
        """
        Performs a cross validation on <in_df> data for given model. Numer of batches 
        and size of train/test split isdetermined by parameter <folds>

        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame for scoring
        
        target_column : str
            the input table column representing the class

        id_column : str, optional
            the input table column identifying a unique instance id - if skipped, 
            the input data frame indexer must be set and will be used as an instance id

        out_table : str, optional
            the output table where the predicted values will be stored

        Returns
        -------
        IdaDataFrame
            the data frame with predicted values for all <in_df>

        float
            classification accuracy (ACC) for all batches 
        """
        params = {
            'modelType': self.fit_proc,
            'model': self.model_name,
            'intable': in_df,
            'id': q(id_column),
            'target': q(target_column),
            'outtable': out_table,
            'folds': folds,
        }

        if not params.get('id'):
            if in_df.indexer:
                params['id'] = q(in_df.indexer)
            else:
                raise TypeError('Missing id column - either use id_column attribute or set '
                    'indexer column in the input data frame')

        if isinstance(rand_seed, int):
            params['seed'] = rand_seed
        
        ModelManager(self.idadb).drop_model(self.model_name)
        
        ret_df, ret_acc = call_proc_df_in_out(proc="CROSS_VALIDATION", in_df=in_df, params=params,
                                   out_table=out_table)
        
        return ret_df, ret_acc[0]

