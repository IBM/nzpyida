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


class KNeighborsClassifier(Classification):
    """
    K-neighbors based classifier.
    """

    def __init__(self, idadb: IdaDataBase, model_name: str):
        """
        Creates the classifier class.

        :param idada: database connector
        :param model_name: model name - if it exists in the database, it will be used, otherwise
        it must be trained using fit() function before prediction or scoring is called.
        """
        super().__init__(idadb, model_name)
        self.fit_proc = 'KNN'
        self.predict_proc = 'PREDICT_KNN'

    def fit(self, in_df: IdaDataFrame, id_column: str, target_column: str, in_column: str=None,
        col_def_type: str=None, col_def_role: str=None, col_properties_table: str=None):
        """
        Builds a K-Nearest Neighbors Classification or Regression model.
        
        :param in_df: the input data frame
        :param id_column: the input table column identifying a unique instance id
        :param target_column: the input table column representing the class
        :param in_column: the input table columns with special properties, separated by a semi-colon (;).
            Each column is followed by one or several of the following properties:
            its type: ':nom' (for nominal), ':cont' (for continuous).
                Per default, all numerical types are continuous, other types are nominal.
            its role: ':id', ':target', ':input', ':ignore'.
        :param col_def_type: default type of the input table columns. Allowed values are 'nom' and 'cont'.
            If the parameter is undefined, all numeric columns are considered continuous, other columns nominal.
        :param col_def_role: default role of the input table columns. Allowed values are 'input' and 'ignore'. If the parameter is undefined, all columns are considered 'input'
        :param col_properties_table: the input table where column properties for the input table columns are stored. If the parameter is undefined, the input table column properties will be detected automatically.
        """
        
        params = {
            'id': id_column,
            'target': target_column,
            'incolumn': in_column,
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colpropertiestable': col_properties_table,
        }

        self._fit(in_df=in_df, params=params)

    def predict(self, in_df: IdaDataFrame, out_table: str=None, id_column: str=None, target_column: str=None,
        distance: str='euclidean', k: int=3, stand: bool=True, fast: bool=True, weights: str=None) -> IdaDataFrame:
        """
        Applies a K-Nearest Neighbors model to generate classification or regression predictions for a data frame.
        
        :param in_df: the input data frame
        :param out_table: the output table where the predictions will be stored
        :param id_column: the input table column identifying a unique instance id
        :param target_column: the input table column representing the class
        :param distance: the distance function. Allowed values are: euclidean, manhatthan, canberra, maximum
        :param k: number of nearest neighbors to consider
        :param stand: flag indicating whether the measurements in the input table are standardized before calculating the distance
        :param fast: flag indicating that the algorithm used coresets based method
        :param weights: the input table containing optional class weights for the input table <target> column.
            The <weights> table is used only when the <target> column is not numeric. If the parameter is undefined, we assume that the weights are uniformly equal to 1.
            The <weights> table contains following columns:
                weight: a numeric column containing the class weight,
                class: a column to be joined with the <target> column of <intable>, defining class weights.
            For classes not occurring in this table, weights of 1 are assumed.

        :return: a data frame with id and predicted class

        """
        
        params = {
            'id': id_column,
            'target': target_column,
            'distance': distance,
            'k': k,
            'stand': stand,
            'fast': fast,
            'weights': weights
        }

        return self._predict(in_df=in_df, params=params, out_table=out_table)

    def score(self, in_df: IdaDataFrame, id_column: str, target_column: str, distance: str='euclidean',
        k: int=3, stand: bool=True, fast: bool=True, weights: str=None) -> float:
        """
        Scores the model and returns classification error ratio.

        :param in_df: the input data frame used to test the model
        :param id_column: the input table column identifying a unique instance id
        :param target_column: the input table column representing the class in the input data frame
        :param distance: the distance function. Allowed values are: euclidean, manhatthan, canberra, maximum
        :param k: number of nearest neighbors to consider
        :param stand: flag indicating whether the measurements in the input table are standardized before calculating the distance
        :param fast: flag indicating that the algorithm used coresets based method
        :param weights: the input table containing optional class weights for the input table <target> column.
            The <weights> table is used only when the <target> column is not numeric. If the parameter is undefined, we assume that the weights are uniformly equal to 1.
            The <weights> table contains following columns:
                weight: a numeric column containing the class weight,
                class: a column to be joined with the <target> column of <intable>, defining class weights.
            For classes not occurring in this table, weights of 1 are assumed.

        :return: model classification error ratio
        """

        params = {
            'id': id_column,
            'target': target_column,
            'distance': distance,
            'k': k,
            'stand': stand,
            'fast': fast,
            'weights': weights
        }

        return self._score(in_df=in_df, predict_params=params, target_column=target_column)
