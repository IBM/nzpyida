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

from nzpyida.wrappers.predictive_modeling_regression import PredictiveModelingRegression


class LinearRegression(PredictiveModelingRegression):
    """
    Linear regression predictive model.
    """

    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
        self.fit_proc = 'LINEAR_REGRESSION'
        self.predict_proc = 'PREDICT_LINEAR_REGRESSION'


    def fit(self, in_df: IdaDataFrame, id_column: str, target_column: str, in_column: str=None,
        col_def_type: str=None, col_def_role: str=None, col_properties_table: str=None,
        use_svd_solver: bool=False, intercept: bool=True, calculate_diagnostics: bool=False):
        """
        Creates a linear regression model based on provided data and store it in a database.
        """

        params = {
            'id': id_column,
            'target': target_column,
            'incolumn': in_column,
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colpropertiestable': col_properties_table,
            'useSVDSolver': use_svd_solver,
            'intercept': intercept,
            'calculateDiagnostics': calculate_diagnostics
        }
        
        self._fit(in_df=in_df, params=params)
