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

from nzpyida.analytics.utils import call_proc_df_in_out, make_temp_table_name, out_str_to_df
from nzpyida.frame import IdaDataFrame
from nzpyida.base import IdaDataBase
from typing import List
import pandas as pd
from nzpyida.analytics.predictive.regression import Regression
from nzpyida.analytics.utils import q


class GLM(Regression):
    """ 
    General Linear Regression model
    """
    def __init__(self, idadb: IdaDataBase, model_name: str):
        """
        Creates GLM object

        Parameters
        ----------

        idada : IdaDataBase
            database connector

        model_name : str
            model name - if it exists in the database, it will be used, otherwise
            it must be trained using fit() function before prediction or scoring is called.
        """
        super().__init__(idadb, model_name)
        self.fit_proc = "GLM"
        self.predict_proc = "PREDICT_GLM"
        self.has_print_proc = True
        self.target_column_in_output = idadb.to_def_case('PRED')
        self.id_column_in_output = None

    
    def fit(self, in_df: IdaDataFrame, target_column: str, id_column: str=None, in_columns: List[str]=None,  
            intercept: bool=True, interaction: str='', family_param: float=-1, link: str='logit', 
            link_param: float=1, max_iter: int=20, epsilon: float=1e-3, tolerance: float=1e-7, 
            method: str='irls', trials: str='', debug: bool=False, col_def_type: str=None, 
            col_def_role: str=None, col_properties_table: str=None):
        """
        in_df : IdaDataFrame
            the input data frame
        
        target_column : str
            the input dataframe column to predict a value for. Only numeric type of target column is
            accepted
        
        id_column : str, optional
            the input datafrme column identifying a unique instance id
        
        incolumn : str, optional
            the list of input dataframe columns with special properties, separated. Each column is 
            followed by one or several of the following properties:
            - its type: ':nom' (for nominal), ':cont' (for continuous). Per default, all numerical 
              types are con-tinuous, other types are nominal.
            - its role: ':id', ':target', ':input', ':ignore', ':objweight'.
            (Remark: ':colweight(<wgt>)' is unsupported, i.e. ':colweight(<wgt>)' same as 
            ':colweight(1)' same as ':input').
            If the parameter is undefined, all columns of the input table have default properties
        
        intercept: bool, optional
            flag indicating whether the model is built with or without an intercept value
        
        interaction: str, optional
            the definition of the allowed interactions between input columns. The interaction 
            is a list of factors separated by a semicolon (;). A factor is a list of variables 
            separated by a star (*). A variable is a column name of the input table. Continuous 
            variables can be followed by a caret (^) and a numeric value, in this case the given 
            power of values of this column is meant. Nominal variables can be followed by a sign 
            equal (=) and a value, so that only the given value of the variable is allowed to 
            interact with the other variables of this factor. If no value is indicated after 
            a nominal variable, all distinct val- ues interact independantly with the other 
            variables of the factor. By default, all input columns are considered independent 
            and do not interact with each other
        
        family_param: float, optional
            additional parameter used for some distributions. IF family_param='quasi' then 
            quasi-likelihood in case of Poisson and Binomial distributions is optimized. 
            IF family_param=-1 (or is omitted then mentioned distribution parameter is estimated 
            from data. IF family_param is given explicit then should by > 0
        
        link: str, optional
            the type of the link function. Allowed values are: canbinom, cangeom, cannegbinom, 
            cauchit, clog, cloglog, gaussit, identity, inverse, invnegative, invsquare, log, logit, 
            loglog, oddspower, power, probit, sqrt
        
        link_param: float, optional
            an additional parameter used for some links like: cannegbinom, oddspower, power. 
            The range of value depends on the used link function
        
        max_iter: int, optional
            the maximum number of iterations
        
        epsilon: float, optional
            the maximum (relative) error used as stopping criteria
        
        tolerance: float, optional
            the tolerance for the linear equation solver when to consider a value to be equal to zero
        
        method: str, optional
            the method used to calculate a GLM model. Allowed values are: irls, psgd
        
        trials: str, optional
            the input table column containing the number of trials for the binominal distribution. 
            This parameter must be specified when family=binomial. 
            This parametrs is ignored for other distributions
        
        debug: str, optional
            flag indicating to display debug information
        
        col_def_type: str, optional
            default type of the input dataframe columns. Allowed values are 'nom' and 'cont'.
            If the parameter is undefined, all numeric columns are considered continuous, 
            other columns nominal
        
        col_def_role: str, optional
            default role of the input dataframe columns. Allowed values are 'input' and 'ignore'. 
            If the parameter is undefined, all columns are considered 'input' columns

        col_properties_table: str, optional
            the input table where column properties for the input dataframe columns are stored. 
            The format of this table is the output format of stored procedure nza..COLUMN_PROPER-TIES().
            If the parameter is undefined, the input table column properties will be detected automatically.
            (Remark: colPropertiesTable with "COLWEIGHT" column with value '<wgt>' is unsupported, i.e. same as '1')
        """
        params = {
            'family': self.family,
            'target': q(target_column),
            'id': q(id_column),
            'incolumn': q(in_columns),
            'coldefrole': col_def_role,
            'coldeftype': col_def_type,
            'colPropertiesTable': col_properties_table,
            'intercept': intercept,
            'family_param': family_param,
            'link': link,
            'link_param': link_param,
            'maxit': max_iter,
            'eps': epsilon,
            'tol': tolerance,
            'method': method,
            'debug': debug
        }
        if interaction:
            params['interaction'] = interaction
        if trials:
            params['trials'] = q(trials)

        return self._fit(in_df=in_df, params=params)

    def predict(self, in_df: IdaDataFrame, out_table: str=None, id_column: str=None, 
                debug: bool=False):
        """
        in_df : IdaDataFrame
            the input data frame
        
        out_table : str, optional
            the output table where the predictions will be stored
        
        id_column : str, optional
            the input data frame column identifying a unique instance
        
        debug : bool, optional
            flag indicating to display debug information
        
        Returns
        -------
        IdaDataFrame
            the data frame containing row identifiers and predicted target values
        """
        params = {
            'id': q(id_column),
            'debug': debug
        }

        return self._predict(in_df=in_df, params=params, out_table=out_table)

class BernoulliRegressor(GLM):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
        self.family = 'bernoulli'


class BinomialRegressor(GLM):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
        self.family = 'binomial'

class PoissonRegressor(GLM):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
        self.family = 'poisson'

class NegativeBinomialRegressor(GLM):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
        self.family = 'negativebinomial,'

class GaussianRegressor(GLM):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
        self.family = 'gaussian'

class WaldRegressor(GLM):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
        self.family = 'wald'

class GammaRegressor(GLM):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
        self.family = 'gamma'

