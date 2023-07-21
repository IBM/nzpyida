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
Tree-shaped Bayesian networks formally belong to the data exploration category. 
However, this algorithm is considerably more complex than other data exploration 
algorithms and not as widely known, warranting detailed description.

A Bayesian network can be considered a graphical representation of probabilistically 
described relationships within a set of attributes, allowing probabilistic inference 
to be performed. The representation is created by extracting the structural properties 
of the distribution from the data.

Creating and using general Bayesian networks are algorithmically and computationally 
complex. Tree- shaped Bayesian networks, however, constitute a simplified subclass 
of Bayesian networks with restrictions imposed on the type of attribute relationships 
that can be discovered and represented. The restrictions permit simpler and more efficient 
algorithms as well as more straightforward interpretation. Tree-shaped Bayesian networks 
may be not sufficient for highly-accurate prediction, but provide an excellent 
qualitative description of the relationship structure observed in the data
"""

from typing import Tuple, List
from nzpyida.analytics.model_manager import ModelManager
from nzpyida.frame import IdaDataFrame
from nzpyida.base import IdaDataBase
from nzpyida.analytics.utils import map_to_props, materialize_df, make_temp_table_name
from nzpyida.analytics.utils import get_auto_delete_context, q
from nzpyida.analytics.predictive.regression import Regression
from nzpyida.analytics.predictive.predictive_modeling import PredictiveModeling


class TreeBayesNetwork(Regression):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        """
        Creates Tree Shaped Bayesian Network class

        Parameters
        ----------

        idada : IdaDataBase
            database connector

        model_name : str
            model name - if it exists in the database, it will be used, otherwise
            it must be trained using fit() function before prediction or scoring is called
        """
        super().__init__(idadb, model_name)
        self.fit_proc = "TBNET_GROW"
        self.predict_proc = "TBNET_APPLY"

    def fit(self, in_df: IdaDataFrame, in_columns: List[str]=None, base_index: int=777,
            sample_size: int=None, talk: str=None, size_warning: str=None, edge_lab_sort: str=None, col_def_type: str=None, 
            col_def_role: str=None, col_properties_table: str=None) -> None:
        """
        Builds a tree-like Bayesian Network for continuous variables. A spanning tree is 
        constructed joining all the variables on grounds of most strong correlations. 
        This gives the user an overview of most significant interrelations governing 
        the whole set of variables

        Parameters
        ----------

        in_df : IdaDataFrame
            the input data frame
        
        in_columns : List[str]
            List of the input dataframe  columns with special properties. 
            Each column is followed by one or several of the following properties:
            - type: ':nom' (for nominal), ':cont' (for continuous). By default, 
              all numerical types are continuous, other types are nominal
            - role: ':id', ':target', ':input', ':ignore'.
            (Remark: ':objweight' is unsupported, i.e. ':objweight' same as ':ignore').
            (Remark: ':colweight(<wgt>)' is unsupported, i.e. ':colweight(<wgt>)' same as 
            ':colweight(1)' same as ':input').

            If the parameter is undefined, all columns of the input dataframe have default properties. 
            Note that this procedure only accepts continuous columns with role 'input'
        
        base_index : int, optional
            the numeric id to be assigned to the first variable
        
        sample_size : int, optional
            the sample size to take if the number of records is too large
        
        talk : str, optional
            if talk=yes then additional information on progress will be displayed
        
        size_warning : str, optional
            if sizewarn=yes then no exception is thrown when there are less records than 
            3 times the number of columns. Instead, a notice is displayed and the stored 
            procedure returns 'sizewarn'
        
        edge_lab_sort : str, optional
            if edge_lab_sort=yes then the left end of the edge will have a name lower 
            in alphabetic order than the right one
        
        col_def_type : str, optional
            default type of the input dataaframe columns. Allowed values are 'nom' and 'cont'.
            If the parameter is undefined, all numeric columns are considered continuous,
            other columns nominal.

        col_def_role : str, optional
            default role of the input dataframe columns.
            Allowed values are 'input' and 'ignore'.
            If the parameter is undefined, all columns are considered 'input' columns.

        col_properties_table : str, optional
            the input table where column properties for the input dataframe columns are stored.
            The format of this table is the output format of stored procedure
            nza..COLUMN_PROPERTIES().
            If the parameter is undefined, the input table column properties will be
            detected automatically.
            (Remark: colPropertiesTable with "COLROLE" column with value 'objweight'
            is unsupported, i.e. same as 'ignore')
            (Remark: colPropertiesTable with "COLWEIGHT" column with value '<wgt>' is unsupported,
            i.e. same as '1')
        """
        params = {
            'incolumn': q(in_columns),
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colPropertiesTable': col_properties_table,
            'baseidx': base_index,
            'samplesize': sample_size,
            'talk': talk,
            'edgelabsort': edge_lab_sort
        }
        if self.fit_proc == 'TBNET_GROW':
            params['sizewarn'] = size_warning

        self._fit(in_df=in_df, params=params, needs_id=False)
    
    def predict(self, in_df: IdaDataFrame, target_column: str=None, id_column: str=None,
                prediction_type: str='best', out_table: str=None) -> IdaDataFrame:
        """
        Makes predictions based on this model. The model must exist.

        Parameters
        ----------

        in_df : IdaDataFrame
            the input data frame
        
        target_column : str
            The model variable to be predicted
        
        id_column : str, optional
            The column of the input dataframe that identifies a unique instance ID
        
        prediction_type : str, optional
            The type of prediction to be made. Valid values are best (most correlated neighbor), 
            neighbors (weighted prediction of neighbors), and nn-neighbors (non null neighbors)
        
        out_table : str, optional
            The name of the output dataframe where the predictions are to be stored
        
        Returns
        -------
        IdaDataFrame
            the data frame containing row identifiers and predicted target values
        """
        params = {
            'id': q(id_column),
            'target': q(target_column),
            'type': prediction_type
        }
        return self._predict(in_df, params, out_table)
    

    def score(self, in_df: IdaDataFrame, target_column: str=None, id_column: str=None,
              prediction_type: str='best') -> float:
        """
        Scores the model. The model must exist.

        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame for scoring

        target_column : str
            the input dataframe column representing the class

        id_column : str
            the input table column identifying a unique instance id - if skipped, 
            the input data frame indexer must be set and will be used as an instance id
        
        prediction_type : str, optional
            The type of prediction to be made. Valid values are best (most correlated neighbor), 
            neighbors (weighted prediction of neighbors), and nn-neighbors (non null neighbors)

        Returns
        -------
        float
            the model score
        """
        params = {
            'id': q(id_column),
            'type': prediction_type,
            'target': q(target_column)
        }
        self.target_column_in_output = f"{target_column}_PRED"
        return self._score(in_df, params, target_column)



class BinaryTreeBayesNetwork(TreeBayesNetwork):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        """
        Creates Binary Tree Shaped Bayesian Network class
        """
        super().__init__(idadb, model_name)
        self.fit_proc = "BTBNET_GROW"



class TreeAgumentedNetwork(TreeBayesNetwork):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        """
        Creates Tree-shaped Agumented Network object
        """
        super().__init__(idadb, model_name)
        self.fit_proc = "TANET_GROW"
        self.predict_proc = "TANET_APPLY"
    
    def fit(self, in_df: IdaDataFrame, in_model: str, class_column: str,
            edge_lab_sort: str=None) -> None:
        """
        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame
        
        in_model : str
            the name of the input Bayesian Network model

        class_column : str
            the target class; this should be column with nominal variables
        
        edge_lab_sort : str, optional
            if edge_lab_sort=yes then the left end of the edge will have a name lower 
            in alphabetic order than the right one
        """
        params = {
            'inmodel': in_model,
            'class': q(class_column),
            'edge_lab_sort': edge_lab_sort
        }
        self._fit(in_df, params, needs_id=False)


class MultiTreeBayesNetwork(TreeBayesNetwork):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        """
        Creates Multi Tree-shaped Bayesian Network object
        """
        super().__init__(idadb, model_name)
        self.fit_proc = "MTBNET_GROW"
        self.predict_proc = "TANET_APPLY"
    
    def fit(self, in_df: IdaDataFrame, class_column: str, in_columns: List[str]=None, 
            base_index: int=None, sample_size: int=None, talk: str=None, 
            edge_lab_sort: str=None, col_def_type: str=None, col_def_role: str=None, 
            col_properties_table: str=None) -> None:
        """
        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame

        class_column : str
            the target class; this should be column with nominal variables

        in_columns : List[str]
            List of the input dataframe columns with special properties. 
            Each column is followed by one or several of the following properties:
            - type: ':nom' (for nominal), ':cont' (for continuous). By default, 
              all numerical types are continuous, other types are nominal
            - role: ':id', ':target', ':input', ':ignore'.
            (Remark: ':objweight' is unsupported, i.e. ':objweight' same as ':ignore').
            (Remark: ':colweight(<wgt>)' is unsupported, i.e. ':colweight(<wgt>)' same as 
            ':colweight(1)' same as ':input').

            If the parameter is undefined, all columns of the input table have default properties. 
            Note that this procedure only accepts continuous columns with role 'input'
            Addition-ally, each column is followed by a colon (:) and either X or Y to distinguish 
            the two sets of variables.
        
        base_index : int, optional
            the numeric id to be assigned to the first variable
        
        sample_size : int, optional
            the sample size to take if the number of records is too large
        
        talk : str, optional
            if talk=yes then additional information on progress will be displayed
        
        edge_lab_sort : str, optional
            if edge_lab_sort=yes then the left end of the edge will have a name lower 
            in alphabetic order than the right one
        
        col_def_type : str, optional
            default type of the input dataframe columns. Allowed values are 'nom' and 'cont'.
            If the parameter is undefined, all numeric columns are considered continuous,
            other columns nominal.

        col_def_role : str, optional
            default role of the input dataframe columns.
            Allowed values are 'input' and 'ignore'.
            If the parameter is undefined, all columns are considered 'input' columns.

        col_properties_table : str, optional
            the input table where column properties for the input dataframe columns are stored.
            The format of this table is the output format of stored procedure
            nza..COLUMN_PROPERTIES().
            If the parameter is undefined, the input dataframe column properties will be
            detected automatically.
            (Remark: colPropertiesTable with "COLROLE" column with value 'objweight'
            is unsupported, i.e. same as 'ignore')
            (Remark: colPropertiesTable with "COLWEIGHT" column with value '<wgt>' is unsupported,
            i.e. same as '1')
        """
        params = {
            'class': q(class_column),
            'incolumn': q(in_columns),
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colPropertiesTable': col_properties_table,
            'baseidx': base_index,
            'samplesize': sample_size,
            'talk': talk,
            'edgelabsort': edge_lab_sort
        }
        self._fit(in_df, params, needs_id=False)





class TreeBayesNetworkBase(PredictiveModeling):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        """
        Creates base class for group of Tree Shaped Bayesian Network models
        """
        super().__init__(idadb, model_name)
    
    def _grow(self, in_df: IdaDataFrame, params: dict):
        """
        Grows the Tree Shaped Bayesian Network on input data and returns
        dataframe with statistics

        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame

        params : dict
            the dictionary of attributes used for running procedure

        Returns
        -------
        IdaDataFrame
            the data frame containing statistics
        """
        if not isinstance(in_df, IdaDataFrame):
            raise TypeError("Argument in_df should be an IdaDataFrame")

        ModelManager(self.idadb).drop_model(self.model_name)

        temp_view_name, need_delete = materialize_df(in_df)
        params['intable'] = temp_view_name

        params_s = map_to_props(params)

        try:
            in_df.ida_query(f'call NZA..TBNET1G(\'{params_s}\')')
        finally:
            if need_delete:
                in_df._idadb.drop_view(temp_view_name)
        
        table_name = f"INZA.nza_meta_{self.model_name}_model"
            
        if not in_df._idadb.exists_table_or_view(table_name):
            # stored procedure call was successful by did not produce a table
            return None
        
        out_df = IdaDataFrame(in_df._idadb, table_name)
        return out_df


class TreeBayesNetwork1G(TreeBayesNetworkBase):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        """
        Creates Tree-shaped Bayesian Network 1G object
        """
        super().__init__(idadb, model_name)
        self.predict_proc = "TBNET1G"

    def grow(self, in_df: IdaDataFrame, in_columns: List[str]=None, base_index: int=777, 
            sample_size: int=330000, talk: str=None, no_check: str=None, edge_lab_sort: str=None, 
            col_def_type: str=None, col_def_role: str=None, 
            col_properties_table: str=None) -> IdaDataFrame:
        """
        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame

        in_columns : List[str]
            List of the input dataframe columns with special properties. 
            Each column is followed by one or several of the following properties:
            - type: ':nom' (for nominal), ':cont' (for continuous). By default, 
              all numerical types are continuous, other types are nominal
            - role: ':id', ':target', ':input', ':ignore'.
            (Remark: ':objweight' is unsupported, i.e. ':objweight' same as ':ignore').
            (Remark: ':colweight(<wgt>)' is unsupported, i.e. ':colweight(<wgt>)' same as 
            ':colweight(1)' same as ':input').

            If the parameter is undefined, all columns of the input table have default properties. 
            Note that this procedure only accepts continuous columns with role 'input'
            Addition-ally, each column is followed by a colon (:) and either X or Y to distinguish 
            the two sets of variables.
        
        base_index : int, optional
            the numeric id to be assigned to the first variable
        
        sample_size : int, optional
            the sample size to take if the number of records is too large
        
        talk : str, optional
            if talk=yes then additional information on progress will be displayed
        
        no_check : str, optional
            if nocheck=yes then no exception is thrown when a column in <in_columns> 
            does not exis
        
        edge_lab_sort : str, optional
            if edge_lab_sort=yes then the left end of the edge will have a name lower 
            in alphabetic order than the right one
        
        col_def_type : str, optional
            default type of the input dataframe columns. Allowed values are 'nom' and 'cont'.
            If the parameter is undefined, all numeric columns are considered continuous,
            other columns nominal.

        col_def_role : str, optional
            default role of the input dataframe columns.
            Allowed values are 'input' and 'ignore'.
            If the parameter is undefined, all columns are considered 'input' columns.

        col_properties_table : str, optional
            the input table where column properties for the input dataframe columns are stored.
            The format of this table is the output format of stored procedure
            nza..COLUMN_PROPERTIES().
            If the parameter is undefined, the input dataframe column properties will be
            detected automatically.
            (Remark: colPropertiesTable with "COLROLE" column with value 'objweight'
            is unsupported, i.e. same as 'ignore')
            (Remark: colPropertiesTable with "COLWEIGHT" column with value '<wgt>' is unsupported,
            i.e. same as '1')
        
        Returns
        -------
        IdaDataFrame
            the data frame containing statistics
        """
        params = {
            'model': self.model_name,
            'incolumn': q(in_columns),
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colPropertiesTable': col_properties_table,
            'baseidx': base_index,
            'samplesize': sample_size,
            'talk': talk,
            'nocheck': no_check,
            'edgelabsort': edge_lab_sort
            }
        
        return self._grow(in_df, params)
    

class TreeBayesNetwork2G(TreeBayesNetworkBase):
    def __init__(self, idadb, model_name) -> None:
        """
        Creates Tree-shaped Bayesian Network 2G object
        """
        super().__init__(idadb, model_name)
        self.predict_proc = "TBNET2G"

    def grow(self, in_df: IdaDataFrame, in_columns: List[str]=None, base_index: int=777, 
            talk: str=None, no_check: str=None, edge_lab_sort: str=None, col_def_type: str=None, 
            col_def_role: str=None, col_properties_table: str=None) -> IdaDataFrame:
        """
        Builds a tree-like Bayesian Network for continuous variables.
        A spanning tree is constructed joining all the variables on grounds of most strong 
        correlations. This gives the user an overview of most significant interrelations 
        governing the whole set of variables.

        The stored procedure operates with two sets of variables and the resulting tree 
        will be bi-partite. The correlations between variables within each set will not 
        be calculated. This feature is useful when the two sets characterize distinct 
        objects and only links between the objects are of interest
        
        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame

        in_columns : List[str]
            List of the input dataframe columns with special properties. 
            Each column is followed by one or several of the following properties:
            - type: ':nom' (for nominal), ':cont' (for continuous). By default, 
              all numerical types are continuous, other types are nominal
            - role: ':id', ':target', ':input', ':ignore'.
            (Remark: ':objweight' is unsupported, i.e. ':objweight' same as ':ignore').
            (Remark: ':colweight(<wgt>)' is unsupported, i.e. ':colweight(<wgt>)' same as 
            ':colweight(1)' same as ':input').

            If the parameter is undefined, all columns of the input table have default properties. 
            Note that this procedure only accepts continuous columns with role 'input'
            Addition-ally, each column is followed by a colon (:) and either X or Y to distinguish 
            the two sets of variables.
        
        base_index : int, optional
            the numeric id to be assigned to the first variable
        
        talk : str, optional
            if talk=yes then additional information on progress will be displayed
        
        no_check : str, optional
            if nocheck=yes then no exception is thrown when a column in <in_columns> 
            does not exis
        
        edge_lab_sort : str, optional
            if edge_lab_sort=yes then the left end of the edge will have a name lower 
            in alphabetic order than the right one
        
        col_def_type : str, optional
            default type of the input dataframe columns. Allowed values are 'nom' and 'cont'.
            If the parameter is undefined, all numeric columns are considered continuous,
            other columns nominal.

        col_def_role : str, optional
            default role of the input dataframe columns.
            Allowed values are 'input' and 'ignore'.
            If the parameter is undefined, all columns are considered 'input' columns.

        col_properties_table : str, optional
            the input table where column properties for the input dataframe columns are stored.
            The format of this table is the output format of stored procedure
            nza..COLUMN_PROPERTIES().
            If the parameter is undefined, the input dataframe column properties will be
            detected automatically.
            (Remark: colPropertiesTable with "COLROLE" column with value 'objweight'
            is unsupported, i.e. same as 'ignore')
            (Remark: colPropertiesTable with "COLWEIGHT" column with value '<wgt>' is unsupported,
            i.e. same as '1')

        Returns
        -------
        IdaDataFrame
            the data frame containing statistics
        """
        params = {
            'model': self.model_name,
            'incolumn': q(in_columns),
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colPropertiesTable': col_properties_table,
            'baseidx': base_index,
            'talk': talk,
            'nocheck': no_check,
            'edgelabsort': edge_lab_sort
            }
        
        return self._grow(in_df, params)

class TreeBayesNetwork1G2P(TreeBayesNetworkBase):
    def __init__(self, idadb, model_name) -> None:
        """
        Creates Tree-shaped Bayesian Network 1G2P object
        """
        super().__init__(idadb, model_name)
        self.predict_proc = "TBNET1G2P"
    
    def grow(self, in_df: IdaDataFrame, in_columns: List[str]=None, base_index: int=777, 
            talk: str=None, no_check: str=None, edge_lab_sort: str=None, col_def_type: str=None, 
            col_def_role: str=None, col_properties_table: str=None) -> IdaDataFrame:
        """
        This stored procedure builds a tree-like Bayesian Network for continuous variables. 
        A spanning tree is constructed joining all the variables on grounds of most strong 
        correlations. This gives the user an overview of most significant interrelations 
        governing the whole set of variables.

        The stored procedure constructs the tree in an incremental manner. It calculates 
        correlations on one set of variables, then on the other set of variables, then 
        between variables of the 2 sets. The final model is obtained by joining the 
        three sub-models
        
        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame

        in_columns : List[str]
            List of the input dataframe columns with special properties. 
            Each column is followed by one or several of the following properties:
            - type: ':nom' (for nominal), ':cont' (for continuous). By default, 
              all numerical types are continuous, other types are nominal
            - role: ':id', ':target', ':input', ':ignore'.
            (Remark: ':objweight' is unsupported, i.e. ':objweight' same as ':ignore').
            (Remark: ':colweight(<wgt>)' is unsupported, i.e. ':colweight(<wgt>)' same as 
            ':colweight(1)' same as ':input').

            If the parameter is undefined, all columns of the input table have default properties. 
            Note that this procedure only accepts continuous columns with role 'input'
            Addition-ally, each column is followed by a colon (:) and either X or Y to distinguish 
            the two sets of variables.
        
        base_index : int, optional
            the numeric id to be assigned to the first variable
        
        talk : str, optional
            if talk=yes then additional information on progress will be displayed
        
        no_check : str, optional
            if nocheck=yes then no exception is thrown when a column in <in_columns> 
            does not exis
        
        edge_lab_sort : str, optional
            if edge_lab_sort=yes then the left end of the edge will have a name lower 
            in alphabetic order than the right one
        
        col_def_type : str, optional
            default type of the input dataframe columns. Allowed values are 'nom' and 'cont'.
            If the parameter is undefined, all numeric columns are considered continuous,
            other columns nominal.

        col_def_role : str, optional
            default role of the input dataframe columns.
            Allowed values are 'input' and 'ignore'.
            If the parameter is undefined, all columns are considered 'input' columns.

        col_properties_table : str, optional
            the input table where column properties for the input dataframe columns are stored.
            The format of this table is the output format of stored procedure
            nza..COLUMN_PROPERTIES().
            If the parameter is undefined, the input dataframe column properties will be
            detected automatically.
            (Remark: colPropertiesTable with "COLROLE" column with value 'objweight'
            is unsupported, i.e. same as 'ignore')
            (Remark: colPropertiesTable with "COLWEIGHT" column with value '<wgt>' is unsupported,
            i.e. same as '1')

        Returns
        -------
        IdaDataFrame
            the data frame containing statistics
        """
        params = {
            'model': self.model_name,
            'incolumn': q(in_columns),
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colPropertiesTable': col_properties_table,
            'baseidx': base_index,
            'talk': talk,
            'nocheck': no_check,
            'edgelabsort': edge_lab_sort
            }
        
        return self._grow(in_df, params)
