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
The divisive clustering algorithm is a computationally efficient, top-down approach 
to creating hierarchical clustering models. Conceptually, it can be thought of as 
a wrapper around the k -means algorithm (with a specialized method for initial centroid setting), 
running the algorithm several times to divide clusters into subclusters. 
The internal k-means algorithm assumes a fixed k =2 value.

The divisive clustering algorithm may return different results for the same data set 
and the same random generator seed when you use different input data distribution or 
a different number of dataslices. This is due to the behavior of the random number 
generator, which generates random sequences depending on the number of dataslices 
and data distribution. The algorithm returns the same model when you use the same machine, 
the same input data distribution, and the same random seed.

The cluster formation process of the divisive clustering algorithm begins with a single cluster 
containing all training instances, then the first invocation of k-means divides it into two 
subclusters by creating two descendant nodes of the clustering tree. Subsequent invocations 
divide these clusters into more subclusters, and so on, until a stop criterion is satisfied. 
Stop criterion can be specified by the maximum clustering tree depth or by the minimum required 
umber of instances for further partitioning. The resulting hierarchical clustering tree can be 
used to classify instances by propagating them down from the root node, and choosing at each 
level the best matching sub-cluster with respect to the instance’s distance from sub-cluster centers.

The internal k-means process of the divisive clustering algorithm operates using the ordinary 
k-means algorithm (with the modified initial centroid generation), discussed in the K-Means 
Clustering section, using a fixed value of k=2 and working with the subset of data from the 
parent cluster. The initial centroid generation consists two steps: random generation n>>k 
candidates and then selection of outermost pair of candidates. The cluster center representation 
and distance measures remain the same. The numbering scheme for clusters in a clustering tree is 
the same as decision trees: the root node is number 1, and the descendants of node number 'i' have 
numbers '2i' and '2i+1' 

Additionally, leaves, which are clusters with no subclusters, are designated by negative numbers.
"""

from typing import List
from nzpyida.frame import IdaDataFrame
from nzpyida.base import IdaDataBase
from nzpyida.analytics.utils import map_to_props, make_temp_table_name
from nzpyida.analytics.utils import get_auto_delete_context
from nzpyida.analytics.predictive.predictive_modeling import PredictiveModeling
from nzpyida.analytics.utils import q


class BisectingKMeans(PredictiveModeling):
    """
    Divisive Clustering
    """
    def __init__(self, idadb: IdaDataBase, model_name: str):
        """
        Creates the clusterer class.

        Parameters
        ----------

        idada : IdaDataBase
            database connector

        model_name : str
            model name - if it exists in the database, it will be used, otherwise
            it must be trained using fit() function before prediction or scoring is called.
        """

        super().__init__(idadb, model_name)
        self.fit_proc = 'DIVCLUSTER'
        self.predict_proc = 'PREDICT_DIVCLUSTER'
        self.score_proc = 'MSE'
        self.target_column_in_output = idadb.to_def_case('CLUSTER_ID')
        self.id_column_in_output = idadb.to_def_case('ID')
        self.has_print_proc = True

    def fit(self, in_df: IdaDataFrame, id_column: str=None, target_column: str=None,
        in_columns: List[str]=None, col_def_type: str=None, col_def_role: str=None,
        col_properties_table: str=None, out_table: str=None, distance: str='euclidean',
        max_iter: int=5, min_split: int=5, max_depth: int=3, rand_seed: int=12345) -> IdaDataFrame:
        """
        Builds a Hierarchical Clustering model using a divisive method (top-down). 
        The K- means algorithm is used recursively. The hierarchy of clusters is 
        represented in a binary tree structure (each parent node has exactly 2 children node). 
        The leafs of the cluster tree are identified by negative numbers. 
        The divisive clustering algorithm may return different results for the same dataset 
        and the same random generator seed when you use different input data distribution 
        or a different number of dataslices. This is due to the behavior of the random number 
        generator, which generates random sequences depending on the number of dataslices 
        and data distribution. The algorithm returns the same model when the same ma-chine, 
        the same input data distribution, and the same random seed is used.

        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame

        id_column : str, optional
            the input table column identifying a unique instance id - if skipped, 
            the input data frame indexer must be set and will be used as an instance id
        
        target_column : str, optional
            the input table column representing a class or a value to predict, 
            this column is ignored by the Hierarchical Clustering algorithm

        in_columns : List[str], optional
            the list of input table columns with special properties.
            Each column is followed by one or several of the following properties:
                its type: ':nom' (for nominal), ':cont' (for continuous).
                Per default, all numerical types are con-tinuous, other types are nominal.
                its role: ':id', ':target', ':input', ':ignore'.
            (Remark: ':objweight' is unsupported, i.e. ':objweight' same as ':ignore').
            (Remark: ':colweight(<wgt>)' is unsupported, i.e. ':colweight(<wgt>)' same
            as ':colweight(1)' same as ':input').
            If the parameter is undefined, all columns of the input table have default properties.

        col_def_type : str, optional
            default type of the input table columns. Allowed values are 'nom' and 'cont'.
            If the parameter is undefined, all numeric columns are considered continuous,
            other columns nominal.

        col_def_role : str, optional
            default role of the input table columns.
            Allowed values are 'input' and 'ignore'.
            If the parameter is undefined, all columns are considered 'input' columns.

        col_properties_table : str, optional
            the input table where column properties for the input table columns are stored.
            The format of this table is the output format of stored procedure
            nza..COLUMN_PROPERTIES().
            If the parameter is undefined, the input table column properties will be
            detected automatically.
            (Remark: colPropertiesTable with "COLROLE" column with value 'objweight'
            is unsupported, i.e. same as 'ignore')
            (Remark: colPropertiesTable with "COLWEIGHT" column with value '<wgt>' is unsupported,
            i.e. same as '1')

        out_table : str, optional
            the output table where clusters are assigned to each input table record

        distance : str, optional
            the distance function. Allowed values are: euclidean, norm_euclidean, manhattan,
            canberra, maximum, mahalanobis.

        max_iter : int, optional
            the maximum number of iterations to perform in the base K-means Clustering algorithm
        
        min_split : int, optional
            the minimum number of instances per cluster that can be split
        
        max_depth : int, optional
            the maximum number of cluster levels (including leaves)

        rand_seed : int, optional
            the random generator seed
        
        Returns
        -------
        IdaDataFrame
            the data frame containing row identifiers, cluster_id and distance to cluster center
        """

        auto_delete_context = None
        if not out_table:
            auto_delete_context = get_auto_delete_context('out_table')
            out_table = make_temp_table_name()

        params = {
            'id': q(id_column),
            'target': q(target_column),
            'incolumn': q(in_columns),
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colpropertiestable': col_properties_table,
            'distance': distance,
            'maxiter': max_iter,
            'minsplit': min_split,
            'maxdepth': max_depth,
            'randseed': rand_seed,
            'outtable': out_table
        }

        self._fit(in_df=in_df, params=params)

        if auto_delete_context:
            auto_delete_context.add_table_to_delete(out_table)

        return IdaDataFrame(self.idadb, out_table)

    def predict(self, in_df: IdaDataFrame, out_table: str=None,
        id_column: str=None, level: int=-1) -> IdaDataFrame:
        """
        Makes predictions based on this model. The model must exist.

        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame

        out_table : str, optional
            the output table where the assigned clusters will be stored

        id_column : str, optional
            the input table column identifying a unique instance id
            Default: id column used to build the model
        
        level : int, optional
            the level of the cluster hierarchy which should be applied to the data. 
            For level=-1, only the leaves of the clustering tree are considered

        Returns
        -------
        IdaDataFrame
            the data frame containing row identifiers and predicted target values
        """

        params = {
            'id': q(id_column),
            'level': level
        }

        return self._predict(in_df=in_df, params=params, out_table=out_table)

    def score(self, in_df: IdaDataFrame, target_column: str,
        id_column: str=None, level: int=-1) -> float:
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
        
        level : int, optional
            the level of the cluster hierarchy which should be applied to the data. 
            For level=-1, only the leaves of the clustering tree are considered

        Returns
        -------
        float
            the model score
        """

        params = {
            'id': q(id_column),
            'level': level
        }

        return self._score(in_df=in_df, predict_params=params, target_column=target_column)
