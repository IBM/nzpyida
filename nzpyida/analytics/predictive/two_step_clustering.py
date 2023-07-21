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
TwoStep clustering is a data mining algorithm for large data sets. It is faster than 
traditional methods because it typically scans a data set only once before it saves 
the data to a clustering feature (CF) tree. TwoStep clustering can make clustering 
decisions without repeated data scans, whereas other clustering methods scan all 
data points, which requires multiple iterations. Non- uniform points are not gathered, 
so each iteration requires a reinspection of each data point, regardless of the 
significance of the data point. Because TwoStep clustering treats dense areas 
as a single unit and ignores pattern outliers, it provides high-quality clustering 
results without exceeding memory constraints.

The TwoStep algorithm has the following advantages:
- It automatically determines the optimal number of clusters. You do not have to 
manually create a different clustering model for each number of clusters.
- It detects input columns that are not useful for the clustering process. 
These columns are automatically set to supplementary. Statistics are gathered 
for these columns but they do not influence the clustering algorithm.
- The configuration of the CF tree can be granular, so that you can balance between 
memory usage and model quality, according to the environment and needs.
"""

from typing import List
from nzpyida.frame import IdaDataFrame
from nzpyida.base import IdaDataBase
from nzpyida.analytics.utils import map_to_props, make_temp_table_name
from nzpyida.analytics.utils import get_auto_delete_context
from nzpyida.analytics.predictive.predictive_modeling import PredictiveModeling
from nzpyida.analytics.utils import q


class TwoStepClustering(PredictiveModeling):
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
        self.fit_proc = 'TWOSTEP'
        self.predict_proc = 'PREDICT_TWOSTEP'
        self.score_proc = 'MSE'
        self.target_column_in_output = idadb.to_def_case('CLUSTER_ID')
        self.id_column_in_output = idadb.to_def_case('ID')
        self.has_print_proc = True

    def fit(self, in_df: IdaDataFrame, id_column: str=None, target_column: str=None,
        in_columns: List[str]=None, col_def_type: str=None, col_def_role: str=None,
        col_properties_table: str=None, out_table: str=None, k: int=0, max_k: int=20, 
        bins: int=10, statistics: str=None, rand_seed: int=12345, distance: str='loglikelihood', 
        distance_threshold: float=None, distance_threshold_factor: float=2.0, 
        epsilon: float=0.0, node_capacity: int=6, leaf_capacity: int=8, 
        max_leaves: int=1000, outlier_fraction: float=0.0) -> IdaDataFrame:
        """
        Builds a TwoStep Clustering model that first distributes the input data into 
        a hierarchical tree structure according to the distance between the data records, 
        then reduces the tree into k clusters. A second pass over the data associates 
        the input data records to the next cluster.

        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame

        id_column : str, optional
            the input table column identifying a unique instance id
        
        target_column : str, optional
            the input table column representing a class or a value to predict, 
            this column is ignored by the TwoStep Clustering algorithm

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
        
        k : int, optional
            the number of clusters. If k is 0 or less, the procedure determines 
            the optimal number of clusters

        max_k : int, optional
            the maximum number of clusters that can be determined automatically. 
            If k is bigger than 0, this parameter is ignored
        
        bins : int, optional
            the average number of bins for numerical statistics with more than <n> values

        statistics : str, optional
            flags indicating which statistics to collect. Allowed values are: none, 
            columns, values:n, all.
            Regardless of the value of the parameter statistics, all statistics are 
            gathered since they are needed to call PREDICT_TWOSTEP on this model. 
            If statistics=none or statistics=columns, the importance of the attributes 
            is not calculated. If statistics=none, statistics=columns or statistics=all, 
            up to 100 discrete values are gathered.
            If statistics=values:n with n a positive number, up to <n> column value 
            statistics are collected:
                - If a nominal column contains more than <n> values, only the <n> most 
                frequent column stat-istics are kept.
                - If a numeric column contains more than <n> values, the values will 
                be discretized and the stat-istics will be collected on the discretized values.
            Indicating statistics=all is equal to statistics=values:100.
            
        rand_seed : int, optional
            the random generator seed

        distance : str, optional
            the distance function. Allowed values are: euclidean, norm_euclidean, loglikelihood

        distance_threshold : float, optional
            the threshold under which 2 data records can be merged into one cluster during 
            the first pass. If not set, the distance threshold is calculated automatically
        
        distance_threshold_factor : float, optional
            the factor used to calculate the distance threshold automatically. 
            The distance threshold is then the median distance value minus 
            distance_threshold_factor times the interquartile distance 
            (or the minimum distance if this value is below it). If distance_threshold is set, 
            this parameter is ignored
        
        epsilon : float, optional
            the value to be used as global variance of all continuous fields for the loglikelihood 
            distance. If the value is 0.0 or less, the global variance is calculated for each 
            continuous field. If distance is not loglikelihood, this parameter is ignored
        
        node_capacity : int, optional
            the branching factor of the internal tree used in pass 1. Each node can have up to 
            node_capacity subnodes
        
        leaf_capacity : int, optional
            the number of clusters per leaf node in the internal tree used in pass 1

        max_leaves : int, optional
            the maximum number of leaf nodes in the internal tree used in pass 1. 
            When the tree contains maxleaves leaf nodes, the following data records are 
            aggregated into the existing clusters
        
        outlier_fraction : float, optional
            the fraction of the records to be considered as outlier in the internal
            tree used in pass 1. Clusters containing less than outlierfraction times 
            the mean number of data records per cluster are removed
        
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
            'k': k,
            'maxk': max_k,
            'bins': bins,
            'statistics': statistics,
            'randseed': rand_seed,
            'distance': distance,
            'distancethreshold': distance_threshold,
            'distancethresholdfactor': distance_threshold_factor,
            'epsilon': epsilon,
            'nodecapacity': node_capacity,
            'leafcapacity': leaf_capacity,
            'maxleaves': max_leaves,
            'outlierfraction': outlier_fraction,
            'outtable': out_table
        }

        self._fit(in_df=in_df, params=params)

        if auto_delete_context:
            auto_delete_context.add_table_to_delete(out_table)

        return IdaDataFrame(self.idadb, out_table)

    def predict(self, in_df: IdaDataFrame, out_table: str=None,
        id_column: str=None) -> IdaDataFrame:
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

        Returns
        -------
        IdaDataFrame
            the data frame containing row identifiers and predicted target values
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
