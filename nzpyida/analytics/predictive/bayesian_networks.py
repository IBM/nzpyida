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
from nzpyida.frame import IdaDataFrame
from nzpyida.base import IdaDataBase
from nzpyida.analytics.utils import map_to_props, materialize_df, make_temp_table_name
from nzpyida.analytics.utils import get_auto_delete_context
from nzpyida.analytics.predictive.predictive_modeling import PredictiveModeling


class TreeShapedBayesianNetwork(PredictiveModeling):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)

    def fit(self, in_df: IdaDataFrame, in_columns: List[str]=None, base_index: int=777,
            sample_size: int=330000, talk: str=None, no_check: str=None, 
            edge_lab_sort: str=None, col_def_type: str=None, col_def_role: str=None, 
            col_properties_table: str=None):
        
        params = {
            'incolumn': in_columns,
            'coldeftype': col_def_type,
            'coldefrole': col_def_role,
            'colPropertiesTable': col_properties_table,
            'baseidx': base_index,
            'samplesize': sample_size,
            'talk': talk,
            'nocheck': no_check,
            'edgelabsort': edge_lab_sort
        }
        self._fit(in_df=in_df, params=params, needs_id=False)


class TreeShapedBayesianNetwork1G(TreeShapedBayesianNetwork):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)

class TreeShapedBayesianNetwork1G2P(TreeShapedBayesianNetwork):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)
    
class TreeShapedBayesianNetwork2G(TreeShapedBayesianNetwork):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        super().__init__(idadb, model_name)