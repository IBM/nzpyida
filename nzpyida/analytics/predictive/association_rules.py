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
Association rules mining is a popular method for discovering interesting 
and useful patterns in a large scale transaction database. The database 
contains transactions which consist of a set of items and a transaction 
identifier (e.g., a market basket). Association rules are implications 
of the form X -> Y where X and Y are two disjoint subsets of all available 
items. X is called the antecedent or LHS (left hand side) and Y is called 
the consequent or RHS (right hand side). Discovered association rules have 
to satisfy user-defined constraints on measures of significance and interest.

The Apriori algorithm organizes the search for frequent itemsets by systematically 
considering itemsets of increasing size in consecutive iterations. Due to its method 
of calculation, the number of candidates identified by the Apriori algorithm may 
be overwhelming for extremely large data sets or a low support threshold. Because 
of this limitation, the FP-growth algorithm is provided in the IBM Netezza In-Database 
Analytics package instead.

The FP-growth algorithm avoids candidate generation as well as multiple passes through 
the data by creating a data structure called a frequent pattern tree, or FP-tree. 
This tree is a compact representation of the data set contents sufficient for finding 
frequent itemsets. Nodes of the tree represent single items and store their occurrence 
counts. Only items with sufficiently high support, frequent item-sets of size 1, are 
represented. Branches, called node-links, in the FP-tree connect nodes that represent 
items co-occurring for some instances in the data set. There is also a frequent item 
header table that points to nodes corresponding to particular items.

The tree is built by identifying all frequent items and their counts, then consecutively 
“inserting” each transaction to the tree. This requires exactly two scans of the 
data set, regardless of its size or support threshold level. The FP-tree is used to 
identity frequent itemsets using a frequent pattern growth process, which traverses 
the tree by following node-links in an appropriate way.

By avoiding explicit candidate generation, the FP-growth algorithm reduces the number 
of data set scans. It can also perform efficiently, regardless of the threshold support.
"""

from typing import List
from nzpyida.frame import IdaDataFrame
from nzpyida.base import IdaDataBase
from nzpyida.analytics.predictive.predictive_modeling import PredictiveModeling
from nzpyida.analytics.utils import q

class ARule(PredictiveModeling):
    def __init__(self, idadb: IdaDataBase, model_name: str):
        """
        Creates Association Rules Class

        Parameters
        ----------

        idada : IdaDataBase
            database connector

        model_name : str
            model name - the name of the Association Rules model to build
        """
        super().__init__(idadb, model_name)
        self.has_print_proc = True
        self.fit_proc = 'ARULE'
        self.predict_proc = 'PREDICT_ARULE'
        self.model_name = model_name
    
    def fit(self, in_df: IdaDataFrame, transaction_id_column: str='tid', item_column: str='item', by_column: str=None, 
            level: int=1, max_set_size: int=6, support: float=None, support_type: str='percent',
            confidence: float=0.5):
        """
        This function builds an Association Rules model. The model is saved to the 
        database in a set of tables and registered in the database model metadata. Use the 
        function 'describe' to display the Association Rules of the model, or the 
        Model Management functions to further manipulate the model

        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame

        transaction_id_column : str, optional
            the input table column identifying transactions
        
        items_column : str, optional
            the input table column identifying items in transactions

        by_column : str, optional
            the input table column identifying groups of transactions if any. 
            Association Rules min-ing is done separately on each of these groups. 
            Leave the parameter undefined if no groups are to be considered.
        
        level : int, optional
            ARULE first temporarily redistributes the data into overlapping parts in such a way, 
            that each part can be processed in parallel without communication between the SPUs. 
            Note that for this to work, there can be redundancy between the parts, such that 
            the accumulative size of the temporary parts can be much higher than the one of 
            the ori- ginal data set. The parameter lvl controls how many parts are created. 
            The higher lvl: 
                - The more computation and temporary database space is required for the splitting
                - The smaller the amount of main memory that is required for each data slice

            Note: To fully use the benefits of parallel computing, do not specify the value 
            of the lvl parameter too low. Additionally, the lower the value of the lvl parameter,
            the higher the memory consumption for each part. The higher memory consumption might 
            cause an out-of-memory error on the SPUs. If an out-of-memory error on the SPUs oc-curs, 
            increase the lvl parameter.
            
            If you specify the value 0, the algorithm is executed in a serial way for each data 
            set group. However, only if the data set fits in one node, and only if the splitting 
            increases the total number of rows dramatically, the stored procedure might be executed 
            faster when you specify the value 0.
            Default - 1
            Min - 0
        
        support, int, optional
            minimum support value satisfied by all association rules. According to supporttype, 
            it defines the absolute number (#supporting transactions) or the percentage of 
            transactions (#supporting transactions/#total transactions*100). Too low minimum support 
            increases the number of generated rules and the computational expense.
        
        support_type: str, optional
            the type how the minimum support should be interpreted. The following values are allowed: 
            absolute, percent.
            Note the support and support_type values are common to all groups in the dataset. 
            E.g. if 3 is the absolute minimum support, then an itemset will be considered frequent 
            if at least 3 trans-actions contain its items, no matter what is the number of transactions 
            in this group. Use support_type='percent' to indicate a minimum support depending on the 
            size of the groups. Specifying support_type='absolute' takes effect only if a support is 
            explicitly supplied.

        confidence: float, optional
            the minimum confidence for an association rule to be
            default - 0.5
            min - 0
            max - 1 
        """
        if support_type == 'percent' and not support:
            support = 5.0
        params = {
            'tid': q(transaction_id_column),
            'item': q(item_column),
            'by': q(by_column),
            'lvl': level,
            'maxsetsize': max_set_size,
            'support': support,
            'supporttype': support_type,
            'confidence': confidence
        }

        self._fit(in_df=in_df, params=params, needs_id=False)
    
    def predict(self, in_df: IdaDataFrame, out_table: str=None, transaction_id_column: str='tid', item_column: str='item',
                by_column: str=None, scoring_type: str='exclusiveRecommend', name_map_column: str=None, item_name_column: str='item',
                item_name_mapped_column: str='item_name', min_size: int=1, max_size: int=64, min_support: float=0.0, max_support: float=1.0,
                min_confidence: float=0.0, max_confidence: float=1.0, min_lift: float=None, max_lift: float=None, 
                min_conviction: float=0.0, max_conviction: float=None, min_affinity: float=0.0, max_affinity: float=1.0,
                min_leverage: float=-0.25, max_leverage: float=1.0) -> IdaDataFrame:
        """
        Makes predictions based on this model. The model must exist.

        Parameters
        ----------

        in_df : IdaDataFrame
            the input data frame

        out_table : str, optional
            the output table where the predictions will be stored
        
        transaction_id_column : str, optional
            the input table column identifying transactions
        
        items_column : str, optional
            the input table column identifying items in transactions

        by_column : str, optional
            the input table column identifying groups of transactions if any. 
            Association Rules min-ing is done separately on each of these groups. 
            Leave the parameter undefined if no groups are to be considered.

        scoring_type: str, optional
            he type how the scoring algorithm should be applied to the input data. 
            The following values are allowed: recommend, exclusiveRecommend.
            recommend - A rule is returned if its left hand side itemset is a subset of the transaction.
            exclusiveRecommend - A rule is returned if its left hand side itemset is a subset of 
            the input itemset, and its right hand side itemset is not a subset of the transaction.
        
        name_map_column: str, optional
            table which provides names of items and their associated mapped values 
            in LHS_ITEMS, RHS_ITEMS columns of outtable

        item_name_column, str, optional
            the column name of namemap table where the item identifiers are

        item_name_mapped_column: str, optional
            the column name of namemap table where the item names are stored which should be used in-stead of the item identifier

        min_size: int, optional
            The minimum number of items per association rule to be applied

        max_size: int, optional
            The maximum number of items per association rule to be applied

        min_support: float, optional
            The minimum support of an association rule to be applied

        max_support: float, optional
            The maximum support of an association rule to be applied

        min_confidence: float, optional
            The minimum confidence of an association rule to be applied.

        max_confidence: float, optional
            The maximum confidence of an association rule to be applied

        min_lift: float, optional
            The minimum lift of an association rule to be applied

        max_lift: float, optional
            The maximum lift of an association rule to be applied

        min_conviction: float, optional
            The minimum conviction of an association rule to be applied

        max_conviction: float, optional
            The maximum conviction of an association rule to be applied

        min_affinity: float, optional
            The minimum affinity of an association rule to be applied
        
        max_affinity: float, optional
            The maximum affinity of an association rule to be applied

        min_leverage: float, optional
            The minimum leverage of an association rule to be applied

        max_leverage: float, optional
            The maximum leverage of an association rule to be applied

        Returns
        -------
        IdaDataFrame
            the data frame containing output of a Association Rules model prediction
        """
        params = {
            'tid': q(transaction_id_column),
            'item': q(item_column),
            'by': q(by_column),
            'type': scoring_type,
            'namemap': name_map_column,
            'itemname': item_name_column,
            'itemnamemapped': item_name_mapped_column,
            'minsize': min_size,
            'maxsize': max_size,
            'minsupp': min_support,
            'maxsupp': max_support,
            'minconf': min_confidence,
            'maxconf': max_confidence,
            'minlift': min_lift,
            'maxlift': max_lift,
            'minconv': min_conviction,
            'maxconv': max_conviction,
            'minaffi': min_affinity,
            'maxaffi': max_affinity,
            'minleve': min_leverage,
            'maxleve': max_leverage
        }
        return self._predict(in_df=in_df, params=params, out_table=out_table)
    