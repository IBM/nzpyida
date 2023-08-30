#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Copyright (c) 2015-2023, IBM Corp.
# All rights reserved.
#
# Distributed under the terms of the BSD Simplified License.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

"""
Test module for join operations
"""

import pytest
import nzpyida
from nzpyida import IdaDataBase

class TestMerge:
    def test_inner_join(self, idadf_iris, idadf_iris2, idadb):
        ida_nzpyida_join = nzpyida.merge(idadf_iris, idadf_iris2, on='index', 
                                        suffixes=("_a", "_b"), indicator=True)
        assert len(ida_nzpyida_join) == 100
        assert all(ida_nzpyida_join.columns == ['index', 'sepal_length_a', 'sepal_width_a',
                                                'petal_length_a', 'petal_width',
                                                'species', 'sepal_length_b', 'sepal_width_b',
                                                'petal_length_b', 'PETAL_WIDTH', 'SPECIES',
                                                idadb.to_def_case('INDICATOR')])
        assert idadf_iris.indexer == ida_nzpyida_join.indexer
        
    def test_left_join(self, idadf_iris, idadf_iris2):
        ida_nzpyida_join = nzpyida.merge(idadf_iris, idadf_iris2, how='left', left_on='index', right_on='index')
        assert len(ida_nzpyida_join) == 150
        assert all(ida_nzpyida_join.columns == ['index', 'sepal_length_x', 'sepal_width_x',
                                                'petal_length_x', 'petal_width',
                                                'species', 'sepal_length_y', 'sepal_width_y',
                                                'petal_length_y', 'PETAL_WIDTH', 'SPECIES',
                                                ])
        assert all(idadf_iris.index == ida_nzpyida_join.index)
        assert idadf_iris.indexer == ida_nzpyida_join.indexer


    def test_right_join(self, idadf_iris, idadf_iris2):
        ida_nzpyida_join = nzpyida.merge(idadf_iris, idadf_iris2, how='right', left_index=True, right_index=True)
        assert len(ida_nzpyida_join) == 150
        assert all(ida_nzpyida_join.columns == ['index', 'sepal_length_x', 'sepal_width_x',
                                                'petal_length_x', 'petal_width',
                                                'species', 'sepal_length_y', 'sepal_width_y',
                                                'petal_length_y', 'PETAL_WIDTH', 'SPECIES',
                                                ])
        assert all(idadf_iris2.index == ida_nzpyida_join.index)
        assert idadf_iris2.indexer == ida_nzpyida_join.indexer

    def test_outer_join(self, idadf_iris, idadf_iris2):
        ida_nzpyida_join = idadf_iris.merge(idadf_iris2, how='outer', left_index=True, right_on='index')
        assert len(ida_nzpyida_join) == 200
        assert all(ida_nzpyida_join.columns == ['index', 'sepal_length_x', 'sepal_width_x',
                                                'petal_length_x', 'petal_width',
                                                'species', 'sepal_length_y', 'sepal_width_y',
                                                'petal_length_y', 'PETAL_WIDTH', 'SPECIES',
                                                ])
        
    def test_cross_join(self, idadf_iris, idadf_iris2):
        ida_nzpyida_join = nzpyida.merge(idadf_iris, idadf_iris2, how='cross')
        assert len(ida_nzpyida_join) == len(idadf_iris)*len(idadf_iris2)
        assert all(ida_nzpyida_join.columns == ['index_x', 'sepal_length_x', 'sepal_width_x',
                                                'petal_length_x', 'petal_width', 'species',
                                                'index_y', 'sepal_length_y', 'sepal_width_y',
                                                'petal_length_y', 'PETAL_WIDTH', 'SPECIES',
                                                ])
        
    def test_join_on_multiple_columns(self, idadf_iris, idadf_iris2, idadb):
        ida_nzpyida_join  = nzpyida.merge(idadf_iris, idadf_iris2, 
                                          on=["sepal_length", "sepal_width"], indicator=True)
        assert len(ida_nzpyida_join) == 230
        assert all(ida_nzpyida_join.columns == ['sepal_length', 'sepal_width', 'index_x',
                                                'petal_length_x', 'petal_width', 'species',
                                                'index_y', 'petal_length_y', 'PETAL_WIDTH', 
                                                'SPECIES', idadb.to_def_case('INDICATOR')])
    def test_join_on_multiple_columns_different_names(self, idadf_iris, idadf_iris2, idadb):
        ida_nzpyida_join  = nzpyida.merge(idadf_iris, idadf_iris2, how="outer", 
                                          left_on=["sepal_length", "petal_width"],
                                          right_on=["sepal_length", "PETAL_WIDTH"], indicator=True)
        assert len(ida_nzpyida_join) == 258
        assert all(ida_nzpyida_join.columns == ['sepal_length', 'index_x', 'sepal_width_x', 
                                                'petal_length_x', 'petal_width', 'species',
                                                'index_y', 'sepal_width_y', 'petal_length_y', 
                                                'PETAL_WIDTH', 'SPECIES', 
                                                idadb.to_def_case('INDICATOR')])
    
    def test_join_on_one_item_list(self, idadf_iris, idadf_iris2):
        ida_nzpyida_join  = nzpyida.merge(idadf_iris, idadf_iris2, on=["sepal_length"])
        ida_nzpyida_join2  = idadf_iris.merge(idadf_iris2, on="sepal_length")
        assert ida_nzpyida_join.internal_state._views[-1] == ida_nzpyida_join2.internal_state._views[-1]

    def test_no_on(self, idadf_iris, idadf_iris2):
        ida_nzpyida_join  = nzpyida.merge(idadf_iris, idadf_iris2, how='right')
        assert len(ida_nzpyida_join) == len(idadf_iris2)
        assert all(ida_nzpyida_join.columns == ['index', 'sepal_length', 'sepal_width',
                                                'petal_length', 'petal_width', 'species',
                                                'PETAL_WIDTH', 'SPECIES'])

    def test_only_left_index(self, idadf_iris, idadf_iris2):
        with pytest.raises(ValueError) as e:
            nzpyida.merge(idadf_iris, idadf_iris2, left_index=True)
            assert str(e) == 'Must pass "right_on" OR "right_index'

    def test_only_left_on(self, idadf_iris, idadf_iris2):
        with pytest.raises(ValueError) as e:
            nzpyida.merge(idadf_iris, idadf_iris2, left_on="index")
            assert str(e) == 'Must pass "right_on" OR "right_index'

    def test_only_right_index(self, idadf_iris, idadf_iris2):
        with pytest.raises(ValueError) as e:
            nzpyida.merge(idadf_iris, idadf_iris2, right_index=True)
            assert str(e) == 'Must pass "left_on" OR "left_index'

    def test_only_right_on(self, idadf_iris, idadf_iris2):
        with pytest.raises(ValueError) as e:
            idadf_iris.merge(idadf_iris2, right_on="index")
            assert str(e) == 'Must pass "left_on" OR "left_index'
        
    def test_wrong_left_on(self, idadf_iris, idadf_iris2):
        with pytest.raises(KeyError) as e:
            nzpyida.merge(idadf_iris, idadf_iris2, left_on="blabla")
            assert str(e) == f"No column blabla in {idadf_iris.name} dataframe"

    def test_no_right_indexer(self, idadf_iris, idadf):
        with pytest.raises(ValueError) as e:
            nzpyida.merge(idadf_iris, idadf, left_on="index", right_index=True)
            assert str(e) == f"'right_index' set to True, but {idadf.name} has no indexer"

    def test_both_left_on_and_indexer(self, idadf_iris, idadf_iris2):
        with pytest.raises(ValueError) as e:
            idadf_iris.merge(idadf_iris2, left_on="index", left_index=True, right_index=True)
            assert str(e) == f'Can only pass argument "left_on" OR "left_index" not both'

    def test_cross_with_on_column(self, idadf_iris, idadf_iris2):
        with pytest.raises(ValueError) as e:
            nzpyida.merge(idadf_iris, idadf_iris2, how="cross", left_index=True, right_index=True)
            assert str(e) == "Can not pass on, right_on, left_on or set right_index=True or left_index=True"
        
    def test_one_wrong_column_in_on_list(self, idadf_iris, idadf_iris2):
        with pytest.raises(KeyError) as e:
            on_cols = ["sepal_length", "sepal_width", "petal_width"]
            idadf_iris.merge(idadf_iris2, on=on_cols)
            assert str(e) == f"Not all on columns {on_cols} in {idadf_iris2.name} dataframe"
    
    def test_on_lengths_mismatch(self, idadf_iris, idadf_iris2):
        with pytest.raises(ValueError) as e:
            nzpyida.merge(idadf_iris, idadf_iris2, 
                          left_on=["sepal_length", "sepal_width", "petal_width"],
                          right_on=["sepal_length", "sepal_width"])
            assert str(e) == "len(right_on) must equal len(left_on)"
    
    def test_left_on_list_with_right_index(self, idadf_iris, idadf_iris2):
        with pytest.raises(ValueError) as e:
            nzpyida.merge(idadf_iris, idadf_iris2, right_on=['index', 'sepal_length'], 
                          left_index=True)
            assert str(e) == "len(right_on) must equal len(left_on)"

class TestConcat:
    def test_outer_join(self, idadf_iris, idadf_iris2, idadb):
        ida_nzpyida_union = nzpyida.concat([idadf_iris, idadf_iris2], keys=['a', 'b'])
        assert len(ida_nzpyida_union) == len(idadf_iris) + len(idadf_iris2)
        assert all(ida_nzpyida_union.columns == [idadb.to_def_case('keys'), 'index', 'sepal_length', 'sepal_width', 
                                                 'petal_length', 'petal_width', 'species', 
                                                 'PETAL_WIDTH', "SPECIES"])
        assert ida_nzpyida_union.indexer == 'KEYS'
    
    def test_inner_join(self, idadf_iris, idadf_iris2):
        ida_nzpyida_union = nzpyida.concat([idadf_iris, idadf_iris2], join='inner')
        assert len(ida_nzpyida_union) == len(idadf_iris) + len(idadf_iris2)
        assert all(ida_nzpyida_union.columns == ['index', 'sepal_length', 'sepal_width', 
                                                 'petal_length'])
    
    def test_outer_join_three_dataframes(self, idadf_iris, idadf_iris2, idadf):
        ida_nzpyida_union = nzpyida.concat([idadf_iris, idadf_iris2, idadf], join='inner')
        assert len(ida_nzpyida_union) == len(idadf_iris) + len(idadf_iris2) + len(idadf)
        assert all(ida_nzpyida_union.columns == ['sepal_length', 'sepal_width', 'petal_length'])
    
    def test_objs_not_sequence(self, idadf_iris):
        with pytest.raises(ValueError) as e:
            nzpyida.concat(idadf_iris)
            assert str(e) == "Argument 'objs' should be a sequnece"

    def test_objs_1_element(self, idadf_iris):
        with pytest.raises(ValueError) as e:
            nzpyida.concat([idadf_iris])
            assert str(e) == "Argument 'objs' should contain at least 2 elements"
    
    def test_inner_join_axis_1(self, idadf_iris, idadf_iris2):
        ida_nzpyida_join = nzpyida.concat([idadf_iris, idadf_iris2], axis=1)
        assert len(ida_nzpyida_join) == 200
        assert all(ida_nzpyida_join.columns == ['index', 'sepal_length_x', 'sepal_width_x',
                                                'petal_length_x', 'petal_width',
                                                'species', 'sepal_length_y', 'sepal_width_y',
                                                'petal_length_y', 'PETAL_WIDTH', 'SPECIES',
                                                ])
    
class TestJoin:
    def test_join_on(self, idadf_iris, idadf_iris2):
        ida_nzpyida_join = idadf_iris.join(idadf_iris2, on='index')
        assert len(ida_nzpyida_join) == 150
        assert all(ida_nzpyida_join.columns == ['index', 'sepal_length_x', 'sepal_width_x',
                                                'petal_length_x', 'petal_width',
                                                'species', 'sepal_length_y', 'sepal_width_y',
                                                'petal_length_y', 'PETAL_WIDTH', 'SPECIES',
                                                ])
        assert all(idadf_iris.index == ida_nzpyida_join.index)
        assert idadf_iris.indexer == ida_nzpyida_join.indexer
    
    def test_join_only_index(self, idadf_iris, idadf_iris2):
        ida_nzpyida_join = idadf_iris.join(idadf_iris2, how='right', lsuffix=None, rsuffix='_copy')
        assert len(ida_nzpyida_join) == 150
        assert all(ida_nzpyida_join.columns == ['index', 'sepal_length', 'sepal_width',
                                                'petal_length', 'petal_width',
                                                'species', 'sepal_length_copy', 'sepal_width_copy',
                                                'petal_length_copy', 'PETAL_WIDTH', 'SPECIES',
                                                ])
        assert all(idadf_iris2.index == ida_nzpyida_join.index)
        assert idadf_iris2.indexer == ida_nzpyida_join.indexer