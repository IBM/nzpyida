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
import pytest
import numpy as np
from nzpyida.base import IdaDataBase
from nzpyida.analytics.model_manager import ModelManager
from nzpyida.analytics.predictive.decision_trees import DecisionTreeClassifier
from nzpyida.analytics.tests.conftest import MOD_NAME, OUT_TABLE_PRED, OUT_TABLE_CM, TAB_NAME_TEST, \
    TAB_NAME_TRAIN, df_test, df_train
from nzpyida.analytics.tests.conftest import df_train, TAB_NAME_TRAIN, \
    TAB_NAME_TEST
from nzpyida.analytics import AutoDeleteContext

@pytest.fixture(scope='module')
def mm(idadb: IdaDataBase):
    return ModelManager(idadb)

@pytest.fixture(scope='module')
def clear_up(idadb: IdaDataBase, mm: ModelManager):
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    if idadb.exists_table(OUT_TABLE_CM):
        idadb.drop_table(OUT_TABLE_CM)
    yield
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    if idadb.exists_table(OUT_TABLE_CM):
        idadb.drop_table(OUT_TABLE_CM)
    
def test_cross_validation(idadb: IdaDataBase, clear_up):
    idf = idadb.as_idadataframe(df_train, tablename=TAB_NAME_TRAIN, clear_existing=True, 
                                indexer="ID")

    model = DecisionTreeClassifier(idadb, MOD_NAME)
    with AutoDeleteContext(idadb):
        df_cv, acc = model.cross_validation(idf, id_column="ID", target_column="B")
        assert acc
        assert acc > 0.99

        assert df_cv
        assert len(df_cv) == len(idf)
        assert all(df_cv.columns == ["ID", "CLASS", "FOLD"])