
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
from nzpyida.analytics.model_manager import ModelManager
from nzpyida.analytics.predictive.naive_bayes import NaiveBayesClassifier
from nzpyida.analytics.tests.conftest import MOD_NAME, OUT_TABLE_PRED, OUT_TABLE_CM
import pandas as pd
import pytest

TAB_NAME_TEST = "TAB_NAME1"
TAB_NAME_TRAIN = "TAB_NAME2"
MOD_NAME = "MOD_NAME1"

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

def test_naive_bayes(idadb: IdaDataBase, mm: ModelManager, clear_up):
    df_train = pd.DataFrame.from_dict({"ID": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                       "A": [-1, -2, 3, 4, 2, -0.5, 0, 1, -2.1, 1.4],
                       "B": ['n', 'n', 'p', 'p', 'p', 'n', 'n', 'p', 'n', 'p']})
    df_test = pd.DataFrame.from_dict({"ID": [10, 11, 12],
                            "A": [-1, 0.0001, 1],
                            "B": ['n', 'p', 'p']})
    idf_train = idadb.as_idadataframe(df_train, tablename=TAB_NAME_TRAIN, clear_existing=True)
    idf_test = idadb.as_idadataframe(df_test, tablename=TAB_NAME_TEST, clear_existing=True)
    assert idf_train
    assert idf_test

    model = NaiveBayesClassifier(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)
    
    model.fit(idf_train, id_column="ID", target_column="B")
    assert mm.model_exists(MOD_NAME)

    pred = model.predict(idf_test, id_column="ID", target_column="B", out_table=OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == ['ID', 'CLASS'])
    assert list(pred['CLASS'].head().values) == ['n', 'n', 'p']

    score = model.score(idf_test, id_column="ID", target_column="B")

    assert score
    assert 0.67 >= score >= 0.66

    cm, acc, wacc = model.conf_matrix(idf_test, id_column='ID', target_column='B', out_matrix_table=OUT_TABLE_CM)
    assert all([cm, acc, wacc])
    assert all(cm.columns == ['REAL', 'PREDICTION', 'CNT'])
    assert len(cm) >= 3
    assert sum(cm.head()["CNT"].values) == 3
    assert wacc == 0.75
    assert acc == score
