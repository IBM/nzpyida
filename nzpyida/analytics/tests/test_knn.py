
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

from nzpyida.base import IdaDataBase
from nzpyida.analytics.model_manager import ModelManager
from nzpyida.analytics.predictive.knn import KNeighborsClassifier
from nzpyida.analytics.tests.conftest import MOD_NAME, OUT_TABLE_PRED, OUT_TABLE_CM
import pytest

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

def test_knn(idadb: IdaDataBase, mm: ModelManager, idf_train, idf_test, clear_up):
    model = KNeighborsClassifier(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)
    
    model.fit(idf_train, id_column="ID", target_column="B")
    assert mm.model_exists(MOD_NAME)

    pred = model.predict(idf_test, id_column="ID", out_table=OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == idadb.to_def_case(['ID', 'CLASS']))
    assert list(pred.head()[idadb.to_def_case('CLASS')].values) == ['p', 'n', 'n']

    score = model.score(idf_test, id_column="ID", target_column="B")

    assert score
    assert 0.67 >= score >= 0.66

    cm, acc, wacc = model.conf_matrix(idf_test, id_column='ID', target_column='B', out_matrix_table=OUT_TABLE_CM)
    assert all([cm, acc, wacc])
    assert all(cm.columns == idadb.to_def_case(['REAL', 'PREDICTION', 'CNT']))
    assert len(cm) >= 3
    assert sum(cm.as_dataframe()[idadb.to_def_case("CNT")].values) == 3
    assert wacc == 0.75
    assert 0.67 >= acc >= 0.66
