
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
from nzpyida.analytics.predictive.linear_regression import LinearRegression
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

@pytest.mark.skip
def test_linear_regression(idadb: IdaDataBase, mm: ModelManager, 
                           idf_train_reg, idf_test_reg, clear_up):
    model = LinearRegression(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)
    
    model.fit(idf_train_reg, id_column="ID", target_column="B")
    assert mm.model_exists(MOD_NAME)

    pred = model.predict(idf_test_reg, out_table=OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == idadb.to_def_case(['ID', 'B']))
    assert any(round(x) == y for x, y in zip(list(pred.as_dataframe()[idadb.to_def_case('B')].values),  [1, 4, 2223, -999, 11112]))

    score = model.score(idf_test_reg, target_column='B')

    assert score
    assert score < 0.001

    mse, mae, rse, rae = model.score_all(idf_test_reg, target_column='B')
    assert all([mse, mae, rse, rae])
