
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
from nzpyida.analytics.predictive.decision_trees import DecisionTreeClassifier
from nzpyida.analytics.tests.conftest import MOD_NAME, OUT_TABLE_PRED, OUT_TABLE_CM

import pytest


@pytest.fixture(scope='module')
def mm(idadb: IdaDataBase):
    return ModelManager(idadb)

@pytest.fixture(scope='module')
def model(idadb):
    print("Start model")
    model = DecisionTreeClassifier(idadb, model_name=MOD_NAME)
    yield model
    ret = idadb.ida_query(f'call NZA..MODEL_EXISTS(\'model={MOD_NAME}\')')
    if not ret.empty and ret[0]:
        idadb.ida_query(f'call NZA..DROP_MODEL(\'model={MOD_NAME}\')')
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    if idadb.exists_table(OUT_TABLE_CM):
        idadb.drop_table(OUT_TABLE_CM)
    print("Stop model")

@pytest.fixture(scope='module')
def data(idadb):
    df = IdaDataFrame(idadb, 'TRAINING_DATA2', indexer='IMSI')
    return df.loc[1000000:1100000]

def test_create_model(model):
    assert model

def test_train_model(data, model, mm):
    model.fit(data, id_column='IMSI', target_column='IS_FRAUD')
    assert mm.model_exists(MOD_NAME)

def test_predict(data, model):
    pred = model.predict(data, id_column='IMSI', target_column='IS_FRAUD', out_table=OUT_TABLE_PRED)
    assert pred

def test_score_model(data, model):
    score = model.score(data, id_column='IMSI', target_column='IS_FRAUD')
    assert score 

def test_conf_matrix(data, model):
    cm_out = model.conf_matrix(in_df=data, id_column='IMSI', target_column='IS_FRAUD', 
                              out_matrix_table=OUT_TABLE_CM)
    assert all(cm_out)
