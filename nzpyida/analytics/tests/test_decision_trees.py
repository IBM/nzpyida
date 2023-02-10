
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
from nzpyida.analytics.tests.conftest import MOD_NAME, OUT_TABLE_PRED, OUT_TABLE_CM, TAB_NAME_TEST, \
    TAB_NAME_TRAIN, df_train, df_test
import pandas as pd
import pytest

ID_COLUMN = 'ID'
TARGET_COLUMN = 'B'

@pytest.fixture(scope='module')
def mm(idadb: IdaDataBase):
    return ModelManager(idadb)

@pytest.fixture(scope='module')
def model(idadb: IdaDataBase, mm: ModelManager):
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    if idadb.exists_table(OUT_TABLE_CM):
        idadb.drop_table(OUT_TABLE_CM)
    
    idf_train = idadb.as_idadataframe(df_train, tablename=TAB_NAME_TRAIN, clear_existing=True)
    model = DecisionTreeClassifier(idadb, model_name=MOD_NAME)
    model.fit(idf_train, id_column=ID_COLUMN, target_column=TARGET_COLUMN)
    yield model

    ret = idadb.ida_query(f'call NZA..MODEL_EXISTS(\'model={MOD_NAME}\')')
    if not ret.empty and ret[0]:
        idadb.ida_query(f'call NZA..DROP_MODEL(\'model={MOD_NAME}\')')
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    if idadb.exists_table(OUT_TABLE_CM):
        idadb.drop_table(OUT_TABLE_CM)
    if idadb.exists_table(TAB_NAME_TEST):
        idadb.drop_table(TAB_NAME_TEST)
    if idadb.exists_table(TAB_NAME_TRAIN):
        idadb.drop_table(TAB_NAME_TRAIN)

@pytest.fixture(scope='module')
def data(idadb: IdaDataBase):
    idf_test = idadb.as_idadataframe(df_test, tablename=TAB_NAME_TEST, clear_existing=True)
    return idf_test

@pytest.fixture(scope='module')
def model_trained(idadb: IdaDataBase, data: IdaDataFrame, model: DecisionTreeClassifier):
    pred = model.predict(data, id_column=ID_COLUMN, target_column=TARGET_COLUMN, out_table=OUT_TABLE_PRED)
    return pred

@pytest.fixture(scope='module')
def model_score(idadb: IdaDataBase, data: IdaDataFrame, model: DecisionTreeClassifier):
    score = model.score(data, id_column=ID_COLUMN, target_column=TARGET_COLUMN)
    return score

@pytest.fixture(scope='module')
def model_cm(idadb: IdaDataBase, data: IdaDataFrame, model: DecisionTreeClassifier):
    cm_out = model.conf_matrix(in_df=data, id_column=ID_COLUMN, target_column=TARGET_COLUMN, 
                               out_matrix_table=OUT_TABLE_CM)
    return cm_out


def test_model_created(model):
    assert model

def test_model_trained(mm):
    assert mm.model_exists(MOD_NAME)

def test_predicted_df_columns(model_trained):
    assert all(model_trained.columns == ['ID', 'CLASS'])

def test_predicted_df_values(model_trained):
    assert list(model_trained['CLASS'].head().values) == ['n', 'n', 'p']

def test_model_score_value(model_score):
    assert 0.67 >= model_score >= 0.66 

def test_conf_matrix(model_cm):
    assert all(model_cm[0])

def test_conf_matrix_columns(model_cm):
    assert all(model_cm[0].columns == ['REAL', 'PREDICTION', 'CNT'])

def test_conf_matrix_length(model_cm):
    assert len(model_cm[0]) >= 3

def test_conf_matrix_sum_count(model_cm):
    assert sum(model_cm[0].head()["CNT"].values) == 3

def test_acc_value(model_cm):
    assert 0.67 >= model_cm[1] >= 0.66

def test_wacc_value(model_cm):
    assert model_cm[2] == 0.75
