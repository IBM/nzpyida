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

from nzpyida.analytics.predictive.association_rules import ARule
from nzpyida.base import IdaDataBase
from nzpyida.frame import IdaDataFrame
from nzpyida.analytics.model_manager import ModelManager
import pytest
from nzpyida.analytics.tests.conftest import MOD_NAME, TAB_NAME_TRAIN, \
    TAB_NAME_TEST, OUT_TABLE_PRED
import pandas as pd
from random import choice

@pytest.fixture(scope='module')
def mm(idadb: IdaDataBase):
    return ModelManager(idadb)

@pytest.fixture
def clean_up(idadb, mm):
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    yield
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)



@pytest.fixture
def idf_train(idadb: IdaDataBase):
    if idadb.exists_table(TAB_NAME_TRAIN):
        idadb.drop_table(TAB_NAME_TRAIN)
    purchases = [[1,2,3,4,5],
                 [2,4,6,7,10],
                 [3,5,8,9,10],
                 [4,6,7,8,10],
                 [1,2,3,7,10],
                 [2,3,4,6,9],
                 [5,6,7,8,10],
                 [1,3,4,6,9],
                 [1,2,3,8,9],
                 [3,5,7,9,10]]
    df = pd.DataFrame.from_dict({
        "ID": [ i//5 for i in range(50)],
        "PRODUCT": [item for sublist in purchases for item in sublist]

    })
    yield idadb.as_idadataframe(df, TAB_NAME_TRAIN)
    if idadb.exists_table(TAB_NAME_TRAIN):
        idadb.drop_table(TAB_NAME_TRAIN)

@pytest.fixture
def idf_test(idadb: IdaDataBase):
    if idadb.exists_table(TAB_NAME_TEST):
        idadb.drop_table(TAB_NAME_TEST)
    df = pd.DataFrame.from_dict({
        "TID": range(50),
        "ITEM": [choice(range(10)) for _ in range(50)]
    })
    yield idadb.as_idadataframe(df, TAB_NAME_TEST)
    if idadb.exists_table(TAB_NAME_TEST):
        idadb.drop_table(TAB_NAME_TEST)

def test_arule(idadb: IdaDataBase, mm: ModelManager, idf_test: IdaDataFrame, 
               idf_train: IdaDataFrame, clean_up):
    model = ARule(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME) 

    model.fit(idf_train, transaction_id_column='ID', item_column="PRODUCT", 
              support_type='absolute', support=2, confidence=0.4)
    assert mm.model_exists(MOD_NAME) 

    pred_ida = model.predict(idf_test, OUT_TABLE_PRED, max_conviction=1.25, 
                       max_affinity=0.6, min_lift=1.0, min_leverage=0.03)
    assert pred_ida
    assert all(pred_ida.columns == ['GID', 'TID', 'LHS_SID', 'RHS_SID', 'LHS_ITEMS', 'RHS_ITEMS', 'SUPPORT',
       'CONFIDENCE', 'LIFT', 'CONVICTION', 'AFFINITY', 'LEVERAGE'])
    pred = pred_ida.as_dataframe()
    assert all(pred["SUPPORT"].values >= 0.2)
    assert all(pred["CONFIDENCE"].values >= 0.4)
    assert all(pred["CONVICTION"].values <= 1.25)
    assert all(pred["AFFINITY"].values <= 0.6)
    assert all(pred["LIFT"].values >= 1.0)
    assert all(pred["LEVERAGE"].values >=0.03)

    assert model.describe()
    
