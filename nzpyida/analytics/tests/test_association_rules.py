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
    df = pd.DataFrame.from_dict({
        "ID": [ i//10 for i in range(1000)],
        "PRODUCT": [choice(range(100)) for _ in range(1000)]

    })
    yield idadb.as_idadataframe(df, TAB_NAME_TRAIN)
    if idadb.exists_table(TAB_NAME_TRAIN):
        idadb.drop_table(TAB_NAME_TRAIN)

@pytest.fixture
def idf_test(idadb: IdaDataBase):
    if idadb.exists_table(TAB_NAME_TEST):
        idadb.drop_table(TAB_NAME_TEST)
    df = pd.DataFrame.from_dict({
        "TID": range(100),
        "ITEM": [choice(range(100)) for _ in range(100)]
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
              support=1, confidence=0.1)
    assert mm.model_exists(MOD_NAME) 

    pred = model.predict(idf_test, OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == ['GID', 'TID', 'LHS_SID', 'RHS_SID', 'LHS_ITEMS', 'RHS_ITEMS', 'SUPPORT',
       'CONFIDENCE', 'LIFT', 'CONVICTION', 'AFFINITY', 'LEVERAGE'])
    pred_len = len(pred)
    assert all(pred.head(pred_len)["SUPPORT"].values >=0.01)
    assert all(pred.head(pred_len)["CONFIDENCE"].values >=0.1)

    # Describing model tends to hang
    #assert model.describe()
    
