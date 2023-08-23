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
from nzpyida.analytics.tests.conftest import MOD_NAME, OUT_TABLE_PRED
import pandas as pd
from random import choice

@pytest.fixture(scope='module')
def mm(idadb: IdaDataBase):
    return ModelManager(idadb)

@pytest.fixture(scope="module")
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

def test_arule(idadb: IdaDataBase, mm: ModelManager, idf_test_purch: IdaDataFrame, 
               idf_train_purch: IdaDataFrame, clean_up):
    model = ARule(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME) 

    model.fit(idf_train_purch, transaction_id_column='ID', item_column="PRODUCT", 
              support_type='absolute', support=2, confidence=0.4)
    assert mm.model_exists(MOD_NAME) 

    pred_ida = model.predict(idf_test_purch, OUT_TABLE_PRED, max_conviction=1.25, 
                       max_affinity=0.6, min_lift=1.0, min_leverage=0.03, transaction_id_column='TID', item_column='ITEM')
    assert pred_ida
    assert all(pred_ida.columns == idadb.to_def_case(['GID', 'TID', 'LHS_SID', 'RHS_SID', 'LHS_ITEMS', 'RHS_ITEMS', 'SUPPORT',
       'CONFIDENCE', 'LIFT', 'CONVICTION', 'AFFINITY', 'LEVERAGE']))
    pred = pred_ida.as_dataframe()
    assert all(pred[idadb.to_def_case("SUPPORT")].values >= 0.2)
    assert all(pred[idadb.to_def_case("CONFIDENCE")].values >= 0.4)
    assert all(pred[idadb.to_def_case("CONVICTION")].values <= 1.25)
    assert all(pred[idadb.to_def_case("AFFINITY")].values <= 0.6)
    assert all(pred[idadb.to_def_case("LIFT")].values >= 1.0)
    assert all(pred[idadb.to_def_case("LEVERAGE")].values >=0.03)

    assert model.describe()
    
