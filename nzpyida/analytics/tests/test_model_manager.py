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

import pytest

MOD_NAME = "TEST_MOD1"

@pytest.fixture(scope='module')
def mm(idadb: IdaDataBase):
    return ModelManager(idadb)

@pytest.fixture(scope='module')
def create_delete_model(idadb: IdaDataBase):
    df = IdaDataFrame(idadb, 'TRAINING_DATA2', indexer='IMSI')
    DecisionTreeClassifier(idadb, model_name=MOD_NAME).fit(df.loc[:10], id_column='IMSI', target_column='IS_FRAUD')
    yield
    ret = idadb.ida_query(f'call NZA..MODEL_EXISTS(\'model={MOD_NAME}\')')
    if not ret.empty and ret[0]:
        idadb.ida_query(f'call NZA..DROP_MODEL(\'model={MOD_NAME}\')')


def test_list_models(mm: ModelManager):
    models_list = mm.list_models()
    assert models_list.exists()

def test_model_not_exist(mm: ModelManager):
    assert not mm.model_exists(MOD_NAME)

def test_model_exist(mm: ModelManager, create_delete_model):
    assert mm.model_exists(MOD_NAME)


def test_drop_model(mm: ModelManager, create_delete_model):
    mm.drop_model(MOD_NAME)
    models_length = len(mm.list_models())
    assert models_length == 0 or MOD_NAME not in mm.list_models().head(models_length)[["MODELNAME"]].values
    