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
from nzpyida.analytics.tests.conftest import MOD_NAME, TAB_NAME_TRAIN, df_train

import pytest

@pytest.fixture(scope='module')
def mm(idadb: IdaDataBase):
    return ModelManager(idadb)

@pytest.fixture(scope='module')
def clear_up(idadb: IdaDataBase, mm: ModelManager):
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    yield
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)


def test_model_manager(idadb, clear_up):
    mm = ModelManager(idadb)
    assert mm

    # test listing models
    models_list = mm.list_models()
    assert models_list.exists()

    # verify model does not exist
    assert not mm.model_exists(MOD_NAME)

    # create model and check if mm.model_exits shows it
    idf_train = idadb.as_idadataframe(df_train, tablename=TAB_NAME_TRAIN, clear_existing=True)
    DecisionTreeClassifier(idadb, model_name=MOD_NAME).fit(idf_train, id_column='ID', target_column='B')
    assert mm.model_exists(MOD_NAME)

    # test droping model
    mm.drop_model(MOD_NAME)
    models_length = len(mm.list_models())
    assert MOD_NAME not in mm.list_models().head(models_length)[["MODELNAME"]].values
