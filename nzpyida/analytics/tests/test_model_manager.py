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
import pytest
from nzpyida.frame import IdaDataFrame
from nzpyida.base import IdaDataBase
from nzpyida.analytics.model_manager import ModelManager
from nzpyida.analytics.predictive.decision_trees import DecisionTreeClassifier
from nzpyida.analytics.tests.conftest import MOD_NAME, TAB_NAME_TRAIN, df_train

import pytest

MOD_NAME_COPY = MOD_NAME+'_COPY'
PRIVILEGES_OUTPUT1 = "INZAUSER  | TEST_MOD1  |   X X   X"""
PRIVILEGES_OUTPUT2 = "INZAUSER  | TEST_MOD1  | X X X   X"
PRIVILEGES_OUTPUT3 = "INZAUSER  | TEST_MOD1  |   X X   X"

@pytest.fixture(scope='module')
def clear_up(idadb: IdaDataBase):
    mm = ModelManager(idadb)
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if mm.model_exists(MOD_NAME_COPY):
        mm.drop_model(MOD_NAME_COPY)
    yield
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if mm.model_exists(MOD_NAME_COPY):
        mm.drop_model(MOD_NAME_COPY)


def test_model_manager(idadb, clear_up):
    mm = ModelManager(idadb)
    assert mm

    # test listing models
    models_list = mm.list_models()
    assert models_list.exists()
    begin_length = len(mm.list_models()) 

    # verify model does not exist
    assert not mm.model_exists(MOD_NAME)

    # create model and check if mm.model_exits shows it
    idf_train = idadb.as_idadataframe(df_train, tablename=TAB_NAME_TRAIN, clear_existing=True)
    DecisionTreeClassifier(idadb, model_name=MOD_NAME).fit(idf_train, id_column='ID', target_column='B')
    assert mm.model_exists(MOD_NAME)

    # test copy model
    mm.copy_model(name=MOD_NAME, copy_name=MOD_NAME_COPY)
    assert mm.model_exists(MOD_NAME_COPY)

    # test listing models 2
    assert len(mm.list_models()) == 2 + begin_length

    # test droping model
    mm.drop_model(MOD_NAME)
    assert not mm.model_exists(MOD_NAME)

    # test alter model
    mm.alter_model(MOD_NAME_COPY, name=MOD_NAME, owner="INZAUSER", description="DecTree model",
                   copyright="Copyright (c) 2023. IBM Corp. All rights reserved.", category="DecTree")
    
    assert mm.model_exists(MOD_NAME)

    lm_ida = mm.list_models()
    lm = lm_ida.as_dataframe()
    lm = lm[lm["MODELNAME"]==MOD_NAME]
    assert len(lm) == 1
    assert lm["OWNER"].iloc[0] == "INZAUSER"
    assert lm["DESCRIPTION"].iloc[0] == "DecTree model"
    assert lm["COPYRIGHT"].iloc[0] == "Copyright (c) 2023. IBM Corp. All rights reserved."
    assert lm["USERCATEGORY"].iloc[0] == "DecTree"
    assert lm["CREATOR"].iloc[0] == "ADMIN"
    assert lm["ALGORITHM"].iloc[0] == "Decision Tree"

    # test grant privileges

    mm.revoke_model(MOD_NAME, ["list", "update"], user=["INZAUSER"])

    assert PRIVILEGES_OUTPUT1 in mm.list_privileges()

    mm.grant_model(MOD_NAME, privilege=["list"], user=["INZAUSER"])

    assert PRIVILEGES_OUTPUT2 in mm.list_privileges()

    assert PRIVILEGES_OUTPUT3 in mm.list_privileges(grant=True)


