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
from nzpyida.analytics.predictive.bayesian_networks import TreeBayesNetwork, TreeBayesNetwork1G, \
     TreeBayesNetwork1G2P, TreeBayesNetwork2G, TreeAgumentedNetwork, BinaryTreeBayesNetwork, \
     MultiTreeBayesNetwork
from nzpyida.analytics.tests.conftest import MOD_NAME, OUT_TABLE_PRED, TAB_NAME_TEST, \
    TAB_NAME_TRAIN, df_train_reg, df_test_reg
import pytest

MOD_NAME2 = "TEST_MOD2"
OUT_TABLE_PRED2 = "OUT_TABLE_PRED2"

@pytest.fixture(scope='module')
def mm(idadb: IdaDataBase):
    return ModelManager(idadb)

@pytest.fixture(scope='function')
def clear_up(idadb: IdaDataBase, mm: ModelManager):
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if mm.model_exists(MOD_NAME2):
        mm.drop_model(MOD_NAME2)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    if idadb.exists_table(OUT_TABLE_PRED2):
        idadb.drop_table(OUT_TABLE_PRED2)
    yield
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    if mm.model_exists(MOD_NAME2):
        mm.drop_model(MOD_NAME2)
    if idadb.exists_table(OUT_TABLE_PRED2):
        idadb.drop_table(OUT_TABLE_PRED2)

@pytest.fixture(scope='module')
def idf_train_nom(idadb: IdaDataBase):
    TAB_NAME_TRAIN_NOM = TAB_NAME_TRAIN + "_NOM"
    df_train_reg["C"] = ['n', 'p', 'n', 'p', 'n', 'p', 'n', 'p', 'n', 'p']*100
    yield idadb.as_idadataframe(df_train_reg, tablename=TAB_NAME_TRAIN_NOM, clear_existing=True)
    if idadb.exists_table(TAB_NAME_TRAIN_NOM):
        idadb.drop_table(TAB_NAME_TRAIN_NOM)

@pytest.fixture(scope='module')
def idf_test_nom(idadb: IdaDataBase):
    TAB_NAME_TEST_NOM = TAB_NAME_TEST + "_NOM"
    df_test_reg["C"]=['n', 'n', 'p', 'p', 'n']
    yield idadb.as_idadataframe(df_test_reg, tablename=TAB_NAME_TEST_NOM, clear_existing=True)
    if idadb.exists_table(TAB_NAME_TEST_NOM):
        idadb.drop_table(TAB_NAME_TEST_NOM)

@pytest.fixture(scope='module')
def idf_train(idadb: IdaDataBase):
    yield idadb.as_idadataframe(df_train_reg, tablename=TAB_NAME_TRAIN, clear_existing=True)
    if idadb.exists_table(TAB_NAME_TRAIN):
        idadb.drop_table(TAB_NAME_TRAIN)

@pytest.fixture(scope='module')
def idf_test(idadb: IdaDataBase):
    yield idadb.as_idadataframe(df_test_reg, tablename=TAB_NAME_TEST, clear_existing=True)
    if idadb.exists_table(TAB_NAME_TEST):
        idadb.drop_table(TAB_NAME_TEST)

def test_tree_bayes_network(idadb: IdaDataBase, mm: ModelManager, idf_train: IdaDataFrame,
                            idf_test: IdaDataFrame, clear_up):
    model = TreeBayesNetwork(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)

    model.fit(idf_train, in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME)

    pred  = model.predict(idf_test, target_column="B", id_column="ID", out_table=OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == ["ID", "B_PRED"])
    # assert any(round(x) == y for x, y in zip(list(pred.as_dataframe()['B_PRED'].values),  [2, 4, 2223, -999, 11112]))


def test_binary_tree_bayes_network(idadb: IdaDataBase, mm: ModelManager, idf_train: IdaDataFrame ,
                                   idf_test: IdaDataFrame , clear_up):
    model = BinaryTreeBayesNetwork(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)

    model.fit(idf_train, in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME)

    pred  = model.predict(idf_test, target_column="B", id_column="ID", out_table=OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == ["ID", "B_PRED"])
    assert any(round(x) == y for x, y in zip(list(pred.head()['B_PRED'].values),  [2, 4, 2223, -999, 11112]))


def test_multi_tree_bayes_network(idadb: IdaDataBase, mm: ModelManager, idf_train_nom: IdaDataFrame ,
                                   idf_test_nom: IdaDataFrame , clear_up):
    model = MultiTreeBayesNetwork(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)

    model.fit(idf_train_nom, class_column= "C", in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME)

    pred  = model.predict(idf_test_nom, target_column="B", id_column="ID", out_table=OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == ["ID", "B_PRED"])
    assert any(round(x) == y for x, y in zip(list(pred.head()['B_PRED'].values),  [2, 4, 2223, -999, 11112]))


def test_tree_bayes_network_1g(idadb: IdaDataBase, mm: ModelManager, idf_train: IdaDataFrame,
                               clear_up):
    
    model = TreeBayesNetwork1G(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)

    outtab = model.grow(idf_train, in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME)
    assert outtab
    # TODO: check output of an outtab


def test_tree_bayes_network_2g(idadb: IdaDataBase, mm: ModelManager, idf_train: IdaDataFrame,
                               clear_up):
    
    model = TreeBayesNetwork2G(idadb, MOD_NAME2)
    assert model
    assert not mm.model_exists(MOD_NAME2)

    outtab = model.grow(idf_train, in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME2)
    assert outtab
    # TODO: check output of an outtab

def test_tree_bayes_network_1g2p(idadb: IdaDataBase, mm: ModelManager, idf_train: IdaDataFrame,
                               clear_up):
    
    model = TreeBayesNetwork1G2P(idadb, MOD_NAME2)
    assert model
    assert not mm.model_exists(MOD_NAME2)

    outtab = model.grow(idf_train, in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME2)
    assert outtab
    # TODO: check output of an outtab


def test_tree_agumented_network(idadb: IdaDataBase, mm: ModelManager, idf_train_nom: IdaDataFrame ,
                                   idf_test_nom: IdaDataFrame , clear_up):
    model = BinaryTreeBayesNetwork(idadb, MOD_NAME2)
    assert model
    assert not mm.model_exists(MOD_NAME2)

    model.fit(idf_train_nom, in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME2)

    model2 = TreeAgumentedNetwork(idadb, MOD_NAME)
    model2.fit(idf_train_nom, in_model=MOD_NAME2, class_column="C")

    assert mm.model_exists(MOD_NAME)
