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
from nzpyida.analytics.tests.conftest import MOD_NAME, MOD_NAME2, OUT_TABLE_PRED,\
      OUT_TABLE_PRED2
import pytest


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

def test_tree_bayes_network(idadb: IdaDataBase, mm: ModelManager, idf_train_reg,
                            idf_test_reg, clear_up):
    model = TreeBayesNetwork(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)

    model.fit(idf_train_reg, in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME)

    pred  = model.predict(idf_test_reg, target_column="B", id_column="ID", out_table=OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == ["ID", "B_" + idadb.to_def_case("PRED")])
    # assert any(round(x) == y for x, y in zip(list(pred.as_dataframe()['B_PRED'].values),  [2, 4, 2223, -999, 11112]))


def test_binary_tree_bayes_network(idadb: IdaDataBase, mm: ModelManager, idf_train_reg,
                                   idf_test_reg, clear_up):
    model = BinaryTreeBayesNetwork(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)

    model.fit(idf_train_reg, in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME)

    pred  = model.predict(idf_test_reg, target_column="B", id_column="ID", out_table=OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == ["ID", "B_" + idadb.to_def_case("PRED")])
    assert any(round(x) == y for x, y in zip(
        list(pred.head()['B_'+idadb.to_def_case('PRED')].values),  
        [2, 4, 2223, -999, 11112]))


def test_multi_tree_bayes_network(idadb: IdaDataBase, mm: ModelManager, idf_train_nom,
                                   idf_test_nom, clear_up):
    model = MultiTreeBayesNetwork(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)

    model.fit(idf_train_nom, class_column= "C", in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME)

    pred  = model.predict(idf_test_nom, target_column="B", id_column="ID", out_table=OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == ["ID", "B_" + idadb.to_def_case("PRED")])
    assert any(round(x) == y for x, y in zip(
        list(pred.head()['B_' + idadb.to_def_case('PRED')].values),  
        [2, 4, 2223, -999, 11112]))


def test_tree_bayes_network_1g(idadb: IdaDataBase, mm: ModelManager, idf_train_reg,
                               clear_up):
    model = TreeBayesNetwork1G(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)

    outtab = model.grow(idf_train_reg, in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME)
    assert outtab
    # TODO: check output of an outtab


def test_tree_bayes_network_2g(idadb: IdaDataBase, mm: ModelManager, idf_train_reg,
                               clear_up):
    
    model = TreeBayesNetwork2G(idadb, MOD_NAME2)
    assert model
    assert not mm.model_exists(MOD_NAME2)

    outtab = model.grow(idf_train_reg, in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME2)
    assert outtab
    # TODO: check output of an outtab

def test_tree_bayes_network_1g2p(idadb: IdaDataBase, mm: ModelManager, idf_train_reg,
                               clear_up):
    
    model = TreeBayesNetwork1G2P(idadb, MOD_NAME2)
    assert model
    assert not mm.model_exists(MOD_NAME2)

    outtab = model.grow(idf_train_reg, in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME2)
    assert outtab
    # TODO: check output of an outtab


def test_tree_agumented_network(idadb: IdaDataBase, mm: ModelManager, idf_train_nom,
                                   idf_test_nom, clear_up):
    model = BinaryTreeBayesNetwork(idadb, MOD_NAME2)
    assert model
    assert not mm.model_exists(MOD_NAME2)

    model.fit(idf_train_nom, in_columns=["A", "B"])
    assert mm.model_exists(MOD_NAME2)

    model2 = TreeAgumentedNetwork(idadb, MOD_NAME)
    model2.fit(idf_train_nom, in_model=MOD_NAME2, class_column="C")

    assert mm.model_exists(MOD_NAME)
