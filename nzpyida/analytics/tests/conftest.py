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
import pandas as pd
from nzpyida.base import IdaDataBase
from random import choice

TAB_NAME_TEST = "TAB_NAME1"
TAB_NAME_TRAIN = "TAB_NAME2"

TAB_NAME_TEST_REG = "TAB_NAME3"
TAB_NAME_TRAIN_REG = "TAB_NAME4"

TAB_NAME_TEST_NOM = "TAB_NAME5"
TAB_NAME_TRAIN_NOM = "TAB_NAME6"

TAB_NAME_TEST_CLUST = "TAB_NAME7"
TAB_NAME_TRAIN_CLUST = "TAB_NAME8"

TAB_NAME_TEST_PURCH = "TAB_NAME9"
TAB_NAME_TRAIN_PURCH = "TAB_NAME10"

MOD_NAME = "TEST_MOD1"
MOD_NAME2 = "TEST_MOD2"


OUT_TABLE_PRED = "OUT_TABLE_PRED"
OUT_TABLE_PRED2 = "OUT_TABLE_PRED2"
OUT_TABLE_CLUST = "OUT_TABLE_CLUST"
OUT_TABLE_CM = "OUT_TABLE_CM"
OUT_TABLE_CV = "OUT_TABLE_CV"



def pytest_addoption(parser):
    """
    Definition of admissible options for the pytest command line
    """
    parser.addoption("--table", default="iris",
        help="Name of the table to test the dataset")
    parser.addoption("--dsn", default="BLUDB",
        help="Data Source Name")
    parser.addoption("--uid", default='',
        help="User ID")
    parser.addoption("--pwd", default='',
        help="Password")
    parser.addoption("--jdbc", default='',
        help="jdbc url string for JDBC connection")
    parser.addoption("--hostname", default='',
        help="hostname for nzpy connection")

@pytest.fixture(scope="session")
def idadb(request):
    """
    DataBase connection fixture, to be used for the whole testing session.
    Hold the main IdaDataBase object. Shall not be closed except by a
    pytest finalizer.
    """
    def fin():
        try:
            idadb.close()
        except:
            pass
    request.addfinalizer(fin)

    hostname = request.config.getoption('--hostname')
    jdbc = request.config.getoption('--jdbc')

    if hostname != '':
        try:
            idadb = IdaDataBase(
                dsn={'database': request.config.getoption('--dsn'), 'host': hostname, 'port': 5480},
                uid=request.config.getoption('--uid'),
                pwd=request.config.getoption('--pwd'),
                autocommit=True)
        except:
            raise
    elif jdbc != '':
        try:
            idadb = IdaDataBase(dsn=jdbc, autocommit=True)
        except:
            raise
    else:
        try:
            idadb = IdaDataBase(dsn=request.config.getoption('--dsn'),
                                        uid=request.config.getoption('--uid'),
                                        pwd=request.config.getoption('--pwd'),
                                        autocommit=True)
        except:
            raise
    return idadb

df_train = pd.DataFrame.from_dict({"ID": range(200),
                                   "A": [-1, -2, 3, 4, 2, -0.5, 0, 1, -2.1, 1.4]*20,
                                   "B": ['n', 'n', 'p', 'p', 'p', 'n', 'n', 'p', 'n', 'p']*20})
df_test = pd.DataFrame.from_dict({"ID": [0, 1, 2],
                                  "A": [2, 0.001, -2],
                                  "B": ['p', 'p', 'n']})

@pytest.fixture(scope='session')
def idf_train(idadb: IdaDataBase):
    yield idadb.as_idadataframe(df_train, tablename=TAB_NAME_TRAIN, indexer="ID",
                                clear_existing=True)
    if idadb.exists_table(TAB_NAME_TRAIN):
        idadb.drop_table(TAB_NAME_TRAIN)

@pytest.fixture(scope='session')
def idf_test(idadb: IdaDataBase):
    yield idadb.as_idadataframe(df_test, tablename=TAB_NAME_TEST, indexer="ID",
                                 clear_existing=True)
    if idadb.exists_table(TAB_NAME_TEST):
        idadb.drop_table(TAB_NAME_TEST)

df_train_reg = pd.DataFrame.from_dict(
    {
        "ID": range(100),
        "A": range(1, 201, 2),
        "T": [100] * 100,
        "B": range(2, 201, 2)
    }
)

df_test_reg = pd.DataFrame.from_dict(
    {
        "ID": [0, 1, 2, 3, 4],
        "A": [0, 3, 2222, -1000, 11111],
        "T": [1000] * 5,
        "B": [1, 4, 2223, -999, 11112]
    }
)

@pytest.fixture(scope='session')
def idf_train_reg(idadb: IdaDataBase):
    yield idadb.as_idadataframe(df_train_reg, tablename=TAB_NAME_TRAIN_REG, 
                                clear_existing=True, indexer='ID')
    if idadb.exists_table(TAB_NAME_TRAIN_REG):
        idadb.drop_table(TAB_NAME_TRAIN_REG)

@pytest.fixture(scope='session')
def idf_test_reg(idadb: IdaDataBase):
    yield idadb.as_idadataframe(df_test_reg, tablename=TAB_NAME_TEST_REG, 
                                clear_existing=True, indexer='ID')
    if idadb.exists_table(TAB_NAME_TEST_REG):
        idadb.drop_table(TAB_NAME_TEST_REG)

@pytest.fixture(scope='session')
def idf_train_nom(idadb: IdaDataBase):
    df_train_reg["C"] = ['n', 'p', 'n', 'p', 'n', 'p', 'n', 'p', 'n', 'p']*10
    yield idadb.as_idadataframe(df_train_reg, tablename=TAB_NAME_TRAIN_NOM, 
                                indexer="ID", clear_existing=True)
    if idadb.exists_table(TAB_NAME_TRAIN_NOM):
        idadb.drop_table(TAB_NAME_TRAIN_NOM)

@pytest.fixture(scope='session')
def idf_test_nom(idadb: IdaDataBase):
    df_test_reg["C"]=['n', 'n', 'p', 'p', 'n']
    yield idadb.as_idadataframe(df_test_reg, tablename=TAB_NAME_TEST_NOM, 
                                indexer="ID", clear_existing=True)
    if idadb.exists_table(TAB_NAME_TEST_NOM):
        idadb.drop_table(TAB_NAME_TEST_NOM)


df_train_clust = pd.DataFrame.from_dict(
    {
        "ID": range(99),
        "A": list(range(33)) + list(range(100, 133)) + list(range(200, 233))
    }
)

df_test_clust = pd.DataFrame.from_dict(
    {
        "ID": [0, 1, 2, 3],
        "A": [-1, 77, 150, 250]
    }
)

@pytest.fixture(scope='session')
def idf_train_clust(idadb: IdaDataBase):
    yield idadb.as_idadataframe(df_train_clust, tablename=TAB_NAME_TRAIN_CLUST, 
                                clear_existing=True, indexer='ID')
    if idadb.exists_table(TAB_NAME_TRAIN_CLUST):
        idadb.drop_table(TAB_NAME_TRAIN_CLUST)

@pytest.fixture(scope='session')
def idf_test_clust(idadb: IdaDataBase):
    yield idadb.as_idadataframe(df_test_clust, tablename=TAB_NAME_TEST_CLUST, 
                                clear_existing=True, indexer='ID')
    if idadb.exists_table(TAB_NAME_TEST_CLUST):
        idadb.drop_table(TAB_NAME_TEST_CLUST)

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
df_train_purch = pd.DataFrame.from_dict({
    "ID": [ i//5 for i in range(50)],
    "PRODUCT": [item for sublist in purchases for item in sublist]
})

df_test_purch = pd.DataFrame.from_dict({
        "TID": range(50),
        "ITEM": [choice(range(10)) for _ in range(50)]
    })

@pytest.fixture(scope="session")
def idf_train_purch(idadb: IdaDataBase):
    yield idadb.as_idadataframe(df_train_purch, TAB_NAME_TRAIN_PURCH, 
                                clear_existing=True, indexer="ID")
    if idadb.exists_table(TAB_NAME_TRAIN_PURCH):
        idadb.drop_table(TAB_NAME_TRAIN_PURCH)
    
@pytest.fixture(scope="session")
def idf_test_purch(idadb: IdaDataBase):
    yield idadb.as_idadataframe(df_test_purch, TAB_NAME_TEST_PURCH, 
                                clear_existing=True, indexer="ID")
    if idadb.exists_table(TAB_NAME_TEST_PURCH):
        idadb.drop_table(TAB_NAME_TEST_PURCH)