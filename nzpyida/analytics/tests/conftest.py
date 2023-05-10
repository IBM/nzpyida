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

@pytest.fixture(scope='session')
def idadb():
    nzpy_cfg = {
        "user":"username",
        "password":"*****", 
        "host":'hostname', 
        "port":5480, 
        "database":"database_name", 
        "logLevel":0, 
        "securityLevel":1
    }
    return IdaDataBase(nzpy_cfg)

df_train = pd.DataFrame.from_dict({"ID": range(1000),
                                   "A": [-1, -2, 3, 4, 2, -0.5, 0, 1, -2.1, 1.4]*100,
                                   "B": ['n', 'n', 'p', 'p', 'p', 'n', 'n', 'p', 'n', 'p']*100})
df_test = pd.DataFrame.from_dict({"ID": [0, 1, 2],
                                  "A": [2, 0.001, -2],
                                  "B": ['p', 'p', 'n']})


df_train_reg = pd.DataFrame.from_dict(
    {
        "ID": range(1000),
        "A": range(1, 2001, 2),
        "B": range(2, 2001, 2)
    }
)

df_test_reg = pd.DataFrame.from_dict(
    {
        "ID": [0, 1, 2, 3, 4],
        "A": [0, 3, 2222, -1000, 11111],
        "B": [1, 4, 2223, -999, 11112]
    }
)

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

TAB_NAME_TEST = "TAB_NAME1"
TAB_NAME_TRAIN = "TAB_NAME2"

MOD_NAME = "TEST_MOD1"

OUT_TABLE_PRED = "OUT_TABLE_PRED"
OUT_TABLE_CM = "OUT_TABLE_CM"
