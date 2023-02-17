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
        "user":"admin",
        "password":"password", 
        "host":'127.0.0.1', 
        "port":5480, 
        "database":"wrappers_tests", 
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

TAB_NAME_TEST = "TAB_NAME1"
TAB_NAME_TRAIN = "TAB_NAME2"

MOD_NAME = "TEST_MOD1"

OUT_TABLE_PRED = "OUT_TABLE_PRED"
OUT_TABLE_CM = "OUT_TABLE_CM"
