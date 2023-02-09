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
from nzpyida.base import IdaDataBase

@pytest.fixture(scope='session')
def idadb():
    nzpy_cfg = {
        "user":"admin",
        "password":"password", 
        "host":'9.30.57.160', 
        "port":5480, 
        "database":"telco", 
        "logLevel":0, 
        "securityLevel":1
        }

    return IdaDataBase(nzpy_cfg)

MOD_NAME = "TEST_MOD1"

OUT_TABLE_PRED = "OUT_TABLE_PRED"
OUT_TABLE_CM = "OUT_TABLE_CM"