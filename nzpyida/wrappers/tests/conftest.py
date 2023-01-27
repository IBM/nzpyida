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
        "host":'127.0.0.1', 
        "port":5480, 
        "database":"telco", 
        "logLevel":0, 
        "securityLevel":1
        }

    return IdaDataBase(nzpy_cfg)
