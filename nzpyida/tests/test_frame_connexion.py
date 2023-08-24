#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Copyright (c) 2015-2023, IBM Corp.
# All rights reserved.
#
# Distributed under the terms of the BSD Simplified License.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

"""
Test module for connexion related functions of IdaDataFrame
"""
import pandas

class Test_Import(object):

    def test_idadf_as_dataframe(self, idadf):
        tmp = idadf.as_dataframe() # For avoiding overhead off loading several time
        assert isinstance(tmp, pandas.core.frame.DataFrame)
        assert list(tmp.columns) == list(idadf.columns)
        assert list(tmp.index) == list(idadf.index)
        assert tmp.name == idadf.tablename

class Test_ConnexionManagement(object):
    def test_idadf_save_as(self, idadf):
        pass

    def test_idadf_commit(self, idadf):
        pass

    def test_idadf_rollback(self, idadf):
        pass

    def test_idadf_close(self, idadf):
        pass

    def test_idadf_reopen(self, idadf):
        pass