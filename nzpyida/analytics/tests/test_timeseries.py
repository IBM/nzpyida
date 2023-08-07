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

from nzpyida.analytics.predictive.timeseries import TimeSeries
from nzpyida.base import IdaDataBase
from nzpyida.analytics.model_manager import ModelManager
import pytest
from nzpyida.analytics.tests.conftest import MOD_NAME, OUT_TABLE_PRED
import pandas as pd
from math import sin

TAB_NAME = "TEST_TAB_NAME"

@pytest.fixture(scope='module')
def mm(idadb: IdaDataBase):
    return ModelManager(idadb)

@pytest.fixture
def clean_up(idadb, mm):
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    yield
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)


@pytest.fixture
def idf(idadb: IdaDataBase):
    if idadb.exists_table(TAB_NAME):
        idadb.drop_table(TAB_NAME)

    time_series = [sin(x)+x for x in range(200)]
    df = pd.DataFrame.from_dict({
        "TIME": range(200),
        "VALUE": time_series
    })
    yield idadb.as_idadataframe(df, TAB_NAME)

    if idadb.exists_table(TAB_NAME):
        idadb.drop_table(TAB_NAME)


def test_timeseries(idadb: IdaDataBase, mm: ModelManager, idf, clean_up):
    model = TimeSeries(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME) 

    outtab = model.fit_predict(idf, time_column="TIME", target_column="VALUE", 
                               out_table=OUT_TABLE_PRED, forecast_horizon='399')

    assert mm.model_exists(MOD_NAME) 
    assert outtab
    assert len(outtab) == 200
    assert round(outtab.head(10).iloc[-1]["VALUE"]) == round(sin(210)+210)
    assert round(outtab.tail().iloc[-1]["VALUE"]) == round(sin(399)+399)
