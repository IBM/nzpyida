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
from nzpyida.analytics import AutoDeleteContext
from nzpyida.analytics.exploration.distribution import *


def test_moments(idadb, idf_train):
    with AutoDeleteContext(idadb):
        out_df = moments(idf_train, in_column="A", by_column="B")
        assert len(out_df) == 2
        assert all(out_df.columns == ["COLUMNNAME", "COUNTT", "AVERAGE", "VARIANCE", "STDDEV", 
                                      "SKEWNESS", "KURTOSIS", "MINIMUM", "MAXIMUM",  "B"])
        assert all((out_df["MAXIMUM"] - out_df["MINIMUM"]).head()>= 0)
        assert idf_train["A"]
        assert any(out_df["MINIMUM"].as_dataframe() == idf_train["A"].min())
        assert any(out_df["MAXIMUM"].as_dataframe() == idf_train["A"].max())


def test_quantile(idadb, idf_train):
    with AutoDeleteContext(idadb):
        out_df = quantile(idf_train, in_column="A", quantiles=[0.1, 0.5, 0.9])
        assert len(out_df) == 3
        assert all(out_df.columns == ["P", "VALUE"])
        assert all(out_df["VALUE"].as_dataframe() > idf_train["A"].min())
        assert all(out_df["VALUE"].as_dataframe() < idf_train["A"].max())


def test_outliers(idadb, idf_train):
    with AutoDeleteContext(idadb):
        out_df = outliers(idf_train, in_column="A", multiplier=0.1)
        assert len(out_df) > 0
        assert any(out_df["A"].as_dataframe() == idf_train["A"].max())
        assert any(out_df["A"].as_dataframe() == idf_train["A"].min())

def test_unitable(idadb, idf_train):
    with AutoDeleteContext(idadb):
        out_df = unitable(idf_train, in_column='A')
        assert len(out_df) > 0
        assert all(out_df.columns == ["A", "COUNT", "FREQ", "CUM"])
        assert out_df.loc[0]["A"].head().values[0] == idf_train["A"].min()
        assert out_df.loc[len(out_df)-1]["A"].head().values[0] == \
            idf_train["A"].max()
    

def test_bitable(idadb, idf_train_reg):
    with AutoDeleteContext(idadb):
        out_df = bitable(idf_train_reg, in_column=['A', 'B'], freq=True, cum=True)
        assert len(out_df) > 0
        assert all(out_df.columns == ['A', 'B', 'COUNT', 'FREQ', 'CUM'])


def test_histogram(idadb, idf_train):
    with AutoDeleteContext(idadb):
        out_df = histogram(idf_train, in_column='A', nbreaks=6, density=True, 
                           midpoints=True, freq=True, cum=True)
        assert len(out_df) == 6
        assert all(out_df.columns == ["IDX", "BLEFT", "BRIGHT", "COUNT", "DENSITY",
                                      "MIDPOINT", "FREQ", "CUM"])
        assert out_df.loc[0]["BLEFT"].head().values[0] == idf_train["A"].min()
        assert out_df.loc[len(out_df)-1]["BRIGHT"].head().values[0] == \
            idf_train["A"].max()

