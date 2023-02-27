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
import numpy as np
from nzpyida.base import IdaDataBase
from nzpyida.analytics.auto_delete_context import AutoDeleteContext
from nzpyida.analytics.tests.conftest import df_train, TAB_NAME_TRAIN
from nzpyida.analytics.transform.preparation import std_norm, impute_data, random_sample

TEST_TAB_NAME = "TEST_TAB_NAME"

@pytest.fixture(scope='function')
def clean_up(idadb: IdaDataBase):
    if idadb.exists_table(TEST_TAB_NAME):
        idadb.drop_table(TEST_TAB_NAME)
    yield
    if idadb.exists_table(TEST_TAB_NAME):
        idadb.drop_table(TEST_TAB_NAME)

@pytest.fixture(scope='module')
def idf(idadb: IdaDataBase):
    idf = idadb.as_idadataframe(df_train, tablename=TAB_NAME_TRAIN, clear_existing=True, indexer='ID')
    yield idf
    if idadb.exists_table(TAB_NAME_TRAIN):
        idadb.drop_table(TAB_NAME_TRAIN)

def test_std_norm(idadb: IdaDataBase, clean_up, idf):

    out_df = std_norm(idf, in_column=["A:S"], by_column=['B'], out_table=TEST_TAB_NAME)
    assert out_df
    assert idadb.exists_table_or_view(TEST_TAB_NAME)

    assert all(out_df.columns == ['B', 'ID', 'STD_A'])

    assert len(out_df) == len(idf)


def test_random_sample(idadb: IdaDataBase, idf, clean_up):
    with AutoDeleteContext(idadb):
        sample_df = random_sample(idf, size=300, by_column=["B"], rand_seed=123)
        assert sample_df

        assert all(sample_df.columns == [ 'ID', 'A', 'B'])

        assert len(sample_df) == 300

        sample_df2 = random_sample(idf, size=300, by_column=["B"], rand_seed=123)
        sample_df3 = random_sample(idf, size=300, by_column=["B"])
        assert all((sample_df.head(10) == sample_df2.head(10))['ID'])
        assert not all((sample_df.head(10) == sample_df3.head(10))['ID'])


def test_impute_data(idadb: IdaDataBase, clean_up):
    df_train['new'] = np.nan
    idf = idadb.as_idadataframe(df_train, tablename=TAB_NAME_TRAIN, clear_existing=True, indexer='ID')

    assert idf
    assert idadb.exists_table_or_view(TAB_NAME_TRAIN)
