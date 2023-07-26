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
from nzpyida.analytics.auto_delete_context import AutoDeleteContext
from nzpyida.analytics.tests.conftest import OUT_TABLE_PRED
from nzpyida.analytics.transform.preparation import std_norm, impute_data, \
    random_sample, train_test_split

TAB_NAME_SPLIT_TRAIN = "TAB_NAME_SPLIT_TRAIN"
TAB_NAME_SPLIT_TEST = "TAB_NAME_SPLIT_TEST"

@pytest.fixture(scope='function')
def clean_up(idadb: IdaDataBase):
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    if idadb.exists_table(TAB_NAME_SPLIT_TRAIN):
        idadb.drop_table(TAB_NAME_SPLIT_TRAIN)
    if idadb.exists_table(TAB_NAME_SPLIT_TEST):
        idadb.drop_table(TAB_NAME_SPLIT_TEST)
    yield
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    if idadb.exists_table(TAB_NAME_SPLIT_TRAIN):
        idadb.drop_table(TAB_NAME_SPLIT_TRAIN)
    if idadb.exists_table(TAB_NAME_SPLIT_TEST):
        idadb.drop_table(TAB_NAME_SPLIT_TEST)

def test_std_norm(idadb: IdaDataBase, clean_up, idf_train):

    out_df = std_norm(idf_train, in_column=['A:S'], by_column=['B'], 
                      out_table=OUT_TABLE_PRED)
    assert out_df
    assert idadb.exists_table_or_view(OUT_TABLE_PRED)

    assert all(out_df.columns == ['B', 'ID', 'std_A'])

    assert len(out_df) == len(idf_train)


def test_random_sample(idadb: IdaDataBase, idf_train, clean_up):
    with AutoDeleteContext(idadb):
        sample_df = random_sample(idf_train, size=100, by_column=["B"], rand_seed=123)
        assert sample_df

        assert all(sample_df.columns == [ 'ID', 'A', 'B'])

        assert len(sample_df) == 100

        sample_df2 = random_sample(idf_train, size=100, by_column=["B"], rand_seed=123)
        sample_df3 = random_sample(idf_train, size=100, by_column=["B"])
        assert all((sample_df.head(10) == sample_df2.head(10))['ID'])
        assert not all((sample_df.head(10) == sample_df3.head(10))['ID'])

@pytest.mark.skip
def test_impute_data(idadb: IdaDataBase, idf_train, clean_up):
    pass


def test_train_test_split(idadb: IdaDataBase, idf_train, clean_up):
    train_df, test_df = train_test_split(idf_train, TAB_NAME_SPLIT_TRAIN, 
                                         TAB_NAME_SPLIT_TEST, fraction=0.8, id_column="ID")
    assert test_df
    assert test_df
    assert round(len(train_df)/len(idf_train), 1) == 0.8
    assert round(len(test_df)/len(idf_train), 1) == 0.2

    train_pdf = train_df['ID'].as_dataframe()
    assert not any(el in train_pdf.values for el in test_df["ID"].as_dataframe().values)

