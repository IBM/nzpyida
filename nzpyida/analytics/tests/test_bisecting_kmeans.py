
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

from nzpyida.frame import IdaDataFrame
from nzpyida.base import IdaDataBase
from nzpyida.analytics.model_manager import ModelManager
from nzpyida.analytics.predictive.bisecting_kmeans import BisectingKMeans

from nzpyida.analytics.tests.conftest import MOD_NAME, OUT_TABLE_PRED, TAB_NAME_TEST, \
    TAB_NAME_TRAIN, df_train_clust, df_test_clust
import pandas as pd
import pytest

OUT_TABLE_CLUST = "OUT_TABLE_CLUST"

@pytest.fixture(scope='module')
def mm(idadb: IdaDataBase):
    return ModelManager(idadb)

@pytest.fixture(scope='module')
def clear_up(idadb: IdaDataBase, mm: ModelManager):
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    if idadb.exists_table(OUT_TABLE_CLUST):
        idadb.drop_table(OUT_TABLE_CLUST)
    yield
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    if idadb.exists_table(OUT_TABLE_CLUST):
        idadb.drop_table(OUT_TABLE_CLUST)

def test_bisecting_kmeans(idadb: IdaDataBase, mm: ModelManager, clear_up):
    idf_train = idadb.as_idadataframe(df_train_clust, tablename=TAB_NAME_TRAIN, clear_existing=True, indexer='ID')
    idf_test = idadb.as_idadataframe(df_test_clust, tablename=TAB_NAME_TEST, clear_existing=True, indexer='ID')
    assert idf_train
    assert idf_test

    model = BisectingKMeans(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)
    
    model.fit(idf_train, distance='manhattan', max_iter=4, min_split=3, max_depth=2,
              rand_seed=4321, out_table=OUT_TABLE_CLUST)
    assert mm.model_exists(MOD_NAME)

    pred = model.predict(idf_test, level=5, out_table=OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == ['ID', 'CLUSTER_ID', 'DISTANCE'])
    assert all(list(pred.as_dataframe()['CLUSTER_ID'].values))

    score = model.score(idf_test, target_column="A")

    assert score
