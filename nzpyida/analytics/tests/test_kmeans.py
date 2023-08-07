
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

from nzpyida.base import IdaDataBase
from nzpyida.analytics.model_manager import ModelManager
from nzpyida.analytics.predictive.kmeans import KMeans

from nzpyida.analytics.tests.conftest import MOD_NAME, OUT_TABLE_CLUST, OUT_TABLE_PRED
import pytest

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

def test_kmeans(idadb: IdaDataBase, mm: ModelManager, idf_train_clust,
                idf_test_clust, clear_up):
    assert idf_train_clust
    assert idf_test_clust

    model = KMeans(idadb, MOD_NAME)
    assert model
    assert not mm.model_exists(MOD_NAME)
    
    model.fit(idf_train_clust, k=3, out_table=OUT_TABLE_CLUST)
    assert mm.model_exists(MOD_NAME)

    pred = model.predict(idf_test_clust, out_table=OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == idadb.to_def_case(['ID', 'CLUSTER_ID', 'DISTANCE']))
    assert all(list(pred.as_dataframe()[idadb.to_def_case('CLUSTER_ID')].values))

    score = model.score(idf_test_clust, target_column="A")

    assert score
