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
from nzpyida.analytics.predictive.glm import BernoulliRegressor, NegativeBinomialRegressor, BinomialRegressor, \
    GaussianRegressor, GammaRegressor, PoissonRegressor, WaldRegressor
from nzpyida.analytics.tests.conftest import MOD_NAME, OUT_TABLE_PRED
import pytest

@pytest.fixture(scope='module')
def mm(idadb: IdaDataBase):
    return ModelManager(idadb)

@pytest.fixture(scope='function')
def clear_up(idadb: IdaDataBase, mm: ModelManager):
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    yield
    if mm.model_exists(MOD_NAME):
        mm.drop_model(MOD_NAME)
    if idadb.exists_table(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)

def model_family(idadb: IdaDataBase, model_name: str):
    models = {
        'bernoulli': {"model": BernoulliRegressor(idadb, MOD_NAME),
                     "params": {'max_iter': 15}},
        'gaussian': {"model": GaussianRegressor(idadb, MOD_NAME),
                     "params": {}},
        'poisson': {"model": PoissonRegressor(idadb, MOD_NAME),
                     "params": {}},
        'binomial': {"model": BinomialRegressor(idadb, MOD_NAME),
                     "params": {'trials': "T"}},
        'negativebinomial': {"model": NegativeBinomialRegressor(idadb, MOD_NAME),
                     "params": {}},
        'wald': {"model": WaldRegressor(idadb, MOD_NAME),
                     "params": {}},
        'gamma': {"model": GammaRegressor(idadb, MOD_NAME),
                     "params": {}},
    }
    return models[model_name]

@pytest.mark.parametrize("model_name", ['bernoulli', 'gaussian', 'poisson', 
                                        'binomial', 'negativebinomial', 'wald', 'gamma'
                                        ])
def test_glm(idadb: IdaDataBase, mm: ModelManager, clear_up, model_name,
             idf_train_reg, idf_test_reg):
    model_info = model_family(idadb, model_name)
    
    model = model_info["model"]
    assert model
    assert not mm.model_exists(MOD_NAME)

    params = {
        'in_df': idf_train_reg,
        'id_column': "ID",
        'target_column': "B",
        'in_columns': None,  
        'intercept': True, 
        'interaction': '', 
        'family_param': -1, 
        'link': 'logit', 
        'link_param': 1, 
        'max_iter': 20, 
        'epsilon': 1e-3, 
        'tolerance': 1e-7, 
        'method': 'irls', 
        'trials': '', 
        'debug': False, 
        'col_def_type': None, 
        'col_def_role': None, 
        'col_properties_table': None
    }
    for k, v in model_info["params"].items():
        params[k] = v
    model.fit(**params)
    assert mm.model_exists(MOD_NAME)

    pred = model.predict(idf_test_reg, id_column="ID", out_table=OUT_TABLE_PRED)
    assert pred
    assert all(pred.columns == ['ID', idadb.to_def_case('PRED')])
    # assert any(round(x) == y for x, y in zip(list(pred.as_dataframe()['CLASS'].values),  [1, 4, 2223, -999, 11112]))

    score = model.score(idf_test_reg, id_column="ID", target_column="B")

    assert score

    mse, mae, rse, rae = model.score_all(idf_test_reg, id_column='ID', target_column='B')
    assert all([mse, mae, rse, rae])
