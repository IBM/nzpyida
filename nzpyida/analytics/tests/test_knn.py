
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
from nzpyida.analytics.model_manager import ModelManager
from nzpyida.frame import IdaDataFrame
from nzpyida.analytics.utils import map_to_props
from nzpyida.analytics.predictive.knn import KNeighborsClassifier
from nzpyida.analytics.tests.conftest import MOD_NAME, OUT_TABLE_PRED, OUT_TABLE_CM
from mock import Mock, patch

import pytest

ID_COLUMN = 'IMSI'
TARGET_COLUMN = 'IS_FRAUD'
TEMP_TABLE_NAME = "TEMP_NAME"

def get_params(process: str, new_params: dict=None):
    if process == 'fit':
        params = {
            'id': ID_COLUMN,
            'target': TARGET_COLUMN,
            'incolumn': None,
            'coldeftype': None,
            'coldefrole': None,
            'colpropertiestable': None,
            'model': MOD_NAME,
            'intable': TEMP_TABLE_NAME
        }
    elif process == 'pred':
        params = {
            'id': ID_COLUMN,
            'target': TARGET_COLUMN,
            'distance': 'euclidean',
            'k': 3,
            'stand': True,
            'fast': True,
            'weights': None,
            'model': MOD_NAME,
            'intable': TEMP_TABLE_NAME,
            'outtable': OUT_TABLE_PRED
        }
    elif process == 'score':
        params = {
            'pred_table': TEMP_TABLE_NAME,
            'true_table': TEMP_TABLE_NAME,
            'pred_id': 'ID',
            'true_id': ID_COLUMN,
            'pred_column': 'CLASS',
            'true_column': TARGET_COLUMN
        }
    elif process == 'cm':
        params = {
            'resulttable': TEMP_TABLE_NAME,
            'intable': TEMP_TABLE_NAME,
            'resultid': 'ID',
            'id': ID_COLUMN,
            'resulttarget': 'CLASS',
            'target': TARGET_COLUMN,
            'matrixTable': OUT_TABLE_CM
        }
    else:
        params = {}
    if new_params:
        for k in new_params:
            params[k] = new_params[k]
    return map_to_props(params)

# --- fixtures ---

@pytest.fixture(scope='module')
def idadb():
    idadb_mock = Mock()
    idadb_mock.ida_query.return_value = [0.6]
    return idadb_mock


@pytest.fixture(scope='module')
def model(idadb):
    return KNeighborsClassifier(idadb, model_name=MOD_NAME)


@pytest.fixture(scope='module')
def data():
    return Mock()

# --- fit tests ---

@pytest.mark.parametrize("new_params", [{},  
                                        {'id': 'id','target': 'target','incolumn': 'in','coldeftype': 'cdf',
                                        'coldefrole': 'cdr', 'colpropertiestable': 'cpt', 'model': MOD_NAME}])
@patch("nzpyida.analytics.predictive.predictive_modeling.materialize_df", return_value=(TEMP_TABLE_NAME, False))
@patch.object(ModelManager, "drop_model", return_value=0)
def test_train_model(mm, materialize_df, data, model, new_params):
    if new_params:
        model.fit(data, id_column=new_params['id'], target_column=new_params['target'], 
                  in_column=new_params['incolumn'], col_def_type=new_params['coldeftype'], 
                  col_def_role=new_params['coldefrole'], col_properties_table=new_params['colpropertiestable'])
    else:
        model.fit(data, id_column=ID_COLUMN, target_column=TARGET_COLUMN)
    params_s = get_params('fit', new_params)
    model.idadb.ida_query.assert_any_call(f'call NZA..{model.fit_proc}(\'{params_s}\')')
    model.idadb.drop_view.assert_not_called()


# --- predict tests ---


@pytest.mark.parametrize("new_params", [{},
                                        {'id': 'id','target': 'target', 'distance': 'far', 'k': 500, 
                                         'stand': False, 'fast': False, 'weights': 'heavy'}])
@patch("nzpyida.analytics.predictive.predictive_modeling.materialize_df", return_value=(TEMP_TABLE_NAME, False))
@patch.object(IdaDataFrame, "__init__", return_value=None)
def test_predict(idadf, materialize_df, data, model: KNeighborsClassifier, new_params):
    if new_params:
        model.predict(data, id_column=new_params['id'], target_column=new_params['target'], 
                      distance=new_params['distance'], k=new_params['k'], stand=new_params['stand'], 
                      fast=new_params['fast'], weights=new_params['weights'], out_table=OUT_TABLE_PRED)
    else:
        model.predict(data, id_column=ID_COLUMN, target_column=TARGET_COLUMN, out_table=OUT_TABLE_PRED)
    params_s = get_params('pred', new_params)
    model.idadb.ida_query.assert_called_with(f'call NZA..{model.predict_proc}(\'{params_s}\')')


# --- score tests ---


@pytest.mark.parametrize("new_params", [{},
                                        {'distance': 'far', 'k': 500, 'stand': False, 'fast': False, 
                                         'weights': 'heavy'}])
@patch("nzpyida.analytics.predictive.predictive_modeling.materialize_df", return_value=(TEMP_TABLE_NAME, False))
@patch.object(IdaDataFrame, "__init__", return_value=None)
def test_score_model(idadf, materialize_df, data, model, new_params):
    if new_params:
        model.score(data, id_column=ID_COLUMN, target_column=TARGET_COLUMN, 
                      distance=new_params['distance'], k=new_params['k'], stand=new_params['stand'], 
                      fast=new_params['fast'], weights=new_params['weights'])
    else:
        model.score(data, id_column=ID_COLUMN, target_column=TARGET_COLUMN)
    params_s = get_params('score')
    model.idadb.ida_query.assert_any_call(f'call NZA..{model.score_proc}(\'{params_s}\')')


# --- conf martix test ---


@pytest.mark.parametrize("new_params", [{},
                                        {'distance': 'far', 'k': 500, 'stand': False, 'fast': False, 
                                         'weights': 'heavy'}])
@patch("nzpyida.analytics.predictive.classification.materialize_df", return_value=(TEMP_TABLE_NAME, False))
@patch.object(IdaDataFrame, "__init__", return_value=None)
def test_conf_matrix(idadf, materialize_df, data, model, new_params):
    if new_params:
        model.conf_matrix(data, id_column=ID_COLUMN, target_column=TARGET_COLUMN, 
                      distance=new_params['distance'], k=new_params['k'], stand=new_params['stand'], 
                      fast=new_params['fast'], weights=new_params['weights'], out_matrix_table=OUT_TABLE_CM)
    else:
        model.conf_matrix(data, id_column=ID_COLUMN, target_column=TARGET_COLUMN, out_matrix_table=OUT_TABLE_CM)
    params_s = get_params('cm')
    model.idadb.ida_query.assert_any_call((f'call NZA..CONFUSION_MATRIX(\'{params_s}\')'))