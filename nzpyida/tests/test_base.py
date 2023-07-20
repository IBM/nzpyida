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
Test module for IdaDataFrameObjects
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import pandas
import pytest
import six
from nzpyida.analytics import DecisionTreeClassifier
from flaky import flaky

class Test_DataBaseExploration(object):

    def test_idadb_current_schema(self, idadb):
        assert isinstance(idadb.current_schema, six.string_types)

    def test_idadb_show_tables(self, idadb):
        test = idadb.show_tables()
        assert isinstance(test, pandas.core.frame.DataFrame)
        assert (list(test.columns) == ['TABSCHEMA','TABNAME','OWNER','TYPE'])

    def test_idadb_show_tables_all(self, idadb):
        test = idadb.show_tables(True)
        assert isinstance(test, pandas.core.frame.DataFrame)
        assert (list(test.columns) == ['TABSCHEMA','TABNAME','OWNER','TYPE'])

    def test_idadb_show_models(self, idadb):
        df = idadb.show_models()
        assert isinstance(df, pandas.core.frame.DataFrame)
        if not df.empty:
            show_model_columns = ['MODELNAME', 'OWNER', 'CREATED', 'STATE',
                                  'MININGFUNCTION', 'ALGORITHM', 'USERCATEGORY']
            if not idadb._is_netezza_system():
                show_model_columns =  ['MODELSCHEMA'] + show_model_columns
            assert (list(df.columns) == idadb.to_def_case(show_model_columns))

    def test_idadb_exists_table_or_view_positive0(self, idadb, idadf, idaview):
        assert(idadb.exists_table_or_view(idadf.name) == 1)
        assert(idadb.exists_table_or_view(idaview.name) == 1)

    @pytest.mark.skipif("'netezza' in config.getvalue('jdbc') or config.getvalue('hostname') != ''")
    def test_idadb_exists_table_or_view_positive(self, idadb, idadf, idaview, idageodf_county_view):
        assert(idadb.exists_table_or_view(idadf.name) == 1)
        assert(idadb.exists_table_or_view(idaview.name) == 1)
        assert(idadb.exists_table_or_view(idageodf_county_view.name) == 1)

    def test_idadb_exists_table_or_view_negative(self, idadb):
        assert(idadb.exists_table_or_view("NOT_EXISTING_DATA_FRAME_130530496_4860385960") == 0)
        assert(idadb.exists_table_or_view("NOT_EXISTING_130530496_4860385960") == 0)

    #def test_idadb_exists_table_or_view_error(self, idadb):
    #    with pytest.raises(TypeError):
    #        idadb.exists_table_or_view("ST_INFORMTN_SCHEMA.ST_UNITS_OF_MEASURE")
    # Note: We cannot assume that "ST_INFORMTN_SCHEMA.ST_UNITS_OF_MEASURE" exists and is of type "A"

    def test_idadb_exists_table_positive(self, idadb, idadf):
        assert(idadb.exists_table(idadf.name) == 1)

    def test_idadb_exists_table_negative(self, idadb):
        assert(idadb.exists_table("NOT_EXISTING_FRAME_130530496_4860385960") == 0)

    def test_idadb_exists_table_error(self, idadb, idaview):
        with pytest.raises(TypeError):
            idadb.exists_table(idaview.name)

    def test_idadb_exists_view_positive(self, idadb, idaview):
        assert(idadb.exists_view(idaview.name) == 1)

    def test_idadb_exists_view_negative(self, idadb):
        assert(idadb.exists_view("NOT_EXISTING_VIEW_130530496_4860385960") == 0)

    def test_idadb_exists_view_error(self, idadb, idadf):
        with pytest.raises(TypeError):
            idadb.exists_view(idadf.name)

    def test_idadb_exists_model_positive(self, idadb, idadf_tmp):
        idadb.add_column_id(idadf_tmp, destructive=True)
        # Create a simple DecisionTreeClassifier model
        model = DecisionTreeClassifier(idadb=idadb, model_name="MODEL_58979457385")
        model.fit(idadf_tmp, target_column='"species"')
        assert(idadb.exists_model("MODEL_58979457385") == 1)
        try :
            idadb.drop_model(model.model_name)
        except : pass

    @pytest.mark.skipif("'netezza' in config.getvalue('jdbc') or config.getvalue('hostname') != ''")
    def test_idadb_exists_model_with_schema_positive(self, idadb, idadf_tmp):
        idadb.add_column_id(idadf_tmp, destructive=True)
        # Create a simple DecisionTreeClassifier model
        kmeans = DecisionTreeClassifier(idadb=idadb, model_name="MYSCHEMA.MODEL_45738558979")
        kmeans.fit(idadf_tmp, target_column='"species"')
        assert(idadb.exists_model("MYSCHEMA.MODEL_45738558979") == 1)
        try :
            idadb.drop_model(kmeans.modelname)
        except : pass

    @pytest.mark.skipif("'netezza' in config.getvalue('jdbc') or config.getvalue('hostname') != ''")
    def test_idadb_exists_model_with_schema_positive_mixed_case(self, idadb, idadf_tmp):
        idadb.add_column_id(idadf_tmp, destructive=True)
        # Create a simple DecisionTreeClassifier model
        kmeans = DecisionTreeClassifier(idadb=idadb, model_name="mySchema.Model_85584573979")
        kmeans.fit(idadf_tmp, target_column='"species"')
        assert(idadb.exists_model("mySchema.Model_85584573979") == 1)
        assert(idadb.exists_model("MYSCHEMA.MODEL_85584573979") == 1)
        assert(idadb.exists_model("myschema.model_85584573979") == 1)
        try :
            idadb.drop_model(kmeans.modelname)
        except : pass

    @flaky
    def test_idadb_exists_model_negative(self, idadb):
        if idadb.exists_model("MODEL_58979457385"):
            idadb.drop_model("MODEL_58979457385")
        assert(idadb.exists_model("MODEL_58979457385") == 0)

    def test_idadb_exists_model_with_schema_negative(self, idadb):
        if idadb.exists_model("SCHEMA_58979457385.MODEL_58979457385"):
            idadb.drop_model("SCHEMA_58979457385.MODEL_58979457385")
        assert(idadb.exists_model("SCHEMA_58979457385.MODEL_58979457385") == 0)

    def test_idadb_exists_model_error(self, idadb, idadf):
        with pytest.raises(TypeError):
            idadb.exists_model(idadf.name)

    def test_idadb_is_table_or_view_positive(self, idadb, idadf, idaview):
        assert(idadb.is_table_or_view(idadf.name) == 1)
        assert(idadb.is_table_or_view(idaview.name) == 1)

    #def test_idadb_is_table_or_view_negative(self, idadb):
    #    assert(idadb.is_table_or_view("ST_INFORMTN_SCHEMA.ST_UNITS_OF_MEASURE") == 0)

    def test_idadb_is_table_or_view_error(self, idadb):
        with pytest.raises(ValueError):
            idadb.is_table_or_view("NOT_EXISTING_DATA_FRAME_130530496_4860385960")

    def test_idadb_is_table_positive(self, idadb, idadf):
        assert(idadb.is_table(idadf.name) == 1)

    def test_idadb_is_table_negative(self, idadb, idaview):
        assert(idadb.is_table(idaview.name) == 0)

    def test_idadb_is_table_error(self, idadb):
        with pytest.raises(ValueError):
            idadb.is_table("NOT_EXISTING_DATA_FRAME_130530496_4860385960")

    def test_idadb_is_view_positive(self, idadb, idaview):
        assert(idadb.is_view(idaview.name) == 1)

    def test_idadb_is_view_negative(self, idadb, idadf):
        assert(idadb.is_view(idadf.name) == 0)

    def test_idadb_is_view_error(self, idadb):
        with pytest.raises(ValueError):
            idadb.is_view("NOT_EXISTING_VIEW_130530496_4860385960")

    def test_idadb_is_model_positive(self, idadb, idadf_tmp):
        idadb.add_column_id(idadf_tmp, destructive = True)
        # Create a simple DecisionTreeClassifier model
        kmeans = DecisionTreeClassifier(idadb=idadb, model_name="Model_85584573777")
        kmeans.fit(idadf_tmp, target_column='species')
        assert(idadb.is_model(kmeans.model_name) == 1)
        try : idadb.drop_model(kmeans.model_name)
        except : pass

    def test_idadb_is_model_negative(self, idadb, idadf, idaview):
        assert(idadb.is_model(idadf.name) == 0)
        assert(idadb.is_model(idaview.name) == 0)
        if not idadb._is_netezza_system():
            assert(idadb.is_model("ST_INFORMTN_SCHEMA.ST_UNITS_OF_MEASURE") == 0)

    def test_idadb_is_model_error(self, idadb):
        with pytest.raises(ValueError):
            idadb.is_model("NOTEXISTINGOBJECT_496070383095079384063739509")

# List of functions that do not need to be tested :
# i.e. the execution of everything here rely on it
# _prepare_and_execute
# _call_stored_procedure
# _autocommit
# _check_connection
# _exists
# _is
# _drop


