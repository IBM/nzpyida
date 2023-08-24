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
Test module for IdaGeoDataFrame
"""
import pandas
import pytest
import six

from copy import deepcopy

from nzpyida import IdaDataFrame
from nzpyida import IdaSeries
from nzpyida import IdaGeoDataFrame
from nzpyida import IdaGeoSeries


GEO_TABLE_NAME1 = "GEO_TEST_TABLE1"
GEO_TABLE_NAME2 = "GEO_TEST_TABLE2"
GEO_COLUMN_NAME = "THE_GEOM"
INDEXER_COLUMN = "OBJECTID"
VARCHAR_COLUMN = "NAME"

@pytest.fixture(scope='module')
def idageodf1(idadb, is_esri):
    COLUMN_TYPE = "ST_GEOMETRY" if is_esri else "VARCHAR"
    prep_table1_commands = f"""
DROP TABLE {GEO_TABLE_NAME1} IF EXISTS;
CREATE TABLE {GEO_TABLE_NAME1} ("{INDEXER_COLUMN}" INTEGER, 
"{VARCHAR_COLUMN}" VARCHAR(200), "{GEO_COLUMN_NAME}" {COLUMN_TYPE}(200));
INSERT INTO {GEO_TABLE_NAME1} VALUES 
(1, 'SQ1', inza..ST_WKTToSQL('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'));
INSERT INTO {GEO_TABLE_NAME1} VALUES 
(2, 'SQ2', inza..ST_WKTToSQL('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'));
"""
    idadb.ida_query(prep_table1_commands)
    yield IdaGeoDataFrame(idadb, GEO_TABLE_NAME1, 
                          indexer=INDEXER_COLUMN, geometry=GEO_COLUMN_NAME)
    idadb.ida_query(f"DROP TABLE {GEO_TABLE_NAME1} IF EXISTS")

@pytest.fixture(scope='module')
def idageodf2(idadb, is_esri):
    COLUMN_TYPE = "ST_GEOMETRY" if is_esri else "VARCHAR"
    prep_table2_commands = f"""
DROP TABLE {GEO_TABLE_NAME2} IF EXISTS;
CREATE TABLE {GEO_TABLE_NAME2} ("{INDEXER_COLUMN}"  INTEGER, "{GEO_COLUMN_NAME}" {COLUMN_TYPE}(200));
INSERT INTO {GEO_TABLE_NAME2} VALUES 
(1, inza..ST_WKTToSQL('POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))'));
INSERT INTO {GEO_TABLE_NAME2} VALUES 
(2, inza..ST_WKTToSQL('POLYGON ((1 1, 11 1, 11 11, 1 11, 1 1))'));
INSERT INTO {GEO_TABLE_NAME2} VALUES 
(3, inza..ST_WKTToSQL('POLYGON ((-1 -1, -2 -1, -2 -2, -1 -2, -1 -1))'));
"""
    idadb.ida_query(prep_table2_commands)
    yield IdaGeoDataFrame.from_IdaDataFrame(IdaDataFrame(
        idadb, GEO_TABLE_NAME2, indexer=INDEXER_COLUMN), geometry=GEO_COLUMN_NAME)
    idadb.ida_query(f"DROP TABLE {GEO_TABLE_NAME2} IF EXISTS")

class Test_IdaGeoDataFrame(object):
    def test_idageodf_set_geometry_error(self, idageodf1):
        with pytest.raises(KeyError):
            idageodf1.set_geometry('not a column in the Ida')
        with pytest.raises(TypeError):
            idageodf1.set_geometry(INDEXER_COLUMN)
        with pytest.raises(TypeError):
            idageodf1.set_geometry(VARCHAR_COLUMN)

    def test_idageodf_set_geometry_success(self, idageodf1):
        assert(isinstance(idageodf1.geometry, IdaGeoSeries))
        assert(idageodf1.geometry.column == GEO_COLUMN_NAME)

    def test_idageodf_nondestructive_geometry_column_deletion(
            self, idageodf1):
        ida_spare = idageodf1[[INDEXER_COLUMN, VARCHAR_COLUMN, GEO_COLUMN_NAME]]
        assert(ida_spare.geometry.column == GEO_COLUMN_NAME)
        del(ida_spare[GEO_COLUMN_NAME])
        with pytest.raises(AttributeError):
            ida_spare.geometry

    def test_idageodf_fromIdaDataFrame(self, idageodf2):
        assert(isinstance(idageodf2, IdaGeoDataFrame))

    def test_idageodf_column_projection_IdaGeoSeries(self, idageodf1):
        assert "THE_GEOM" in idageodf1.columns
        ida = idageodf1[GEO_COLUMN_NAME]
        assert(isinstance(ida, IdaGeoSeries))
        assert(ida.column == GEO_COLUMN_NAME)
        assert(ida.indexer == INDEXER_COLUMN)

    def test_idageodf_column_projection_IdaSeries(self, idageodf1):
        ida = idageodf1[VARCHAR_COLUMN]
        assert(isinstance(ida, IdaSeries))
        assert(ida.column == VARCHAR_COLUMN)
        assert(ida.indexer == INDEXER_COLUMN)

    def test_idageodf_column_projection_IdaGeoDataFrame(self, idageodf1):
        columns = [VARCHAR_COLUMN, GEO_COLUMN_NAME]
        ida = idageodf1[columns]
        assert(isinstance(ida, IdaGeoDataFrame))
        assert(all(ida.columns) == all(columns))
        assert(ida.geometry.column == GEO_COLUMN_NAME)

    def test_idageodf_column_projection_IdaDataFrame(self, idageodf1):
        columns = [VARCHAR_COLUMN, INDEXER_COLUMN]
        ida = idageodf1[columns]
        assert(isinstance(ida, IdaDataFrame))
        assert(all(ida.columns) == all(columns))
        with pytest.raises(AttributeError):
            ida.geometry

    def test_idageodf_geospatial_method_call_carried_on_IdaGeoSeries(
            self, idageodf1):
        attribute = 'area'
        assert(not hasattr(IdaGeoDataFrame, attribute))
        assert(hasattr(IdaGeoSeries, attribute))
        assert(idageodf1.__getattr__(attribute))

    def test_idageodf_getattr_unresolved(self, idageodf1):
        with pytest.raises(AttributeError):
            idageodf1.__getattr__('not_an_attribute')

    def test_idageodf_equals(self, idageodf1, idageodf2):
        ida = idageodf1.equals(idageodf2)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_distance(self, idageodf1,idageodf2):
        ida = idageodf1.distance(idageodf2)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_crosses(self,idageodf1,idageodf2):
        ida = idageodf1.crosses(idageodf2)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_intersects(self, idageodf1, idageodf2):
        ida = idageodf2.intersects(idageodf1)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_overlaps(self, idageodf1, idageodf2):
        ida = idageodf2.overlaps(idageodf1)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_touches(self, idageodf1, idageodf2):
        ida = idageodf1.touches(idageodf2)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_disjoint(self, idageodf1, idageodf2):
        ida = idageodf1.disjoint(idageodf2)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_contains(self, idageodf1,idageodf2):
        ida  = idageodf2.contains(idageodf1)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_within(self, idageodf1, idageodf2):
        ida  = idageodf1.within(idageodf2)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_mbr_intersects(self, idageodf1, idageodf2):
        ida  = idageodf1.mbr_intersects(idageodf2)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_difference(self, idageodf1,idageodf2):
        ida = idageodf2.difference(idageodf1)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_intersection(self, idageodf1,idageodf2):
        ida = idageodf1.intersection(idageodf2)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_union(self, idageodf1,idageodf2):
        ida = idageodf1.union(idageodf2)
        assert (isinstance(ida, IdaGeoDataFrame))
        assert len(ida.head())

    def test_idageodf_binary_operation_handler_non_geometry_column(
            self, idageodf1,idageodf2):
        with pytest.raises(TypeError):
            idageodf1._binary_operation_handler(
                idageodf2,
                db2gse_function='inza..ST_AGEOSPATIALFUNCTION',
                valid_types=['ST_POINT'])

    def test_idageodf_max_distance(self, idageodf1, idageodf2):
         res = idageodf1.distance(idageodf2, 'kilometer')
         assert res['RESULT'].max()

    def test_idageodf_max_distance_mbr(self, idageodf1):
         idageodf1_mbr = idageodf1
         idageodf1_mbr['MBR'] = idageodf1_mbr.geometry.mbr()
         idageodf1_mbr.set_geometry('MBR')
         ida1 = idageodf1_mbr[idageodf1_mbr['NAME'] == 'SQ1']
         ida2 = idageodf1_mbr[idageodf1_mbr['NAME'] == "SQ2"]
         res = ida1.distance(ida2)
         assert res['RESULT'].max() is not None

    def test_idageodf_max_area_union(self, idageodf1, idageodf2):
         idageodf1_mbr = idageodf1
         idageodf1_mbr['MBR'] = idageodf1_mbr.geometry.mbr()
         idageodf1_mbr.set_geometry('MBR')
         idageodf2_mbr = idageodf2
         idageodf2_mbr['MBR'] = idageodf2_mbr.geometry.mbr()
         idageodf2_mbr.set_geometry('MBR')
         ida12_union = idageodf1_mbr.union(idageodf2_mbr)
         ida12_union.set_geometry('RESULT')
         ida12_union['MBR_UNION_AREA'] = ida12_union.area()
         assert ida12_union['MBR_UNION_AREA'].max()

