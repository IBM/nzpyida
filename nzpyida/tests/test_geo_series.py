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
Test module for IdaGeoSeries
"""
import pandas
import pytest
import six
from nzpy.core import ProgrammingError

from nzpyida import IdaSeries
from nzpyida import IdaGeoSeries
from nzpyida.exceptions import IdaGeoDataFrameError

GEO_SERIES_NAME = "GEO_TEST_SERIES"
GEO_COLUMN_NAME = "THE_GEOM"
INDEXER_COLUMN = "OBJECTID"

@pytest.fixture(scope='module')
def idageoseries(idadb, is_esri):
    COLUMN_TYPE = "ST_GEOMETRY" if is_esri else "VARCHAR"
    prep_series_commands = f"""
DROP TABLE {GEO_SERIES_NAME} IF EXISTS;
CREATE TABLE {GEO_SERIES_NAME} ("{INDEXER_COLUMN}"  INTEGER, "{GEO_COLUMN_NAME}" {COLUMN_TYPE}(200));
INSERT INTO {GEO_SERIES_NAME} VALUES 
(1, inza..ST_WKTToSQL('POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))'));
INSERT INTO {GEO_SERIES_NAME} VALUES 
(2, inza..ST_WKTToSQL('POLYGON ((1 1, 11 1, 11 11, 1 11, 1 1))'));
INSERT INTO {GEO_SERIES_NAME} VALUES 
(3, inza..ST_WKTToSQL('POLYGON ((-1 -1, -2 -1, -2 -2, -1 -2, -1 -1))'));
"""
    idadb.ida_query(prep_series_commands)
    yield IdaGeoSeries(idadb, GEO_SERIES_NAME, 
                          indexer=INDEXER_COLUMN, column=GEO_COLUMN_NAME)
    idadb.ida_query(f"DROP TABLE {GEO_SERIES_NAME} IF EXISTS")

class Test_IdaGeoSeries(object):
    def test_idageoseries_buffer(self, idageoseries):
        with pytest.raises(TypeError):
            idageoseries.buffer(distance='not a number')
        # TODO: ERROR:  SPU job process terminated (Segmentation fault)
        # assert(isinstance(idageoseries.buffer(distance=2.3), IdaGeoSeries))
        # assert len(ida.head())

    def test_idageoseries_centroid(self, idageoseries):
        ida = idageoseries.centroid()
        assert(isinstance(ida, IdaGeoSeries))
        assert len(ida.head())

    def test_idageoseries_convex_hull(self, idageoseries):
        ida = idageoseries.convex_hull()
        assert(isinstance(ida, IdaGeoSeries))
        assert len(ida.head())

    def test_idageoseries_boundary(self, idageoseries):
        ida = idageoseries.boundary()
        assert(isinstance(ida, IdaGeoSeries))
        assert len(ida.head())

    def test_idageoseries_envelope(self, idageoseries):
        ida = idageoseries.envelope()
        assert(isinstance(ida, IdaGeoSeries))
        assert len(ida.head())

    def test_idageoseries_exterior_ring(self, idageoseries):
        ida = idageoseries.exterior_ring()
        assert(isinstance(ida, IdaGeoSeries))
        assert len(ida.head())

    def test_idageoseries_mbr(self, idageoseries):
        ida = idageoseries.mbr()
        assert(isinstance(ida, IdaGeoSeries))
        assert len(ida.head())

    @pytest.mark.skip
    def test_idageoseries_end_point(self, idageoseries):
        # TODO: add dataset with a column with ST_LINESTRING
        pass
    
    @pytest.mark.skip
    def test_idageoseries_start_point(self, idageoseries):
        # TODO: add dataset with a column with ST_LINESTRING
        pass

    def test_idageoseries_srid(self, idageoseries):
        ida = idageoseries.srid()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_geometry_type(self, idageoseries):
        ida = idageoseries.geometry_type()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_area(self, idageoseries):
        ida = idageoseries.area(unit='foot')
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_dimension(self, idageoseries):
        ida = idageoseries.dimension()
        assert(isinstance(ida, IdaSeries))  
        assert len(ida.head())

    def test_idageoseries_length(self, idageoseries):
        # TODO 
        pass

    def test_idageoseries_perimeter(self, idageoseries):
        ida = idageoseries.perimeter(unit="kilometer")
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    @pytest.mark.skip
    def test_idageoseries_num_geometries(self, idageoseries):
        # TODO
        pass

    def test_idageoseries_num_interior_ring(self, idageoseries):
        ida = idageoseries.num_interior_ring()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_num_points(self, idageoseries):
        ida = idageoseries.num_points()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_coord_dim(self, idageoseries):
        ida = idageoseries.coord_dim()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_is_3d(self, idageoseries):
        ida = idageoseries.is_3d()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_is_measured(self, idageoseries):
        ida = idageoseries.is_measured()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())
        
    def test_idageoseries_max_m(self, idageoseries):
        ida = idageoseries.max_m()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_max_x(self, idageoseries):
        ida = idageoseries.max_x()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_max_y(self, idageoseries):
        ida = idageoseries.max_y()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_max_z(self, idageoseries):
        ida = idageoseries.max_z()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())
        
    def test_idageoseries_min_m(self, idageoseries):
        ida = idageoseries.min_m()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_min_x(self, idageoseries):
        ida = idageoseries.min_x()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_min_y(self, idageoseries):
        ida = idageoseries.min_y()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_min_z(self, idageoseries):
        ida = idageoseries.min_z()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_m(self, idageoseries):
        ida = idageoseries.centroid().m()
        assert(isinstance(ida, IdaSeries))
        with pytest.raises(ProgrammingError) as e:
            ida.head()
            assert "Geometry does not have M value" in str(e)

    def test_idageoseries_x(self, idageoseries):
        ida = idageoseries.centroid().x()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_y(self, idageoseries):
        ida = idageoseries.centroid().y()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_z(self, idageoseries):
        ida = idageoseries.centroid().z()
        assert(isinstance(ida, IdaSeries))
        with pytest.raises(ProgrammingError) as e:
            ida.head()
            assert "Geometry does not have Z value" in str(e)

    @pytest.mark.skip
    def test_idageoseries_is_closed(self, idageoseries):
        # TODO: add dataset with a column with ST_LINESTRING
        pass

    def test_idageoseries_is_empty(self, idageoseries):
        ida = idageoseries.is_empty()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())

    def test_idageoseries_is_simple(self, idageoseries):
        ida = idageoseries.is_simple()
        assert(isinstance(ida, IdaSeries))
        assert len(ida.head())
    
    def test_idageoseries_check_linear_unit(self, idageoseries):
        with pytest.raises(TypeError):
            idageoseries._check_linear_unit(10)
        unit = 'meter'
        ans = idageoseries._check_linear_unit(unit)
        assert(ans == '\'meter\'')
        unit = 'FOOT' # parenthesis
        ans = idageoseries._check_linear_unit(unit)
        assert(ans == '\'foot\'')
        with pytest.raises(IdaGeoDataFrameError):
            unit = "Uknown unit"
            idageoseries._check_linear_unit(unit)

    def test_idageoseries_unary_operation_handler_non_geometry_column(
            self, idageoseries):
        with pytest.raises(TypeError):
            idageoseries._unary_operation_handler(
                db2gse_function = 'inza..ST_AGEOSPATIALFUNCTION',
                valid_types = ['ST_POINT'])
