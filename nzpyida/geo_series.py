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
IdaGeoSeries
"""
from numbers import Number
from collections import OrderedDict

from lazy import lazy
import six

import nzpyida
from nzpyida.series import IdaSeries
from nzpyida.exceptions import IdaGeoDataFrameError

from nzpy.core import ProgrammingError

class IdaGeoSeries(nzpyida.IdaSeries):
    """
    An IdaSeries whose column must have geometry type.
    It has geospatial methods based on Netezza Performance Server Analytics.
    
    Note on sample data used for the examples:

        * Sample tables available out of the box in Netezza:

          GEO_TORNADO, GEO_COUNTY

        * Sample tables which you can create by executing the SQL statements in
          https://github.com/IBM/nzpyida/blob/main/nzpyida/sampledata/sql_script:

          SAMPLE_POLYGONS, SAMPLE_LINES, SAMPLE_GEOMETRIES, SAMPLE_MLINES, SAMPLE_POINTS

    Notes
    -----
    IdaGeoDataSeries objects are not supported on Netezza.

    An IdaGeoSeries doesn't have an indexer attribute because geometries are
    unorderable in Netezza Performance Server Analytics.

    Examples
    --------
    >>> idageodf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer='OBJECTID', geometry = "SHAPE")
    >>> idageoseries = idageodf["SHAPE"]
    >>> idageoseries.dtypes
                 --------------
                | TYPE_NAME   |
         ----------------------
        | SHAPE | ST_GEOMETRY |
         ----------------------

    """
    def __init__(self, idadb, tablename, indexer, column):
        """
        Ensures that the specified column has geometry type.
        See __init__ of IdaSeries.

        Parameters
        ----------
        column : str
            Column name. It must have geometry type.

        Notes
        -----
        Even though geometry types are unorderable in NPS, the IdaGeoSeries
        might have as indexer another column of the table whose column the
        IdaGeoSeries refers to.
        """

        super(IdaGeoSeries, self).__init__(idadb, tablename, indexer, column)
        is_geometry_type = True
        try:
            self.column_data_type = self.geometry_type().head().iloc[0]
        except ProgrammingError as e:
            if "Geometry unsupported" in str(e):
                is_geometry_type = False
            else:
                raise e    
        if not is_geometry_type:
            raise TypeError("Specified column doesn't have geometry type. " + 
                            "Cannot create IdaGeoSeries object")

    @classmethod
    def from_IdaSeries(cls, idaseries):
        """
        Creates an IdaGeoSeries from an IdaSeries, ensuring that the column
        of the given IdaSeries has geometry type.
        """
        is_geometry_type = True
        if not isinstance(idaseries, IdaSeries):
            raise TypeError("Expected IdaSeries")
        else:
            # Mind that the given IdaSeries might have non-destructive
            # columns that were added by the user. That's why __init__ is not
            # used for this purpose.
            idageoseries = idaseries
            idageoseries.__class__ = IdaGeoSeries
            try:
                idageoseries.column_data_type = idageoseries.geometry_type().head().iloc[0]
            except ProgrammingError as e:
                if "Geometry unsupported" in str(e) or \
                'Unable to identify a function that satisfies the given argument types' in str(e):
                    is_geometry_type = False
                else:
                    raise e    
            if not is_geometry_type:
                raise TypeError("Specified column doesn't have geometry type. " + 
                                    "Cannot create IdaGeoSeries object")
            return idageoseries

#==============================================================================
### Methods whose behavior is not defined for geometry types in NPS.
#==============================================================================

    # TODO: Override all the methods of IdaSeries (and those of its parent,
    # i.e. IdaDataFrame, which are not defined in NPS for geometry columnns,
    # like min(), max(), etc.)

    def min(self):
        raise TypeError("Unorderable geometries")
        pass

    def max(self):
        raise TypeError("Unorderable geometries")
        pass

#==============================================================================
### Unary geospatial methods
#==============================================================================

    def buffer(self, distance, unit = None):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoSeries of geometries in which each point is the
        specified distance away from the geometries in the calling
        IdaGeoSeries, measured in the given unit.

        Parameters
        ----------
        distance : float
            Distance, can be positive or negative.
        unit : str, optional
            Name of the unit, it is case-insensitive.
            If omitted, the following rules are used:

                * If geometry is in a projected or geocentric coordinate
                  system, the linear unit associated with this coordinate system
                  is the default.
                * If geometry is in a geographic coordinate system, the angular
                  unit associated with this coordinate system is the default.

        Returns
        -------
        IdaGeoSeries.

        See also
        ---------
        linear_units

        Notes
        -----
        Restrictions on unit conversions: An error (SQLSTATE 38SU4) is returned
        if any of the following conditions occur:

            * The geometry is in an unspecified coordinate system and the unit
              parameter is specified.
            * The geometry is in a projected coordinate system and an angular
              unit is specified.
            * The geometry is in a geographic coordinate system, but is not an
              ST_Point value , and a linear unit is specified.

        # TODO: handle this SQLSTATE error

        References
        ----------
        Netezza Performance Server Analytics ST_BUFFER() function.

        Examples
        --------
        >>> tornadoes = IdaGeoDataFrame(idadb,'SAMPLES.GEO_TORNADO',indexer='OBJECTID')
        >>> tornadoes.set_geometry('SHAPE')
        >>> tornadoes['buffer_20_km'] = tornadoes.buffer(distance = 20, unit = 'KILOMETER')
        >>> tornadoes[['OBJECTID','SHAPE','buffer_20_km']].head()
        OBJECTID  SHAPE                   buffer_20_km
        1         <Geometry binary data>  <Geometry binary data>
        2         <Geometry binary data>  <Geometry binary data>
        3         <Geometry binary data>  <Geometry binary data>
        4         <Geometry binary data>  <Geometry binary data>
        5         <Geometry binary data>  <Geometry binary data>
        """
        if not isinstance(distance, Number):
            # distance can be positive or negative
            raise TypeError("Distance must be numerical")
        additional_args = []
        additional_args.append(distance)
        if unit is not None:
            unit = self._check_linear_unit(unit)  # Can raise exceptions
            additional_args.append(unit)
        return self._unary_operation_handler(
            function_name = 'inza..ST_BUFFER',
            additional_args = additional_args,
            return_geo_series=True)

    def centroid(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoSeries of points which represent the geometric center
        of each of the geometries in the calling IdaGeoSeries.

        The geometric center is the center of the minimum bounding rectangle of
        the given geometry, as a point.

        The resulting point is represented in the spatial reference system of
        the given geometry.

        For None geometries the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_CENTROID() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> counties['centroid'] = counties.centroid()
        >>> counties[['NAME','centroid']].head()
        NAME         centroid
        Wood         <Geometry binary data>
        Cass         <Geometry binary data>
        Washington   <Geometry binary data>
        Fulton       <Geometry binary data>
        Clay         <Geometry binary data>
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_CENTROID',
                return_geo_series=True)

    def convex_hull(self):
        """
        The convex hull of a shape, also called convex envelope or convex closure, is the smallest convex set that contains it.
        For example, if you have a bounded subset of points in the Euclidean space, the convex hull may be visualized as 
        the shape enclosed by an elastic band stretched around the outside points of the subset. 
        If vertices of the geometry do not form a convex, convexhull returns a null.
        
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        If possible, the specific type of the returned geometry will be ST_Point, ST_LineString, or ST_Polygon.
        The convex hull of a convex polygon with no holes is a single linestring, represented as ST_LineString. 
        The convex hull of a non convex polygon does not exit. 
        
        Returns
        -------
        IdaGeoSeries

            Returns an IdaGeoSeries containing geometries which are the convex hull of each
            of the geometries in the calling IdaGeoSeries.
            The resulting geometry is represented in the spatial reference system
            of the given geometry.
            For None geometries, for empty geometries and for non convex geometries the output is None.

        References
        ----------
        Netezza Performance Server Analytics ST_CONVEXHULL() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> counties['convex_envelope'] = counties["SHAPE"].convex_hull()
        >>> counties[['OBJECTID','SHAPE','convex_envelope']].head()
            OBJECTID    SHAPE                   convex_envelope
        0   1           <Geometry binary data>  <Geometry binary data>
        1   2           <Geometry binary data>  <Geometry binary data>
        2   3           <Geometry binary data>  <Geometry binary data>
        3   4           <Geometry binary data>  <Geometry binary data>
        4   5           <Geometry binary data>  <Geometry binary data>
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_CONVEXHULL',
                return_geo_series=True)

    def boundary(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoSeries of geometries which are the boundary of each
        of the geometries in the calling IdaGeoSeries.

        The resulting geometry is represented in the spatial reference system
        of the given geometry.

        If the given geometry is a point, multipoint, closed curve, or closed
        multicurve, or if it is empty, then the result is an empty geometry of
        type ST_Point. For curves or multicurves that are not closed, the start
        points and end points of the curves are returned as an ST_MultiPoint
        value, unless such a point is the start or end point of an even number
        of curves. For surfaces and multisurfaces, the curve defining the
        boundary of the given geometry is returned, either as an ST_Curve or an
        ST_MultiCurve value.

        If possible, the specific type of the returned geometry will be
        ST_Point, ST_LineString, or ST_Polygon. For example, the boundary of a
        polygon with no holes is a single linestring, represented as
        ST_LineString. The boundary of a polygon with one or more holes
        consists of multiple linestrings, represented as ST_MultiLineString.

        For None geometries the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        ST_BOUNDARY() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> counties['boundary'] = counties.boundary()
        >>> counties[['NAME','boundary']].head()
        NAME         boundary
        Madison      <Geometry binary data>
        Lake         <Geometry binary data>
        Broward      <Geometry binary data>
        Buena Vista  <Geometry binary data>
        Jones        <Geometry binary data>
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_BOUNDARY',
                return_geo_series=True)

    def envelope(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry.

        Returns an IdaGeoSeries of polygons which are an envelope around each
        of the geometries in the calling IdaGeoSeries. The envelope is a
        rectangle that is represented as a polygon.

        If the given geometry is a point, a horizontal linestring, or a
        vertical linestring, then a rectangle, which is slightly larger than
        the given geometry, is returned. Otherwise, the minimum bounding
        rectangle of the geometry is returned as the envelope.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaGeoSeries.

        See also
        --------
        mbr

        References
        ----------
        Netezza Performance Server Analytics ST_ENVELOPE() function.

        Examples
        --------
        >>> tornadoes = IdaGeoDataFrame(idadb,'SAMPLES.GEO_TORNADO',indexer='OBJECTID')
        >>> tornadoes.set_geometry('SHAPE')
        >>> tornadoes['envelope'] = tornadoes.envelope()
        >>> tornadoes[['OBJECTID', 'SHAPE', 'envelope']].head()
        OBJECTID   SHAPE                    envelope
        1          <Geometry binary data>   <Geometry binary data>
        2          <Geometry binary data>   <Geometry binary data>
        3          <Geometry binary data>   <Geometry binary data>
        4          <Geometry binary data>   <Geometry binary data>
        5          <Geometry binary data>   <Geometry binary data>
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_ENVELOPE',
                return_geo_series=True)

    def exterior_ring(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Polygon.

        Returns an IdaGeoSeries of curves which are the exterior ring of each
        of the geometries in the calling IdaGeoSeries.

        The resulting curve is represented in the spatial reference system of
        the given polygon.

        If the polygon does not have any interior rings, the returned exterior
        ring is identical to the boundary of the polygon.

        For None polygons the output is None.
        For empty polygons the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_EXTERIORRING() function.

        Examples
        --------
        >>> sample_polygons["ext_ring"] = sample_polygons.exterior_ring()
        >>> sample_polygons.head()
            ID      GEOMETRY                ext_ring
        0   1101    <Geometry binary data>  <Geometry binary data>
        1   1102    <Geometry binary data>  <Geometry binary data>

        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_EXTERIORRING',
                valid_types = ['ST_POLYGON'],
                return_geo_series=True)

    def mbr(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoSeries of geometries which are the minimum bounding
        rectangle of each of the geometries in the calling IdaGeoSeries.

        If the given geometry is a point, then the point itself is returned.
        If the geometry is a horizontal linestring or a vertical linestring,
        the horizontal or vertical linestring itself is returned.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_MBR() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> counties["MBR"] = counties.mbr()
        >>> counties[["NAME", "SHAPE", "MBR"]].head()
            NAME        SHAPE 	                MBR
        0   Lafayette   <Geometry binary data>  <Geometry binary data>
        1 	Sanilac 	<Geometry binary data> 	<Geometry binary data>
        2 	Taylor 	    <Geometry binary data> 	<Geometry binary data>
        3 	Ohio 	    <Geometry binary data> 	<Geometry binary data>
        4 	Houston 	<Geometry binary data> 	<Geometry binary data>

        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_MBR',
                return_geo_series=True)

    def end_point(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_LINESTRING.

        Returns an IdaGeoSeries with the last point of each of the curves in
        the calling IdaGeoSeries.

        The resulting point is represented in the spatial reference system of
        the given curve.

        For None curves the output is None.
        For empty curves the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_ENDPOINT() function.

        Examples
        --------
        Sample to create in Netezza, geometry column with data type ST_LineString
        Use this sample data for testing:

        >>> sample_lines = IdaGeoDataFrame(idadb, "SAMPLE_LINES", indexer = "ID", geometry  = "GEOMETRY")
        >>> sample_lines['end_point'] = sample_lines.end_point()
        >>> sample_lines.head()
        	ID 	    GEOMETRY 	            end_point
        0 	1110 	<Geometry binary data> 	<Geometry binary data>
        1 	1111 	<Geometry binary data> 	<Geometry binary data>      
        
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_ENDPOINT',
                valid_types = ['ST_LINESTRING'],
                return_geo_series=True)

    def start_point(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_LINESTRING.

        Returns an IdaGeoSeries with the first point of each of the curves in
        the calling IdaGeoSeries.

        The resulting point is represented in the spatial reference system of
        the given curve.

        For None curves the output is None.
        For empty curves the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_STARTPOINT() function.

        Examples
        --------
        Sample to create in Netezza, geometry column with data type ST_LineString
        
        >>> sample_lines = IdaGeoDataFrame(idadb, "SAMPLE_LINES", indexer = "ID", geometry  = "LOC")
        >>> sample_lines.start_point().head()
        0    <Geometry binary data>
        1    <Geometry binary data>
               
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_STARTPOINT',
                valid_types = ['ST_LINESTRING'],
                return_geo_series=True)

    def srid(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers representing the spatial reference
        system of each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_SRID() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = 'OBJECTID', geometry = 'SHAPE')        
        >>> counties.srid().head()
        0    1005
        1    1005
        2    1005
        3    1005
        4    1005       
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_SRID')

    def geometry_type(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry.

        Returns an IdaSeries with strings representing the fully qualified type
        name of the dynamic type of each of the geometries in the calling
        IdaGeoSeries.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_GEOMETRYTYPE() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> counties.geometry_type().head(3)
        0    ST_MULTIPOLYGON
        1    ST_MULTIPOLYGON
        2    ST_MULTIPOLYGON

        
        See boundary method
        
        >>> counties["boundary"].geometry_type().head(3)
        0    ST_LINESTRING
        1    ST_LINESTRING
        2    ST_LINESTRING

        See centroid method
        
        >>> counties["centroid"].geometry_type().head(3) 
        0    ST_POINT
        1    ST_POINT
        2    ST_POINT     
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_GEOMETRYTYPE')

    def area(self, unit = None):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the area covered by
        each of the geometries in the calling IdaGeoSeries, in the given unit
        or else in the default unit.

        If the geometry is a polygon or multipolygon, then the area covered by
        the geometry is returned. The area of points, linestrings, multipoints,
        and multilinestrings is 0 (zero).

        For None geometries the output is None.
        For empty geometries the output is None.

        Parameters
        ----------
        unit : str, optional
            Name of the unit, it is case-insensitive.
            If omitted, the following rules are used:

                * If geometry is in a projected or geocentric coordinate
                  system, the linear unit associated with this coordinate system
                  is used.
                * If geometry is in a geographic coordinate system, the angular
                  unit associated with this coordinate system is used.

        Returns
        -------
        IdaSeries.

        See also
        --------
        linear_units

        Notes
        -----
        Restrictions on unit conversions: An error (SQLSTATE 38SU4) is returned
        if any of the following conditions occur:

            * The geometry is in an unspecified coordinate system and the unit
              parameter is specified.
            * The geometry is in a projected coordinate system and an angular
              unit is specified.
            * The geometry is in a geographic coordinate system, and a linear
              unit is specified.

        # TODO: handle this SQLSTATE error

        References
        ----------
        Netezza Performance Server Analytics ST_AREA() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> counties['area_in_km'] = counties.area(unit = 'KILOMETER')
        >>> counties[['NAME','area_in_km']].head()
        NAME         area_in_km
        Wood         1606.526429
        Cass         2485.836511
        Washington   1459.393496
        Fulton       1382.620091
        Clay         2725.095566
        """
        additional_args = []
        if unit is not None:
            unit = self._check_linear_unit(unit)  # Can raise exceptions
            additional_args.append(unit)
        return self._unary_operation_handler(
            function_name = 'inza..ST_AREA',
            additional_args = additional_args)

    def dimension(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry.

        Returns an IdaSeries with integers representing the dimension of each
        of the geometries in the calling IdaGeoSeries.

        If the given geometry is empty, then -1 is returned.
        For points and multipoints, the dimension is 0 (zero).
        For curves and multicurves, the dimension is 1.
        For polygons and multipolygons, the dimension is 2.

        For None geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_DIMENSION() function.

        Examples
        --------
        >>> tornadoes = IdaGeoDataFrame(idadb, "SAMPLES.GEO_TORNADO, indexer = 'OBJECTID')
        >>> tornadoes["buffer_20_km"] =  tornadoes.buffer(distance = 20, unit = 'KILOMETER')
        >>> tornadoes["buffer_20_km_dim"] = tornadoes["buffer_20_km"].dimension()
        >>> tornadoes[["buffer_20_km", "buffer_20_km_dim"]].head()
        	buffer_20_km 	        buffer_20_km_dim
        0 	<Geometry binary data>  2
        1 	<Geometry binary data> 	2
        2 	<Geometry binary data> 	2
        3 	<Geometry binary data> 	2
        4 	<Geometry binary data>	2

        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> counties['centroid_dim'] = counties['centroid'].dimension()
        >>> counties[['centroid', 'centroid_dim']].head()
            centroid                centroid_dim
        0   <Geometry binary data>  0
        1   <Geometry binary data>  0
        2   <Geometry binary data>  0
        3   <Geometry binary data>  0
        4   <Geometry binary data>  0
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_DIMENSION')

    def length(self, unit = None):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_LINESTRING, ST_MULTILINESTRING.

        Returns an IdaSeries with doubles representing the length of each of
        the curves or multicurves in the calling IdaGeoSeries, in the given
        unit or else in the default unit.

        For None curves or multicurves the output is None.
        For empty curves or multicurves the output is None.

        Parameters
        ----------
        unit : str, optional
            Name of the unit, it is case-insensitive.
            If omitted, the following rules are used:

                * If curve is in a projected or geocentric coordinate system,
                  the linear unit associated with this coordinate system is the
                  default.
                * If curve is in a geographic coordinate system, the angular
                  unit associated with this coordinate system is the default.

        Returns
        -------
        IdaSeries.

        See also
        --------
        linear_units

        Notes
        -----
        Restrictions on unit conversions: An error (SQLSTATE 38SU4) is returned
        if any of the following conditions occur:

            * The curve is in an unspecified coordinate system and the unit
              parameter is specified.
            * The curve is in a projected coordinate system and an angular unit
              is specified.
            * The curve is in a geographic coordinate system, and a linear unit
              is specified.

        # TODO: handle this SQLSTATE error

        References
        ----------
        Netezza Performance Server Analytics ST_LENGTH() function.

        Examples
        --------
        >>> tornadoes = IdaGeoDataFrame(idadb,'SAMPLES.GEO_TORNADO',indexer='OBJECTID')
        >>> tornadoes.set_geometry('SHAPE')
        >>> tornadoes['length'] = tornadoes.length(unit = 'KILOMETER')
        >>> tornadoes[['OBJECTID', 'SHAPE', 'length']].head()
        OBJECTID    SHAPE                   length
        1           <Geometry binary data>  17.798545
        2           <Geometry binary data>  6.448745
        3           <Geometry binary data>  0.014213
        4           <Geometry binary data>  0.014173
        5           <Geometry binary data>  4.254681
        """
        additional_args = []
        if unit is not None:
            unit = self._check_linear_unit(unit)  # Can raise exceptions
            additional_args.append(unit)
        return self._unary_operation_handler(
                function_name = 'inza..ST_LENGTH',
                valid_types = ['ST_LINESTRING', 'ST_MULTILINESTRING'],
                additional_args = additional_args)

    def perimeter(self, unit = None):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_POLYGON, ST_MULTIPOLYGON.

        Returns an IdaSeries with doubles representing the perimeter of each of
        the surfaces or multisurfaces in the calling IdaGeoSeries, in the given
        unit or else in the default unit.

        For None curves or multicurves the output is None.
        For empty curves or multicurves the output is None.

        Parameters
        ----------
        unit : str, optional
            Name of the unit, it is case-insensitive.
            If omitted, the following rules are used:

                * If surface is in a projected or geocentric coordinate system,
                  the linear unit associated with this coordinate system is the
                  default.
                * If surface is in a geographic coordinate system, the angular
                  unit associated with this coordinate system is the default.

        Returns
        -------
        IdaSeries.

        See also
        --------
        linear_units
        
        Notes
        -----
        Restrictions on unit conversions: An error (SQLSTATE 38SU4) is returned
        if any of the following conditions occur:

            * The geometry is in an unspecified coordinate system and the unit
              parameter is specified.
            * The geometry is in a projected coordinate system and an angular
              unit is specified.
            * The geometry is in a geographic coordinate system and a linear
              unit is specified.

        # TODO: handle this SQLSTATE error

        References
        ----------
        Netezza Performance Server Analytics ST_PERIMETER() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = 'OBJECTID')
        >>> counties["perimeter"] = counties.perimeter()
        >>> counties[["NAME", "SHAPE", "perimeter"]].head()
        	NAME 	    SHAPE                   perimeter
        0   Claiborne   <Geometry binary data>  2.033745
        1   Otsego      <Geometry binary data>  1.656962
        2   Madison     <Geometry binary data>  1.600404
        3   Cleveland   <Geometry binary data>  1.662438
        4   McIntosh    <Geometry binary data>  2.122012       
        """
        additional_args = []
        if unit is not None:
            unit = self._check_linear_unit(unit)  # Can raise exceptions
            additional_args.append(unit)
        return self._unary_operation_handler(
                function_name = 'inza..ST_PERIMETER',
                valid_types = ['ST_POLYGON', 'ST_MULTIPOLYGON'],
                additional_args = additional_args)

    def num_geometries(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_MULTIPOINT, ST_MULTIPOLYGON, ST_MULTILINESTRING.

        Returns an IdaSeries with integers representing the number of
        geometries in each of the collections in the calling IdaGeoSeries.

        For None collections the output is None.
        For empty collections the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_NUMGEOMETRIES() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = "OBJECTID", geometry = "SHAPE")
        >>> print(counties.geometry.dtypes)
                      TYPENAME
        SHAPE  ST_MULTIPOLYGON        
        >>> counties["SHAPE"].num_geometries().head()
        0    1
        1    1
        2    1
        3    1
        4    1
        
        Use sample data created in Netezza with SQL script, data type ST_MultiLineString
        
        >>> sample_mlines = IdaGeoDataFrame(idadb, "SAMPLE_MLINES", indexer = "ID", geometry = "GEOMETRY")
        >>> print(sample_mlines.geometry.dtypes)
                    TYPENAME
        GEOMETRY    ST_GEOMETRY
        
        >>> sample_mlines.num_geometries().head()
        0    3       
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_NUMGEOMETRIES',
                valid_types = ['ST_MULTIPOINT', 'ST_MULTIPOLYGON',
                               'ST_MULTILINESTRING'])

    def num_interior_ring(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_POLYGON.

        Returns an IdaSeries with integers representing the number of interior
        rings of each of the polygons in the calling IdaGeoSeries.

        For None collections the output is None.
        For empty collections the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_NUMINTERIORRING() function.

        Examples
        --------
        Use sample table SAMPLE_POLYGONS, obtained with SQL script
        
        >>> sample_polygons = IdaGeoDataFrame(idadb, "SAMPLE_POLYGONS", indexer = "ID", geometry = "GEOMETRY")
        >>> sample_polygons["int_ring"] = sample_polygons.num_interior_ring()
        >>> sample_polygons[["GEOMETRY", "int_ring"]].head()        
            GEOMETRY                int_ring
        0   <Geometry binary data>  0
        1   <Geometry binary data>  1        
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_NUMINTERIORRING',
                valid_types = ['ST_POLYGON'])

    def num_points(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers representing the number of points of
        each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_NUMPOINTS() function.

        Examples
        --------
        Use sample table SAMPLE_GEOMETRIES, obtained with SQL script
        
        >>> sample_geometries = IdaGeoDataFrame(idadb, "SAMPLE_GEOMETRIES", indexer = "ID", geometry = "GEOMETRY")
        >>> sample_geometries["num_points"] = sample_geometries.num_points()
        >>> sample_geometries[["GEOMETRY", "num_points"]].head()
            GEOMETRY                num_points
        0   <Geometry binary data> 	1.0
        1   <Geometry binary data> 	5.0
        2   <Geometry binary data> 	NaN
        3   <Geometry binary data> 	NaN
        4   <Geometry binary data> 	3.0
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_NUMPOINTS')

    def coord_dim(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers representing the dimensionality of
        the coordinates of each of the geometries in the calling IdaGeoSeries.

        If the given geometry does not have Z and M coordinates,
        the dimensionality is 2.
        If it has Z coordinates and no M coordinates, or if it has M
        coordinates and no Z coordinates, the dimensionality is 3.
        If it has Z and M coordinates, the dimensionality is 4.

        For None geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DNetezza Performance Server Analytics ST_COORDDIM() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = "OBJECTID", geometry = "SHAPE")        
        >>> counties.coord_dim().head()
        0    2
        1    2
        2    2
        3    2
        4    2
        # use sample table SAMPLE_POINTS, obtained with SQL script
        >>> sample_points = IdaGeoDataFrame(idadb, "SAMPLE_POINTS", indexer = "ID", geometry = "LOC")        
        >>> sample_points['coord_dim'] = sample_points.coord_dim()
        >>> sample_points[['ID', 'LOC','coord_dim']].head()
         	ID 	LOC 	                coord_dim
        0 	1 	<Geometry binary data> 	2
        1 	2 	<Geometry binary data>  3
        2 	3 	<Geometry binary data>  3
        3 	4 	<Geometry binary data> 	2
        4 	5 	<Geometry binary data>  3
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_COORDDIM')

    def is_3d(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers (1 if it has Z coordiantes, 0
        otherwise) for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_IS3D() function.

        Examples
        --------
        Use sample table SAMPLE_POINTS, obtained with SQL script

        >>> sample_points = IdaGeoDataFrame(idadb, "SAMPLE_POINTS", indexer = "id", geometry = "LOC")
        >>> sample_points["is_3d"] = sample_points.is_3d()
        >>> sample_points[["LOC", "is_3d"]].head()
         	LOC 	                is_3d
        0 	<Geometry binary data>  False
        1 	<Geometry binary data> 	True
        2 	<Geometry binary data>  True
        3 	<Geometry binary data>	False
        4 	<Geometry binary data> 	True        
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_IS3D')

    def is_measured(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers (1 if it has M coordiantes, 0
        otherwise) for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_ISMEASURED() function.

        Examples
        --------
        Use sample table SAMPLE_POINTS, obtained with SQL script
        
        >>> sample_points = IdaGeoDataFrame(idadb, "SAMPLE_POINTS", indexer = "id", geometry = "LOC")
        >>> sample_points["is_M"]=sample_points.is_measured()
        >>> sample_points.head()
        	ID 	LOC 	                coord_dim   is_3d   is_M
        0 	1 	<Geometry binary data> 	2 	        False   False
        1 	2 	<Geometry binary data> 	3 	        True    False
        2 	3 	<Geometry binary data> 	3	        False   True
        3 	4 	<Geometry binary data>  2 	        False   False
        4 	5 	<Geometry binary data> 	3 	        True    False

        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_ISMEASURED')

    def max_m(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the maximum M coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.
        For geometries without M coordinate the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_MAXM() function.

        Examples
        --------
        Max M, X, Y and Z

        >>> sample_geometries = IdaGeoDataFrame(idadb, "SAMPLE_GEOMETRIES", indexer = "ID", geometry = "GEOMETRY")
        >>> sample_geometries["max_X"] = sample_geometries.max_x()
        >>> sample_geometries["max_Y"] = sample_geometries.max_y()
        >>> sample_geometries["max_Z"] = sample_geometries.max_z()
        >>> sample_geometries["max_M"] = sample_geometries.max_m()
        >>> sample_geometries.head()
            ID  GEOMETRY                max_X   max_Y   max_Z   max_M
        0   1   <Geometry binary data>  1.0 	2.0 	None 	None
        1   2   <Geometry binary data> 	5.0 	4.0 	None 	None
        2   3   <Geometry binary data>	NaN 	NaN 	None 	None
        3   4   <Geometry binary data> 	NaN 	NaN 	None 	None
        4   5   <Geometry binary data> 	35.0 	6.0 	None 	None
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_MAXM')

    def max_x(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the maximum X coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_MAXX() function.

        Examples
        --------
        Max M, X, Y and Z        
        
        >>> sample_geometries = IdaGeoDataFrame(idadb, "SAMPLE_GEOMETRIES", indexer = "ID", geometry = "GEOMETRY")
        >>> sample_geometries["max_X"] = sample_geometries.max_x()
        >>> sample_geometries["max_Y"] = sample_geometries.max_y()
        >>> sample_geometries["max_Z"] = sample_geometries.max_z()
        >>> sample_geometries["max_M"] = sample_geometries.max_m()
        >>> sample_geometries.head()
            ID  GEOMETRY                max_X   max_Y   max_Z   max_M
        0   1   <Geometry binary data>  1.0 	2.0 	None 	None
        1   2   <Geometry binary data> 	5.0 	4.0 	None 	None
        2   3   <Geometry binary data>	NaN 	NaN 	None 	None
        3   4   <Geometry binary data> 	NaN 	NaN 	None 	None
        4   5   <Geometry binary data> 	35.0 	6.0 	None 	None
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_MAXX')

    def max_y(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the maximum Y coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_MAXY() function.

        Examples
        --------
        Max M, X, Y and Z        
        
        >>> sample_geometries = IdaGeoDataFrame(idadb, "SAMPLE_GEOMETRIES", indexer = "ID", geometry = "GEOMETRY")
        >>> sample_geometries["max_X"] = sample_geometries.max_x()
        >>> sample_geometries["max_Y"] = sample_geometries.max_y()
        >>> sample_geometries["max_Z"] = sample_geometries.max_z()
        >>> sample_geometries["max_M"] = sample_geometries.max_m()
        >>> sample_geometries.head()
            ID  GEOMETRY                max_X   max_Y   max_Z   max_M
        0   1   <Geometry binary data>  1.0 	2.0 	None 	None
        1   2   <Geometry binary data> 	5.0 	4.0 	None 	None
        2   3   <Geometry binary data>	NaN 	NaN 	None 	None
        3   4   <Geometry binary data> 	NaN 	NaN 	None 	None
        4   5   <Geometry binary data> 	35.0 	6.0 	None 	None
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_MAXY')

    def max_z(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the maximum Z coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.
        For geometries without Z coordinate the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_MAXZ() function.

        Examples
        --------
        Max M, X, Y and Z        
        
        >>> sample_geometries = IdaGeoDataFrame(idadb, "SAMPLE_GEOMETRIES", indexer = "ID", geometry = "GEOMETRY")
        >>> sample_geometries["max_X"] = sample_geometries.max_x()
        >>> sample_geometries["max_Y"] = sample_geometries.max_y()
        >>> sample_geometries["max_Z"] = sample_geometries.max_z()
        >>> sample_geometries["max_M"] = sample_geometries.max_m()
        >>> sample_geometries.head()
            ID  GEOMETRY                max_X   max_Y   max_Z   max_M
        0   1   <Geometry binary data>  1.0 	2.0 	None 	None
        1   2   <Geometry binary data> 	5.0 	4.0 	None 	None
        2   3   <Geometry binary data>	NaN 	NaN 	None 	None
        3   4   <Geometry binary data> 	NaN 	NaN 	None 	None
        4   5   <Geometry binary data> 	35.0 	6.0 	None 	None
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_MAXZ')

    def min_m(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the minimum M coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.
        For geometries without M coordinate the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_MINM() function.

        Examples
        --------
        Min M, X, Y and Z   
        Use sample table SAMPLE_GEOMETRIES, obtained with SQL script
        
        >>> sample_geometries = IdaGeoDataFrame(idadb, "SAMPLE_GEOMETRIES", indexer = "ID", geometry = "GEOMETRY")
        >>> sample_geometries["min_X"] = sample_geometries.min_x()
        >>> sample_geometries["min_Y"] = sample_geometries.min_y()
        >>> sample_geometries["min_Z"] = sample_geometries.min_z()
        >>> sample_geometries["min_M"] = sample_geometries.min_m()
        >>> sample_geometries.head()        
            ID  GEOMETRY                min_X   min_Y   min_Z   min_M
        0   1   <Geometry binary data> 	1.0 	2.0 	None 	None
        1   2   <Geometry binary data> 	0.0 	0.0 	None 	None
        2   3   <Geometry binary data> 	NaN 	NaN 	None 	None
        3   4   <Geometry binary data> 	NaN 	NaN 	None 	None
        4   5   <Geometry binary data> 	33.0 	2.0 	None 	None
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_MINM')

    def min_x(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the minimum X coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_MINX() function.

        Examples
        --------
        >>> counties = IdaDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = 'OBJECTID')
        >>> counties.set_geometry("SHAPE")
        >>> counties.min_x().head()
        0   -100.227146
        1    -77.749934
        2    -85.401789
        3    -83.794279
        4    -79.856688
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_MINX')

    def min_y(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the minimum Y coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_MINY() function.

        Examples
        --------
        >>> counties = IdaDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = 'OBJECTID')
        >>> counties.set_geometry("SHAPE")
        >>> counties.min_y().head()
        0    37.912775
        1    41.998697
        2    37.630910
        3    35.562878
        4    37.005883
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_MINY')

    def min_z(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the minimum Z coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.
        For geometries without Z coordinate the output is None.
       
        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_MINZ() function.

        Examples
        --------
        Min M, X, Y and Z   
        Use sample table SAMPLE_GEOMETRIES, obtained with SQL script
        
        >>> sample_geometries = IdaGeoDataFrame(idadb, "SAMPLE_GEOMETRIES", indexer = "ID", geometry = "GEOMETRY")
        >>> sample_geometries["min_X"] = sample_geometries.min_x()
        >>> sample_geometries["min_Y"] = sample_geometries.min_y()
        >>> sample_geometries["min_Z"] = sample_geometries.min_z()
        >>> sample_geometries["min_M"] = sample_geometries.min_m()
        >>> sample_geometries.head()        
            ID  GEOMETRY                min_X   min_Y   min_Z   min_M
        0   1   <Geometry binary data> 	1.0 	2.0 	None 	None
        1   2   <Geometry binary data> 	0.0 	0.0 	None 	None
        2   3   <Geometry binary data> 	NaN 	NaN 	None 	None
        3   4   <Geometry binary data> 	NaN 	NaN 	None 	None
        4   5   <Geometry binary data> 	33.0 	2.0 	None 	None
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_MINZ')

    def m(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_POINT.

        Returns an IdaSeries with doubles representing the measure (M)
        coordinate of each of the points in the calling IdaGeoSeries.

        For None points the output is None.
        For empty points the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_M() function.

        Examples
        --------
        Use sample table SAMPLE_POINTS, obtained with SQL script
        
        >>> sample_points_extractor = IdaGeoDataFrame(idadb, "SAMPLE_POINTS", indexer = "ID")
        >>> sample_points_extractor.set_geometry("LOC")
        >>> partial = IdaGeoDataFrame.from_IdaDataFrame(sample_points_extractor.loc[3])
        >>> partial.set_geometry("LOC")
        >>> partial["M"] = partial.m()
        >>> partial.head()
         	ID 	LOC 	                X 	    Y 	    M
        0 	3 	<Geometry binary data> 	12.0 	66.0 	43.0
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_M',
                valid_types = ['ST_POINT'])

    def x(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_POINT.

        Returns an IdaSeries with doubles representing the X coordinate of each
        of the points in the calling IdaGeoSeries.

        For None points the output is None.
        For empty points the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_X() function.

        Examples
        --------
        Use sample table SAMPLE_POINTS, obtained with SQL script
        
        >>> sample_points_extractor = IdaGeoDataFrame(idadb, "SAMPLE_POINTS", indexer = "ID")
        >>> sample_points_extractor.set_geometry("LOC")
        >>> sample_points_extractor["X"] = sample_points_extractor.x()
        >>> sample_points_extractor["Y"] = sample_points_extractor.y()
        >>> sample_points_extractor.head()
         	ID 	LOC 	                X 	    Y 	 
        0 	1 	<Geometry binary data> 	14.0 	58.0
        1 	2 	<Geometry binary data> 	12.0 	35.0
        2 	3 	<Geometry binary data> 	12.0 	66.0
        3 	4 	<Geometry binary data> 	14.0 	58.0
        4 	5 	<Geometry binary data> 	12.0 	35.0

        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_X',
                valid_types = ['ST_POINT'])

    def y(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_POINT.

        Returns an IdaSeries with doubles representing the Y coordinate of each
        of the points in the calling IdaGeoSeries.

        For None points the output is None.
        For empty points the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_Y() function.

        Examples
        --------
        Use sample table SAMPLE_POINTS, obtained with SQL script
        
        >>> sample_points_extractor = IdaGeoDataFrame(idadb, "SAMPLE_POINTS", indexer = "ID")
        >>> sample_points_extractor.set_geometry("LOC")
        >>> sample_points_extractor["X"] = sample_points_extractor.x()
        >>> sample_points_extractor["Y"] = sample_points_extractor.y()
        >>> sample_points_extractor.head()
         	ID 	LOC 	                X 	    Y 	 
        0 	1 	<Geometry binary data> 	14.0 	58.0
        1 	2 	<Geometry binary data> 	12.0 	35.0
        2 	3 	<Geometry binary data> 	12.0 	66.0
        3 	4 	<Geometry binary data> 	14.0 	58.0
        4 	5 	<Geometry binary data> 	12.0 	35.0
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_Y',
                valid_types = ['ST_POINT'])

    def z(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_POINT.

        Returns an IdaSeries with doubles representing the Z coordinate of each
        of the points in the calling IdaGeoSeries.

        For None points the output is None.
        For empty points the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_Z() function.

        Examples
        --------
        Use sample table SAMPLE_POINTS, obtained with SQL script
        
        >>> sample_points_extractor = IdaGeoDataFrame(idadb, "SAMPLE_POINTS", indexer = "ID")
        >>> sample_points_extractor.set_geometry("LOC")
        >>> partial = IdaGeoDataFrame.from_IdaDataFrame(sample_points_extractor.loc[5:6])
        >>> partial.set_geometry("LOC")
        >>> partial["Z"] = partial.z()
        >>> partial.head()
            ID  LOC                     X       Y       Z
        0   5   <Geometry binary data>  12.0    35.0    12.0
        1   6   <Geometry binary data>  17.0    65.0    32.0

        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_Z',
                valid_types = ['ST_POINT'])

    def is_closed(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_LINESTRING, ST_MULTILINESTRING.

        Returns an IdaSeries with integers (1 if it is closed, 0 otherwise) for
        each of the curves or multicurves in the calling IdaGeoSeries.

        A curve is closed if the start point and end point are equal.
        If the curve has Z coordinates, the Z coordinates of the start and end
        point must be equal. Otherwise, the points are not considered equal,
        and the curve is not closed.
        A multicurve is closed if each of its curves are closed.

        For None curves or multicurves the output is None.
        For empty curves or multicurves the output is 0.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_ISCLOSED() function.

        Examples
        --------
        Use sample table SAMPLE_LINES, obtained with SQL script
        
        >>> sample_lines = IdaGeoDataFrame(idadb, "SAMPLE_LINES", indexer = "ID", geometry = "GEOMETRY")
        >>> sample_lines.is_closed().head()
        0    False
        1    False
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_ISCLOSED',
                valid_types = ['ST_LINESTRING', 'ST_MULTILINESTRING'])

    def is_empty(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers (1 if it is empty, 0 otherwise) for
        each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_ISEMPTY() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>>counties["boundary"] = counties.boundary()
        >>> counties["boundary"].is_empty().head(3)
        0    0
        1    0
        2    0     
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_ISEMPTY')

    def is_simple(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers (1 if it is simple, 0 otherwise) for
        each of the geometries in the calling IdaGeoSeries.

        Points, surfaces, and multisurfaces are always simple.
        A curve is simple if it does not pass through the same point twice.
        Amultipoint is simple if it does not contain two equal points.
        A multicurve is simple if all of its curves are simple and the only
        intersections occur at points that are on the boundary of the curves in
        the multicurve.

        For None geometries the output is None.
        For empty geometries the output is 1.

        Returns
        -------
        IdaSeries.

        References
        ----------
        Netezza Performance Server Analytics ST_ISSIMPLE() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>>counties["boundary"] = counties.boundary()
        >>> counties["is_simple"] = counties.is_simple()
        >>> filtered_counties = counties[counties['is_simple'] == 0]
        >>> filtered_counties.shape
        (0, 25)
        
        >>> counties["is_simple"] = counties['boundary'].is_simple()
        >>> filtered_counties = counties[counties['is_simple'] == 0]
        >>> filtered_counties.shape
        (37, 25)
        """
        return self._unary_operation_handler(
                function_name = 'inza..ST_ISSIMPLE')


#==============================================================================
### Private utilities for geospatial methods
#==============================================================================

    def _check_linear_unit(self, unit):
        """
        Parameters:
        -----------
        unit : str
            Name of a user-entered unit (case-insensitive).

        Returns
        -------
        str
            The name of the unit in uppercase and formatted for netezza syntax.

        Raises
        ------
        TypeError
            * If the unit is not a string
            * If the unit is a string larger than 128 characters
        """
        available_units = ['meter', 'kilometer', 'foot', 'mile', 'nautical mile']
        if not isinstance(unit, six.string_types):
            raise TypeError("unit must be a string")
        elif len(unit) > 128:
            raise TypeError("unit length exceeded")
        else:
            unit = unit.lower()
            if unit not in available_units:
                raise IdaGeoDataFrameError(
                    f"Invalid unit,  must be one of: {available_units}")
            if "\'" in unit:
                unit = unit.replace("'", "''")

            # Enclose in single quotation marks
            unit = '\''+unit+'\''
            return unit

    def _unary_operation_handler(self, function_name,
                                 valid_types = None,
                                 additional_args = None,
                                 return_geo_series=False):
        """
        Returns the resulting column of an unary geospatial method as an
        IdaGeoSeries if it has geometry type, as an IdaSeries otherwise.

        Parameters
        ----------
        function_name : str
            Name of the corresponding function.
        valid_types : list of str
            Valid input typenames.
        additional_args : list of str, optional
            Additional arguments for the function.
        return_geo_series : bool, optional
            Flag whether expected output series contains spatial data

        Returns
        -------
        IdaGeoSeries
            If the return_geo_series argument is True.
        IdaSeries
            If the return_geo_series argument is False.
        """
        if valid_types and not (self.column_data_type in valid_types):
            raise TypeError("Column " + self.column +
                            " has incompatible type.")

        # Obtain an IdaSeries object by cloning current one
        # Then modify its attribute column
        idaseries = self._clone()

        # Get the first argument for the function, i.e. a column.
        # Because it might be a non-destructive column that was added by the
        # user, the column definition is considered, instead of its alias
        # in the Ida object.
        column_name = self.internal_state.columndict[self.column]

        arguments_for_function = [column_name]

        if additional_args is not None:
            for arg in additional_args:
                arguments_for_function.append(arg)

        result_column = (
            function_name +
            '(' +
            ','.join(map(str, arguments_for_function)) +
            ')'
            )

        new_columndict = OrderedDict()
        # result_column_key must not include double quotes because it is used as as Python key and as
        # an SQL alias for the result column expression like in
        # SELECT inza..ST_AREA("SHAPE",'KILOMETER') AS "inza..ST_AREA(SHAPE,'KILOMETER')" FROM SAMPLES.GEO_COUNTY
        result_column_key = result_column.replace('"', '')
        new_columndict[result_column_key] = result_column

        idaseries._reset_attributes(["get_columns", "shape", "dtypes"])
        idaseries.internal_state.columns = ['\"' + result_column_key + '\"']

        idaseries.internal_state.columndict = new_columndict
        idaseries.internal_state.update()

        # Set the column attribute of the new idaseries
        idaseries._column = result_column_key
        
        try:
            del(idaseries.columns)
        except:
            pass
        
        if return_geo_series:
            return IdaGeoSeries.from_IdaSeries(idaseries)
        else:
            return idaseries
