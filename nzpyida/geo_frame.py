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
IdaGeoDataFrame
"""
import nzpyida
from nzpyida.frame import IdaDataFrame
from nzpyida.geo_series import IdaGeoSeries
from nzpyida.exceptions import IdaGeoDataFrameError
from nzpy.core import ProgrammingError

from copy import deepcopy

import six


class IdaGeoDataFrame(IdaDataFrame):
    """  
    An IdaGeoDataFrame container inherits from IdaDataFrame.

    It has a property called "geometry" which refers to a column with
    geometry type. It is set as a string with a column name, either at
    instantiation time or with the set_geometry() method.

    If the "geometry" property is set, when calling a geospatial method from
    IdaDataFrame the method will be carried on the column this property refers
    to.

    The property "geometry" returns an IdaGeoSeries.

    See IdaDataFrame.
    See IdaGeoSeries.

    Notes
    -----
    IdaGeoDataFrame objects are not supported on Netezza.

    Examples
    --------
    >>> idageodf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY',
    indexer='OBJECTID')

    >>> idageodf[['NAME', 'SHAPE']].head()
           NAME                   SHAPE
    0    Becker  <Geometry binary data>
    1  Jim Hogg  <Geometry binary data>
    2     Henry  <Geometry binary data>
    3     Keith  <Geometry binary data>
    4   Clinton  <Geometry binary data>

    >>> idageodf.geometry
    AttributeError: Geometry property has not been set yet. Use set_geometry
    method to set it.

    >>> idageodf.set_geometry('SHAPE')
    >>> idageodf.geometry.column
    'SHAPE'

    >>> type(idageodf.geometry)
    <class 'ibmdbpy.geoSeries.IdaGeoSeries'>

    >>> idageoseries = idageodf.geometry    
    >>> idageoseries.head()
    0    <Geometry binary data>
    1    <Geometry binary data>
    2    <Geometry binary data>
    3    <Geometry binary data>
    4    <Geometry binary data>
    Name: SHAPE, dtype: object

    >>> idageodf['County area'] = idageodf.area(unit='mile')

    >>> counties_with_areas = idageodf[['NAME', 'SHAPE', 'County area']]
    In case
    >>> counties_with_areas.dtypes
    (In case you are working with nzspatial_esri)
                        TYPENAME
    NAME                 VARCHAR
    SHAPE            ST_GEOMETRY
    County area           DOUBLE

    
    >>> counties_with_areas.dtypes
    (In case you are working with nzspatial)
                        TYPENAME
    NAME                 VARCHAR
    SHAPE                VARCHAR
    County area           DOUBLE

    >>> counties_with_areas.head()
            NAME                    SHAPE  County area
    0     Menard   <Geometry binary data>   902.281540
    1      Boone   <Geometry binary data>   282.045087
    2  Ochiltree   <Geometry binary data>   918.188142
    3    Sharkey   <Geometry binary data>   435.548518
    4    Audubon   <Geometry binary data>   444.827726
    """

    def __init__(self, idadb, tablename, indexer = None, geometry = None):
        """
        Constructor for IdaGeoDataFrame objects.
        See IdaDataFrame.__init__ documentation.

        Parameters
        ----------
        geometry : str, optional
            Column name to set the "geometry" property of the IdaGeoDataFrame.
            The column must have geometry type.

        Attributes
        ----------
        _geometry_colname : str
            Name of the column that "geometry" property refers to.
            This attribute must be set through the set_geometry() method.
        geometry : IdaGeoSeries
            The column referenced by _geometry_colname attribute.
        """      

        if geometry is not None and not isinstance(geometry, six.string_types):
            raise TypeError("geometry must be a string")
        super(IdaGeoDataFrame, self).__init__(idadb, tablename, indexer)
        self._geometry_colname = None
        if geometry is not None:
            self.set_geometry(geometry)

    def __getitem__(self, item):
        """
        Returns an IdaDataFrame, IdaSeries, IdaGeoDataFrame or IdaGeoSeries
        as appropriate.
        
        Returns
        --------
        IdaGeoSeries
            When the projection has only one column and it has geometry type.
        IdaGeoDataFrame
            When the projection has more than one column, and the "geometry"
            column of the IdaGeoDataFrame is included in them.
        IdaDataFrame
            When the projection has more than one column, and the "geometry"
            column of the IdaGeoDataFrame is not included in them.
        IdaSeries
            When the projection has only one column and it doesn't have 
            geometry type.
        """
        ida = super(IdaGeoDataFrame, self).__getitem__(item)
        if isinstance(ida, nzpyida.IdaSeries):
            if item == self._geometry_colname:
                idageoseries = IdaGeoSeries.from_IdaSeries(ida)
                # Return IdaGeoSeries
                return idageoseries
            else:
                # Return IdaSeries
                return ida
        elif isinstance(ida, nzpyida.IdaDataFrame):
            if self._geometry_colname in ida.dtypes.index:
                # Return IdaGeoDataFrame
                idageodf = IdaGeoDataFrame.from_IdaDataFrame(ida)
                idageodf._geometry_colname = self._geometry_colname
                return idageodf
            else:
                # Return IdaDataFrame
                return ida

    def __delitem__(self, item):
        """
        Erases the "geometry" property if the column it refers to is deleted.
        """
        super(IdaGeoDataFrame, self).__delitem__(item)
        if item == self._geometry_colname:
            self._geometry_colname = None

    def __getattr__(self, name):
        """
        Carry geospatial method calls on the "geometry" column of the
        IdaGeoDataFrame, if it was set.
        
        Notes
        -----
        This method gets called only when an attribute lookup on
        IdaGeoDataFrame is not resolved, i.e. it is not an instance attribute
        and it's not found in the class tree.
        """        
        
        if name == 'geometry':
            # When .geometry is accessed and _geometry_colname is None
            return self.__getattribute__('geometry')

        if hasattr(IdaGeoSeries, name):
            # Geospatial method call
            if self._geometry_colname is None:
                raise AttributeError("Geometry column has not been set yet.")
            else:
                # Get a IdaGeoSeries and carry the operation on it
                idageoseries = self.__getitem__(item = self._geometry_colname)
                return idageoseries.__getattribute__(name)
        else:
            raise AttributeError
    
    @property
    def geometry(self):
        """
        Returns an IdaGeoSeries with the column whose name is stored in 
        _geometry_colname attribute.

        The setter calls the set_geometry() method.

        Returns
        -------
        IdaGeoSeries

        Raises
        ------
        AttributeError
            If the property has not been set yet.
        
        """
        if self._geometry_colname is None:
            raise AttributeError(
                "Geometry property has not been set yet. "
                "Use set_geometry method to set it.")
        else:
            return self.__getitem__(self._geometry_colname)
    
    @geometry.setter
    def geometry(self, value):
        """
        See set_geometry() method.
        """
        self.set_geometry(value)

    @classmethod
    def from_IdaDataFrame(cls, idadf, geometry = None):
        """ 
        Creates an IdaGeoDataFrame from an IdaDataFrame.
        
        Parameters
        ----------
        geometry : str, optional
            Column name to set the "geometry" property of the IdaGeoDataFrame.
            The column must have geometry type.

        Raises
        ------
        TypeError
            If idadf is not an IdaDataFrame.
        """
        
        if not isinstance(idadf, IdaDataFrame):
            raise TypeError("Expected IdaDataFrame")
        else:
            # TODO: check if it's better to only change the .__base__ attribute

            #behavior based on _clone() method of IdaDataFrame
            newida = IdaGeoDataFrame(
                    idadf._idadb, idadf._name, idadf.indexer, geometry)
            
            newida.internal_state.name = deepcopy(idadf.internal_state.name)
            newida.internal_state.ascending = deepcopy(idadf.internal_state.ascending)
            #newida.internal_state.views = deepcopy(idadf.internal_state.views)
            newida.internal_state._views = deepcopy(idadf.internal_state._views)
            newida.internal_state._cumulative = deepcopy(idadf.internal_state._cumulative)
            newida.internal_state.order = deepcopy(idadf.internal_state.order)
            newida.internal_state.columndict = deepcopy(idadf.internal_state.columndict)
            newida.columns = idadf.columns 
            newida.dtypes = idadf.dtypes
            return newida

    def set_geometry(self, column_name):
        """
        Receives a column name to set as the "geometry" column of the
        IdaDataFrame.

        Parameters
        -----------
        column_name : str
            Name of the column to be set as geometry column of the 
            IdaDataFrame. It must have geometry type.

        Raises
        ------
        KeyError
            If the column is not present in the IdaGeoDataFrame.
        TypeError
            If the column doesn't have geometry type.
        """
        if not isinstance(column_name, six.string_types):
            raise TypeError("column_name must be a string")
        if column_name not in self.columns:
            raise KeyError( "'" + column_name + "' cannot be set as geometry column: "
                "not a column in the IdaGeoDataFrame.")
        
        is_geometry_type = True
        try: 
            idaseries = IdaGeoSeries.from_IdaSeries(self[column_name])
            self.geo_column_data_type = idaseries.column_data_type
        except TypeError:
            raise TypeError("'" + column_name + "' cannot be set as geometry column: "
                "specified column doesn't have geometry type")

        del idaseries
        
        self._geometry_colname = column_name

    # ==============================================================================
    ### Binary geospatial methods
    # ==============================================================================
    def equals(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the geometry of the first IdaGeoDataFrame
        crosses the second.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.


        References
        ----------
        Netezza Performance Server Analytics ST_CROSSES() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.equals(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          0
        2            1840         0
        2            109          0
        """
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_EQUALS')

    def distance(self, ida2, unit=None):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with a numeric value
        which is the geographic distance measured between the
        geometries of the input IdaGeoDataFrames.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.
        unit : str, optional
            Name of the unit, it is case-insensitive.
            If omitted, the following rules are used:

                * If geometry is in a projected or geocentric coordinate
                  system, the linear unit associated with this coordinate system
                  is used.
                * If geometry is in a geographic coordinate system, the angular
                  unit associated with this coordinate system is used.

        References
        ----------
        Netezza Performance Server Analytics ST_DISTANCE() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.distance(ida2,unit = 'KILOMETER')
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          26.918942
        2            1840         4.868971
        2            109          16.387094
        """
        add_args = None
        if unit is not None:
            unit = self._check_linear_unit(unit)  # Can raise exceptions
            add_args = []
            add_args.append(unit)
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_DISTANCE',
            additional_args = add_args)

    def crosses(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the geometry of the first IdaGeoDataFrame
        crosses the second.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        Netezza Performance Server Analytics ST_CROSSES() function.

        See also
        --------
        linear_units : list of valid units.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.crosses(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          0
        2            1840         0
        2            109          0
        """
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_CROSSES')

    def intersects(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the geometries of the input IdaGeoDataFrames
        intersect each other.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        Netezza Performance Server Analytics ST_INTERSECTS() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.intersects(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          0
        2            1840         0
        2            109          0
        """
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_INTERSECTS')

    def overlaps(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the geometries of the input IdaGeoDataFrames
        overlap each other.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation


        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        Netezza Performance Server Analytics ST_OVERLAPS() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.overlaps(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          0
        2            1840         0
        2            109          0
        """
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_OVERLAPS')

    def touches(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the boundary of the first geometry touches
        the second.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        Netezza Performance Server Analytics ST_TOUCHES() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.touches(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          0
        2            1840         0
        2            109          0
        """
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_TOUCHES')

    def disjoint(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the geometries in the input dataframes are
        disjoint.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        Netezza Performance Server Analytics ST_DISJOINT() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.disjoint(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          1
        2            1840         1
        2            109          1
        """
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_DISJOINT')

    def contains(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the second geometry contains the first.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        Netezza Performance Server Analytics ST_CONTAINS() function.

        Examples
        --------
        >>> idageodf_customer = IdaGeoDataFrame(idadb,'SAMPLES.GEO_CUSTOMER',indexer='OBJECTID')
        >>> idageodf_customer.set_geometry('SHAPE')
        >>> idageodf_county = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> idageodf_county.set_geometry('SHAPE')
        >>> ida1 = idageodf_customer[idageodf_customer['INSURANCE_VALUE']>250000]
        >>> ida2 = idageodf_county[idageodf_county['NAME']=='Madison']
        >>> result = ida2.contains(ida1)
        >>> result[result['RESULT']==1].head()
        INDEXERIDA1    INDEXERIDA2    RESULT
        21473          134            1
        21413          134            1
        21414          134            1
        21417          134            1
        21419          134            1
        """
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_CONTAINS')

    def within(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the first geometry is inside the second.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        Netezza Performance Server Analytics ST_WITHIN() function.

        Examples
        --------
        >>> idageodf_customer = IdaGeoDataFrame(idadb,'SAMPLES.GEO_CUSTOMER',indexer='OBJECTID')
        >>> idageodf_customer.set_geometry('SHAPE')
        >>> idageodf_county = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> idageodf_county.set_geometry('SHAPE')
        >>> ida1 = idageodf_customer[idageodf_customer['INSURANCE_VALUE']>250000]
        >>> ida2 = idageodf_county[idageodf_county['NAME']=='Madison']
        >>> result = ida1.within(ida2)
        >>> result[result['RESULT']==1].head()
        INDEXERIDA1    INDEXERIDA2    RESULT
        134            21473          1
        134            21413          1
        134            21414          1
        134            21417          1
        134            21419          1
        """
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_WITHIN')

    def mbr_intersects(self, ida2):
        """
        This method takes a second IdaGeoDataFrame an an input
        and checks if the Minimum Bounding rectangles of the
        geometries from both IdaGeoDataFrames intersect and
        stores the result as 0 or 1 in the RESULT column of
        the resulting IdaGeoDataFrame.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        Netezza Performance Server Analytics ST_MBRIntersects() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.difference(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          0
        2            1840         0
        2            109          0
        """
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_MBRINTERSECTS')

    def difference(self, ida2):
        """
        This method takes a second IdaGeoDataFrame an an input
        and returns the difference of the geometries from both
        IdaGeoDataFrames as a new geometry stored in the RESULT
        column of the resulting IdaGeoDataFrame.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        Netezza Performance Server Analytics ST_Difference() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.difference(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          <Geometry binary data>
        2            1840         <Geometry binary data>
        2            109          <Geometry binary data>
        """
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_DIFFERENCE')

    def intersection(self, ida2):
        """
        This method takes a second IdaGeoDataFrame an an input
        and returns the intersection of the geometries from both
        IdaGeoDataFrames as a new geometry stored in the RESULT
        column of the resulting IdaGeoDataFrame.

        For None geometries the output is None.
        For empty geometries the output is POINT EMPTY.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        Netezza Performance Server Analytics ST_Intersection() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.intersection(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          POINT EMPTY
        2            1840         POINT EMPTY
        2            109          POINT EMPTY
        """
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_INTERSECTION')

    def union(self, ida2):
        """
        This method takes a second IdaGeoDataFrame an an input
        and returns the union of the geometries from both
        IdaGeoDataFrames as a new geometry stored in the RESULT
        column of the resulting IdaGeoDataFrame.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        Netezza Performance Server Analytics ST_Union() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.union(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          <Geometry binary data>
        2            1840         <Geometry binary data>
        2            109          <Geometry binary data>
        """
        return self._binary_operation_handler(
            ida2,
            function_name='inza..ST_UNION')

    def _binary_operation_handler(self, ida2, function_name,
                                          valid_types_ida1=None, valid_types_ida2=None,
                                          additional_args=None):


        """
        Returns an IdaGeoDataFrame with three columns:
        [
        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation
        ]


        Parameters
        ----------
        function_name : str
                Name of the corresponding function.
        valid_types_ida1 : list of str
                Valid input typenames for the first IdaGeoSeries.
        valid_types_ida2 : list of str
                Valid input typenames for the second IdaGeoSeries.
        additional_args : list of str, optional
                Additional arguments for the function.

        Returns
        -------
        IdaGeoDataFrame
        """
        ida1 = self
        
        # Check if allowed data type
        if valid_types_ida1 and not ida1.geo_column_data_type in valid_types_ida1:
            raise TypeError("Column " + ida1.geometry.column +
                            " has incompatible type: ")
        if valid_types_ida2 and not ida2.geo_column_data_type in valid_types_ida2:
            raise TypeError("Column " + ida2.geometry.column +
                            " has incompatible type.")

        # Get the definitions of the columns, which will be the arguments for
        # the function
        column1 = 'IDA1.\"%s\"' %(ida1.geometry.column)
        column2 = 'IDA2.\"%s\"' %(ida2.geometry.column)
        arguments_for_function = [column1, column2]
        if additional_args is not None:
            for arg in additional_args:
                arguments_for_function.append(arg)

        # SELECT statement
        select_columns=[]
        if hasattr(ida1, '_indexer') and ida1._indexer is not None:
            select_columns.append('IDA1.\"%s\" AS \"INDEXERIDA1\"' %(ida1.indexer))
        else:
            message = (ida1.tablename + "has no indexer defined. Please assign index column with set_indexer and retry.")
            raise IdaGeoDataFrameError(message)
        if hasattr(ida2, '_indexer') and ida2._indexer is not None:
            select_columns.append('IDA2.\"%s\" AS \"INDEXERIDA2\"' %(ida2.indexer))
        else:
            message = (ida2.tablename + "has no indexer defined. Please assign index column with set_indexer and retry.")
            raise IdaGeoDataFrameError(message)
        result_column = (
            function_name+
            '('+
            ','.join(map(str, arguments_for_function))+
            ')'
        )
        select_columns.append('%s AS \"RESULT\"' %(result_column))
        select_statement = 'SELECT '+','.join(select_columns)+' '        
        
        # FROM clause
        from_clause=(
            'FROM '+
            '(SELECT * FROM ' + ida1.name + ') AS IDA1, '+
            '(SELECT * FROM ' + ida2.name + ') AS IDA2 '
        )

        # Create a view
        view_creation_query='('+select_statement+from_clause+')'
        viewname=self._idadb._create_view_from_expression(view_creation_query)

        idageodf=nzpyida.IdaGeoDataFrame(self._idadb, viewname, indexer='INDEXERIDA1')
        return idageodf