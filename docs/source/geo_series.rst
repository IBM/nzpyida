.. highlight:: python

IdaGeoSeries
************
The spatial methods of an IdaGeoDataFrame can be used to operate on the geometry attribute and wil return a IdaGeoSeries
object in each case.
An IdaGeoSeries is essentially a reference to a spatial column where each entry in the column is a
set of shapes corresponding to one observation represented by ST_GEOMETRY. An entry may consist of only one
shape (like a ST_POINT/ST_LINESTRING/ST_POLYGON) or multiple shapes that are meant to be thought of as one observation
(like the many polygons that make up the County of Santa Cruz in California or a state like Connecticut).

Netezza has three basic classes of geometric objects, which are Netezza spatial objects that follow OGC guidelines:

    + ST_Point / ST_MultiPoint
    + ST_Linestring / ST_MultiLineString
    + ST_Polygon / ST_MultiPolygon

Open an IdaGeoSeries
====================
.. currentmodule:: nzpyida.geo_series

.. autoclass:: IdaGeoSeries

   .. automethod:: __init__

Geospatial Methods which return an IdaGeoSeries
===============================================
Once the geometry property of the IdaGeoDataFrame is set, the geospatial methods of IdaGeoSeries can be accessed
with the IdaGeoDataFrame object. Currently the following methods are supported. 

Note on valid unit names
-------------------------------------
Here is the comprehensive list of the allowed unit names which can be given to the `unit`` option of the methods listed below:
'meter', 'kilometer', 'foot', 'mile', 'nautical mile'. 

.. automethod:: IdaGeoSeries.linear_units

Area
----
.. automethod:: IdaGeoSeries.area

Boundary
--------
.. automethod:: IdaGeoSeries.boundary

Buffer
------
.. automethod:: IdaGeoSeries.buffer

Centroid
--------
.. automethod:: IdaGeoSeries.centroid

Convex Hull
-----------
.. automethod:: IdaGeoSeries.convex_hull

coordDim
--------
.. automethod:: IdaGeoSeries.coord_dim

Dimension
---------
.. automethod:: IdaGeoSeries.dimension

Envelope
--------
.. automethod:: IdaGeoSeries.envelope

End Point
---------
.. automethod:: IdaGeoSeries.end_point

Exterior Ring
-------------
.. automethod:: IdaGeoSeries.exterior_ring

Geometry Type
-------------
.. automethod:: IdaGeoSeries.geometry_type

is 3d
-----
.. automethod:: IdaGeoSeries.is_3d

is Closed
---------
.. automethod:: IdaGeoSeries.is_closed

is Empty
--------
.. automethod:: IdaGeoSeries.is_empty

is Measured
-----------
.. automethod:: IdaGeoSeries.is_measured

is Simple
---------
.. automethod:: IdaGeoSeries.is_simple

Length
------
.. automethod:: IdaGeoSeries.length

max M
-----
.. automethod:: IdaGeoSeries.max_m

max X
-----
.. automethod:: IdaGeoSeries.max_x

max Y
-----
.. automethod:: IdaGeoSeries.max_y

max Z
-----
.. automethod:: IdaGeoSeries.max_z

MBR
---
.. automethod:: IdaGeoSeries.mbr

min M
-----
.. automethod:: IdaGeoSeries.min_m

min X
-----
.. automethod:: IdaGeoSeries.min_x

min Y
-----
.. automethod:: IdaGeoSeries.min_y

min Z
-----
.. automethod:: IdaGeoSeries.min_z

num Geometries
--------------
.. automethod:: IdaGeoSeries.num_geometries

num Interior Ring
-----------------
.. automethod:: IdaGeoSeries.num_interior_ring

num Points
----------
.. automethod:: IdaGeoSeries.num_points

perimeter
---------
.. automethod:: IdaGeoSeries.perimeter

Start Point
-----------
.. automethod:: IdaGeoSeries.start_point

SR ID
-----
.. automethod:: IdaGeoSeries.srid

X coordinate
------------
.. automethod:: IdaGeoSeries.x

Y coordinate
------------
.. automethod:: IdaGeoSeries.y

Z coordinate
------------
.. automethod:: IdaGeoSeries.z
