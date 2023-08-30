.. highlight:: python

IdaDataFrame
************

Open an IdaDataFrame Object.
============================

.. currentmodule:: nzpyida.frame

.. autoclass:: IdaDataFrame

   .. automethod:: __init__

.. rubric:: Methods


DataFrame introspection
=======================

internal_state
--------------
.. autoattribute:: IdaDataFrame.internal_state

indexer
-------
.. autoattribute:: IdaDataFrame.indexer

type
----
.. autoattribute:: IdaDataFrame.type

dtypes
------
.. autoattribute:: IdaDataFrame.dtypes

index
-----
.. autoattribute:: IdaDataFrame.index

columns
-------
.. autoattribute:: IdaDataFrame.columns

axes
----
.. autoattribute:: IdaDataFrame.axes

shape
-----
.. autoattribute:: IdaDataFrame.shape

empty
-----
.. autoattribute:: IdaDataFrame.empty

__len__
-------
.. automethod:: IdaDataFrame.__len__

__iter__
---------
.. automethod:: IdaDataFrame.__iter__

DataFrame modification
======================

Selection, Projection
---------------------
.. automethod:: IdaDataFrame.__getitem__

Selection and Projection are also possible using the ``nzpyida.Loc`` object stored in ``IdaDataFrame.loc``.

.. autoclass:: nzpyida.indexing.Loc
	:members:


Filtering
---------

.. automethod:: IdaDataFrame.delete_na 

.. autoclass:: nzpyida.filtering.FilterQuery
	:members:

.. automethod:: IdaDataFrame.__lt__

.. automethod:: IdaDataFrame.__le__

.. automethod:: IdaDataFrame.__eq__

.. automethod:: IdaDataFrame.__ne__

.. automethod:: IdaDataFrame.__ge__

.. automethod:: IdaDataFrame.__gt__


Feature Engineering
-------------------

.. automethod:: IdaDataFrame.__setitem__

.. automethod:: nzpyida.aggregation.aggregate_idadf

.. currentmodule:: nzpyida.frame

.. automethod:: IdaDataFrame.__add__

.. automethod:: IdaDataFrame.__radd__

.. automethod:: IdaDataFrame.__div__

.. automethod:: IdaDataFrame.__rdiv__

.. automethod:: IdaDataFrame.__truediv__

.. automethod:: IdaDataFrame.__rtruediv__

.. automethod:: IdaDataFrame.__floordiv__

.. automethod:: IdaDataFrame.__rfloordiv__

.. automethod:: IdaDataFrame.__mod__

.. automethod:: IdaDataFrame.__rmod__

.. automethod:: IdaDataFrame.__mul__

.. automethod:: IdaDataFrame.__rmul__

.. automethod:: IdaDataFrame.__neg__

.. automethod:: IdaDataFrame.__rpos__

.. automethod:: IdaDataFrame.__pow__

.. automethod:: IdaDataFrame.__rpow__

.. automethod:: IdaDataFrame.__sub__

.. automethod:: IdaDataFrame.__rsub__

.. automethod:: IdaDataFrame.__delitem__

.. automethod:: IdaDataFrame.save_as

DataBase Features
=================

exists
------
.. automethod:: IdaDataFrame.exists

is_view / is_table
------------------
.. automethod:: IdaDataFrame.is_view

.. automethod:: IdaDataFrame.is_table


get_primary_key
---------------
.. automethod:: IdaDataFrame.get_primary_key

ida_query
---------
.. automethod:: IdaDataFrame.ida_query

ida_scalar_query
----------------
.. automethod:: IdaDataFrame.ida_scalar_query


Data Exploration
================

head
----
.. automethod:: IdaDataFrame.head

tail
----
.. automethod:: IdaDataFrame.tail

pivot_table
-----------
.. automethod:: IdaDataFrame.pivot_table

join
----
.. automethod:: IdaDataFrame.join

merge
-----
.. automethod:: IdaDataFrame.merge

concat
------
.. automethod:: nzpyida.join_tables.concat

sort
----
.. automethod:: IdaDataFrame.sort

=======
groupby
-------
.. automethod:: IdaDataFrame.groupby

Obtaing IdaDataFrame with grouped data is possible with aggregate methods of IdaDataFrameGroupBy
 
IdaDataFrameGroupBy
-------------------
.. autoclass:: nzpyida.groupby.IdaDataFrameGroupBy
	:members:


Descriptive Statistics
======================

describe
--------
.. automethod:: IdaDataFrame.describe

cov (covariance)
----------------
.. automethod:: IdaDataFrame.cov

corr (correlation)
------------------
.. automethod:: IdaDataFrame.corr

quantile
--------
.. automethod:: IdaDataFrame.quantile

mad (mean absolute deviation)
-----------------------------
.. automethod:: IdaDataFrame.mad

min (minimum)
-------------
.. automethod:: IdaDataFrame.min

max (maximum)
-------------
.. automethod:: IdaDataFrame.max

count
-----
.. automethod:: IdaDataFrame.count

count_distinct
--------------
.. automethod:: IdaDataFrame.count_distinct

std (standard deviation)
------------------------
.. automethod:: IdaDataFrame.std

var (variance)
--------------
.. automethod:: IdaDataFrame.var

mean
----
.. automethod:: IdaDataFrame.mean

sum
---
.. automethod:: IdaDataFrame.sum

median
------
.. automethod:: IdaDataFrame.median

Import as DataFrame
===================

as_dataframe
------------
.. automethod:: IdaDataFrame.as_dataframe

Connection Management
=====================

commit
------
.. automethod:: IdaDataFrame.commit

rollback
--------
.. automethod:: IdaDataFrame.rollback

