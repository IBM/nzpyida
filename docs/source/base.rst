.. highlight:: python

IdaDataBase
***********

Connect to Netezza
==============

.. currentmodule:: nzpyida.base

.. autoclass:: IdaDataBase

   .. automethod:: __init__

.. rubric:: Methods

DataBase Exploration
====================

current_schema
--------------
.. automethod:: IdaDataBase.current_schema

show_tables
-----------
.. automethod:: IdaDataBase.show_tables

show_models
-----------
.. automethod:: IdaDataBase.show_models

exists_table_or_view
--------------------
.. automethod:: IdaDataBase.exists_table_or_view

exists_table
------------
.. automethod:: IdaDataBase.exists_table

exists_view
-----------
.. automethod:: IdaDataBase.exists_view

exists_model
------------
.. automethod:: IdaDataBase.exists_model

is_table_or_view
----------------
.. automethod:: IdaDataBase.is_table_or_view

is_table
--------
.. automethod:: IdaDataBase.is_table

is_view
-------
.. automethod:: IdaDataBase.is_view

is_model
--------
.. automethod:: IdaDataBase.is_model

ida_query
---------
.. automethod:: IdaDataBase.ida_query

ida_scalar_query
----------------
.. automethod:: IdaDataBase.ida_scalar_query

Upload DataFrames
=================

as_idadataframe
---------------
.. automethod:: IdaDataBase.as_idadataframe

Delete DataBase Objects
=======================

drop_table
----------
.. automethod:: IdaDataBase.drop_table

drop_view
---------
.. automethod:: IdaDataBase.drop_view

drop_model
----------
.. automethod:: IdaDataBase.drop_model

DataBase Modification
=====================

rename
------
.. automethod:: IdaDataBase.rename

add_column_id
-------------
.. automethod:: IdaDataBase.add_column_id

delete_column
-------------
.. automethod:: IdaDataBase.delete_column

append
------
.. automethod:: IdaDataBase.append


Connection Management
=====================

commit
------
.. automethod:: IdaDataBase.commit

rollback
--------
.. automethod:: IdaDataBase.rollback

close
-----
.. automethod:: IdaDataBase.close

reconnect
---------
.. automethod:: IdaDataBase.reconnect
