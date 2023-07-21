.. nzpyida documentation master file, created by
   sphinx-quickstart on Tue Jul 14 13:18:19 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


.. nzpyida documentation master file, created by
   sphinx-quickstart on Tue Jul 14 13:18:19 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

nzpyida
*******

Accelerating Python Analytics by In-Database Processing
=======================================================

The nzpyida project provides a Python interface to the in-database data-manipulation algorithms provided by IBM Netezza:

* It accelerates Python analytics by seamlessly pushing operations written in Python into the underlying database for execution, thereby benefitting from in-database performance-enhancing features, such as columnar storage and parallel processing.
* It can be used by Python developers with very little additional knowledge, because it copies the well-known interface of the Pandas library for data manipulation and the Scikit-learn library for the use of machine learning algorithms.
* It is compatible with Python 3.6.
* It can connect to Netezza databases via nzpy, ODBC or JDBC.

**nzpyida = NeteZa PYthon In Database Analytics**

The latest version of nzpyida is available on the `Python Package Index`__ and Github_.

__ https://pypi.python.org/pypi/nzpyida

.. _Github: https://github.com/IBM/nzpyida

How nzpyida works
-----------------

The nzpyida project translates Pandas-like syntax into SQL and uses a middleware API (like pypyodbc or nzpy) to send it to an ODBC, JDBC or nzpy connected database for execution. The results are fetched and formatted into the corresponding data structure, for example, a Pandas.Dataframe or a Pandas.Series.

The following scenario illustrates how nzpyida works.

Issue the following statements to connect via nzpy to a Netezza database server NETEZZA_HOSTNAME on port 5480 logging in as DATABASE_USER with password PASSWORD.
The database to use on that server is DATABASE.

>>> from nzpyida import IdaDataBase, IdaDataFrame
>>> nzpy_cfg = {
    'user': 'DATABASE_USER',
    'password': 'PASSWORD', 
    'host': 'NETEZZA_HOSTNAME', 
    'port': 5480, 
    'database': 'DATABASE', 
    'logLevel': 0, 
    'securityLevel': 0}
>>> idadb = IdaDataBase(nzpy_cfg)

A few sample data sets are included in nzpyida for you to experiment. First, we can load the IRIS table into this database instance.

>>> from nzpyida.sampledata import iris
>>> idadb.as_idadataframe(iris, "IRIS")

Next, we can create an IDA data frame that points to the table we just uploaded:

>>> idadf = IdaDataFrame(idadb, 'IRIS')

Note that to create an IDA data frame using the IdaDataFrame object, we need to specify our previously opened IdaDataBase object, because it holds the connection.

Next, we compute the correlation matrix:

>>> idadf.corr()

In the background, nzpyida looks for numerical columns in the table and
builds an SQL request that returns the correlation between each pair of columns.

The result fetched by nzpyida is a tuple containing all values of the matrix.
This tuple is formatted back into a Pandas.DataFrame and then returned::


                 sepal_length  sepal_width  petal_length  petal_width
   sepal_length      1.000000    -0.117570      0.871754     0.817941
   sepal_width      -0.117570     1.000000     -0.428440    -0.366126
   petal_length      0.871754    -0.428440      1.000000     0.962865
   petal_width       0.817941    -0.366126      0.962865     1.000000


Table of Contents
=================

.. highlight:: python

.. toctree::
   :maxdepth: 2

   install.rst
   start.rst
   base.rst
   frame.rst
   geospatial.rst
   analytics.rst
   utils.rst
   legal.rst

Contributors
============

The nzpyida is based on ibmdbpy project developed for IBM Db2 Warehouse.
See https://github.com/ibmdbanalytics/ibmdbpy for details.

Indexes and tables
==================

* :ref:`genindex`
* :ref:`modindex`



