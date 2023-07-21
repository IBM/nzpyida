.. highlight:: python

Quickstart
**********

Connect
=======

First, connect to a remote Netezza database.

nzpy Connection
---------------

For an nzpy connection, if you have nzpyida, you do not have to install drivers on the client side as with the ODBC and JDBC connections.
nzpyida supports a native Python driver nzpy-based connection for Netezza Performance Server.

You can declare connection credentials in the following format.

>>> nzpy_dsn = {
        "database":DATABASE NAME,
        "port" :5480,
        "host": SERVER NAME,
        "securityLevel":0,
        "logLevel":0
       }

To connect to a database, run:

>>> from nzpyida import IdaDataBase
>>> idadb = IdaDataBase(nzpy_dsn, uid="USERNAME", pwd="PASSWORD")


ODBC Connection
---------------

The default connection type for nzpyida is ODBC, which is the default setting for Windows users. First, download an IBM Netezza driver. Then, set up an ODBC connection, including the connection protocol, the port, and the host name.

If your credentials are stored in the Windows ODBC windows connection settings, the database connection can be established very easily by creating an IdaDataBase:

>>> from nzpyida import IdaDataBase
>>> idadb = IdaDataBase(DATASOURCE_NAME)

Otherwise, it is possible to connect with your Netezza credentials:

>>> idadb = IdaDataBase(dsn=DATASOURCE_NAME, uid="<UID>", pwd="<PWD>")

Using ODBC platforms other than Windows may require additional configuration.


JDBC Connection
---------------

The JDBC Connection is based on a Java Virtual Machine, so it is available on every machine that can run Java. In nzpyida, users can choose to use JDBC to connect to a remote Netezza database using JDBC. To be able to use JDBC to connect, you must install and configure IBM Netezza JDBC.


After you download and install the nzjdbc3.jar file, you must include its location in the value of the CLASSPATH environment variable.

>>> export CLASSPATH=PATH TO nzjdbc3.jar:$CLASSPATH

The connection is done using the JDBC URL string.

>>> from nzpyida import IdaDataBase
>>> jdbc_dsn = "jdbc:netezza://IP ADDRESS:PORT/DATABASE NAME"
>>> idadb = IdaDataBase(jdbc_dsn, uid="USERNAME", pwd="PASSWORD")

Conventions
-----------

Users need to create an IdaDataBase instance before they can create an IdaDataFrame. By convention, users should use only one instance of IdaDataBase per database. However, they may use several instances of IdaDataFrame objects per table and connection.

Most methods of the IdaDataBase interface may change the data in the database (they are destructive). However, all methods of the IdaDataFrame interface do not change the physical data in the database. As a result, they can be used without any risk for the data integrity.

Close the connection
--------------------

To ensure expected behaviors, IdaDataBase instances need to be closed. Closing the IdaDataBase is equivalent to closing the connection: once the connection is closed, it is not possible to use the IdaDataBase instance and any IdaDataFrame instances that were opened on this connection anymore.

>>> idadb.close()
'Connection closed.'

If the autocommit mode is activated, then all changes in the IdaDataFrame and others will be commited, otherwise they will be discarded (rollback).

Note: It is possible to reopen the connection of IdaDataBase by using ``IdaDataBase.reconnect()``. This can be useful in case of a timeout or a sloppy connection.

>>> idadb.reconnect()
'The connection was successfully restored'

Manipulate database objects
===========================

Loading a sample table
----------------------

To load a sample table named IRIS that is used in examples in this guide, run the following code:

>>> from nzpyida.sampledata.iris import iris
>>> idadb.as_idadataframe(iris, 'IRIS')

To learn more about data loading, read :ref:`Upload a DataFrame`.


Open an IdaDataFrame
--------------------

Using our previously opened IdaDataBase instance named 'idadb', we can open one or several IdaDataFrame objects. They behave like pointers to remote tables.

Let us open the iris data set, assuming it is stored in the database under the name 'IRIS'

>>> from nzpyida import IdaDataFrame
>>> idadf = IdaDataFrame(idadb, 'IRIS')


Explore data
------------

You can very easily explore the data in the IdaDataFrame by using built in functions

Use ``IdaDataFrame.head`` to get the first n records of your data set (default 5)

>>> idadf.head()
   sepal_length  sepal_width  petal_length  petal_width species
0           5.1          3.5           1.4          0.2  setosa
1           4.9          3.0           1.4          0.2  setosa
2           4.7          3.2           1.3          0.2  setosa
3           4.6          3.1           1.5          0.2  setosa
4           5.0          3.6           1.4          0.2  setosa

Use ``IdaDataFrame.tail`` to get the last n records of your data set (default 5)

>>> idadf.tail()
     sepal_length  sepal_width  petal_length  petal_width    species
145           6.7          3.0           5.2          2.3  virginica
146           6.3          2.5           5.0          1.9  virginica
147           6.5          3.0           5.2          2.0  virginica
148           6.2          3.4           5.4          2.3  virginica
149           5.9          3.0           5.1          1.8  virginica

Note: Because Netezza operates on a distributed system, the order of rows using ``IdaDataFrame.head`` and ``IdaDataFrame.tail`` is not guaranteed unless the table is sorted (using an ‘ORDER BY’ clause) or a column is declared as index for the IdaDataFrame (parameter/attribute ``indexer``). To better mimic the behaviour of a Pandas dataframe the data is sorted by the first numeric column or if there is none the first column in the dataframe. To disable this implicit sorting specify sort=False on ``IdaDataFrame.head`` and ``IdaDataFrame.tail``. 

IdaDataFrame also implements most attributes that are available in a Pandas DataFrame.

>>> idadf.shape
(150,5)

>>> idadf.columns
Index(['sepal_length', 'sepal_width', 'petal_length', 'petal_width',
       'species'],
      dtype='object')

>>> idadf.dtypes
             TYPENAME
sepal_length   DOUBLE
sepal_width    DOUBLE
petal_length   DOUBLE
petal_width    DOUBLE
species       VARCHAR


Simple statistics
-----------------

Several standard statistics functions from the Pandas interface are also available for IdaDataFrame. For example, let us calculate the covariance matrix for the iris data set:

>>> idadf.cov()
              sepal_length  sepal_width  petal_length  petal_width
sepal_length      0.685694    -0.042434      1.274315     0.516271
sepal_width      -0.042434     0.189979     -0.329656    -0.121639
petal_length      1.274315    -0.329656      3.116278     1.295609
petal_width       0.516271    -0.121639      1.295609     0.581006

For more information about methods that are supported by IdaDataFrame objects, see the IdaDataFrame class documentation.

Selection
---------

It is possible to subset the rows of an IdaDataFrame by accessing the IdaDataFrame with a slice object. You can also use the ``IdaDataFrame.loc`` attribute, which contains an ``nzpyida.Loc`` object. However, the row selection might be inaccurate if the current IdaDataFrame is not sorted or does not contain an indexer. This is due to the fact that Netezza stores the data across several nodes if available.

>>> idadf_new = idadf[0:9] # Select the first 10 rows

Alternatively

>>> idadf_new = idadf.loc[0:9]

Which is equivalent to selecting the first 10 IDs in a list:

>>> idadf_new = idadf.loc[[0,1,2,3,4,5,6,7,8,9]]

Of course, this only makes sense if an ID column is provided. Otherwise, the selection is non-deterministic. A warning is shown to users in that case.

Projection
----------

* It is possible to select a subset of columns in an IdaDataFrame.

>>> idadf_new = idadf[['sepal_length', 'sepal_width']]

As in the Pandas interface, this operation creates a new IdaDataFrame instance that is similar to the current one and contains only the selected column(s). This is done to allow users to manipulate the original IdaDataFrame and the new one independently.

>>> idadf_new.head()
   sepal_length  sepal_width
0           5.1          3.5
1           4.9          3.0
2           4.7          3.2
3           4.6          3.1
4           5.0          3.6

Note that ``idadf['sepal_length']`` is not equivalent to ``idadf[['sepa_length']]``. The first one returns an IdaSeries object that behaves like a Pandas.Series object. The second one returns an IdaDataFrame which contains only one column. For example:

>>> idadf_new = idadf[['sepal_length']]
>>> idadf_new.head()
   sepal_length
0           5.1
1           4.9
2           4.7
3           4.6
4           5.0

>>> idaseries = idadf['sepal_length']
>>> idaseries.head()
0    5.1
1    4.9
2    4.7
3    4.6
4    5.0
Name: sepal_length, dtype: float64

* Selection and projection can be done simultaneously by using the ``IdaDataFrame.loc`` attribute.

This selects all even rows in the ``sepal_length`` column:

>>> idadf_new = idadf.loc[::2][['sepal_length']]

Given that an ID column is provided to the data set and declared as an indexer, the selection operates on its ID column. In that case, an ID column has been added to the data set. This column contains unique integers to identify the rows. In the example below we add an ID column and set it as indexer. The default name for this new column is "ID".

>>> idadf = IdaDataFrame(idadb, "IRIS")
>>> idadb.add_column_id(idadf)
>>> idadb.indexer = 'ID'
>>> idadf_new = idadf.loc[::2][['ID', 'sepal_length']]
>>> idadf_new.head(10)
   ID  sepal_length
0   0           5.1
1   2           5.1
2   4           4.6
3   6           5.2
4   8           5.2
5  10           5.5
6  12           5.0
7  14           5.0
8  16           6.5
9  18           6.0

Sorting
-------

Sorting is possible by using ``IdaDataFrame.sort``, which implements similar arguments as ``Pandas.DataFrame.sort``. It is possible to sort in an ascending or descending order, along both axes.

Sort by rows over one column:

>>> idadf_new = idadf.sort("sepal_length")
>>> idadf_new.head()
    ID  sepal_length  sepal_width  petal_length  petal_width species
0  120           4.3          3.0           1.1          0.1  setosa
1  124           4.4          3.0           1.3          0.2  setosa
2   44           4.4          2.9           1.4          0.2  setosa
3   52           4.4          3.2           1.3          0.2  setosa
4   78           4.5          2.3           1.3          0.3  setosa

Sort by rows over several columns:

>>> idadf_new = idadf.sort(["sepal_length","sepal_width"])
>>> idadf_new.head()
    ID  sepal_length  sepal_width  petal_length  petal_width species
0  120           4.3          3.0           1.1          0.1  setosa
1   44           4.4          2.9           1.4          0.2  setosa
2  124           4.4          3.0           1.3          0.2  setosa
3   52           4.4          3.2           1.3          0.2  setosa
4   78           4.5          2.3           1.3          0.3  setosa

Sort by rows over several columns in descending order:

>>> idadf_new = idadf.sort("sepal_length", ascending=False)
>>> idadf_new.head()
    ID  sepal_length  sepal_width  petal_length  petal_width    species
0  144           7.9          3.8           6.4          2.0  virginica
1  105           7.7          3.8           6.7          2.2  virginica
2  106           7.7          2.6           6.9          2.3  virginica
3   37           7.7          2.8           6.7          2.0  virginica
4  111           7.7          3.0           6.1          2.3  virginica

Sort by rows over several columns in descending order inplace:

>>> idadf.sort("sepal_length", ascending=False, inplace=True)
>>> idadf.head()
    ID  sepal_length  sepal_width  petal_length  petal_width    species
0  144           7.9          3.8           6.4          2.0  virginica
1  105           7.7          3.8           6.7          2.2  virginica
2  106           7.7          2.6           6.9          2.3  virginica
3   37           7.7          2.8           6.7          2.0  virginica
4  111           7.7          3.0           6.1          2.3  virginica

Sort by columns:

>>> idadf = IdaDataFrame(idadb, "IRIS", indexer="ID")
>>> idadf.sort(axis = 1, inplace=True)
>>> idadf.head()
   ID  petal_length  petal_width  sepal_length  sepal_width species
0   0           1.4          0.2           5.1          3.5  setosa
1   1           1.5          0.2           5.0          3.4  setosa
2   2           1.4          0.3           5.1          3.5  setosa
3   3           1.5          0.4           5.1          3.7  setosa
4   4           1.0          0.2           4.6          3.6  setosa

Filtering
---------

It is possible to subset the data set depending on one or several criteria, which can be combined. Filters are based on string or integer values.

The supported comparison operators are: <, <=, ==, !=, >=, >.

Select all rows for which the 'sepal_length' value is smaller than 5:

>>> idadf.shape
(150,5)

>>> idadf_new = idadf[idadf['sepal_length'] < 5]
>>> idadf_new.head()
    ID  sepal_length  sepal_width  petal_length  petal_width species
0   46           4.8          3.4           1.6          0.2  setosa
1  119           4.8          3.0           1.4          0.1  setosa
2  118           4.9          3.1           1.5          0.1  setosa
3   66           4.7          3.2           1.3          0.2  setosa
4   49           4.8          3.4           1.9          0.2  setosa

>>> idadf_new.shape
(22, 5) # Here we can see that only 22 records meet the criterion

Select all samples belonging to the 'versicolor' species:

>>> idadf_new = idadf[idadf['species'] == 'versicolor']
>>> idadf_new.head()
   ID  sepal_length  sepal_width  petal_length  petal_width     species
0  89           6.7          3.0           5.0          1.7  versicolor
1  56           5.8          2.7           4.1          1.0  versicolor
2  32           5.7          2.8           4.1          1.3  versicolor
3  92           6.0          3.4           4.5          1.6  versicolor
4  99           5.1          2.5           3.0          1.1  versicolor

Filtering criteria can also be combined. The supported Boolean symbols are: &, \|, ^

Select all samples belonging to the 'versicolor' species with a 'sepal_length' smaller than 5:

>>> criterion = (idadf['species'] == 'versicolor')&(idadf['sepal_length'] < 5)
>>> idadf_new = idadf[criterion ]
>>> idadf_new.head()
    ID  sepal_length  sepal_width  petal_length  petal_width     species
0  128           4.9          2.4           3.3            1  versicolor

Conclusion: there is only one sample for which both conditions are true.

Feature Engineering
-------------------

New columns in an IdaDataFrame can be defined based on the aggregation of existing columns and numbers. The following operations are defined: +, -, \*, /, //, %, \*\*. This happens in a non-destructive way, which means that the original data in the database remains unchanged. A view is created in which user aggregations are defined. The following operations are possible:

Add a new column by aggregating existing columns:

>>> idadf['new'] = idadf['sepal_length'] * idadf['sepal_width']
>>> idadf.head()
   ID  sepal_length  sepal_width  petal_length  petal_width species    new
0   0           5.1          3.5           1.4          0.2  setosa  17.85
1   1           5.0          3.4           1.5          0.2  setosa  17.00
2   2           5.1          3.5           1.4          0.3  setosa  17.85
3   3           5.1          3.7           1.5          0.4  setosa  18.87
4   4           4.6          3.6           1.0          0.2  setosa  16.56

Modify an existing column:

>>> idadf['new'] = 2 ** idadf['petal_length']
>>> idadf.head()
   ID  sepal_length  sepal_width  petal_length  petal_width species       new
0   0           5.1          3.5           1.4          0.2  setosa  2.639016
1   1           5.0          3.4           1.5          0.2  setosa  2.828427
2   2           5.1          3.5           1.4          0.3  setosa  2.639016
3   3           5.1          3.7           1.5          0.4  setosa  2.828427
4   4           4.6          3.6           1.0          0.2  setosa  2.000000

Modify an existing columns based on itself:

>>> idadf['new'] = idadf['new'] - idadf['new'].mean()
>>> idadf.head()
   sepal_length  sepal_width  petal_length  petal_width     species        new
0           4.4          2.9           1.4          0.2      setosa -21.867544
1           5.6          2.9           3.6          1.3  versicolor -12.380828
2           5.4          3.9           1.3          0.4      setosa -22.044271
3           5.0          3.4           1.5          0.2      setosa -21.678133
4           5.8          2.6           4.0          1.2  versicolor  -8.506560

Delete colummns:

>>> del idadf['new']
>>> del idadf['species']

Modify existing columns:

>>> idadf['sepal_length'] = idadf['sepal_length'] / 2
   ID  sepal_length  sepal_width  petal_length  petal_width
0   0          2.55          3.5           1.4          0.2
1   1          2.50          3.4           1.5          0.2
2   2          2.55          3.5           1.4          0.3
3   3          2.55          3.7           1.5          0.4
4   4          2.30          3.6           1.0          0.2

Modify several or all columns at the same time:

>>> newidadf = idadf[['sepal_length', 'sepal_width']] + 2
>>> idadf[['sepal_length', 'sepal_width']] = newidadf
>>> idadf.head()
   ID  sepal_length  sepal_width  petal_length  petal_width
0   0          4.55          5.5           1.4          0.2
1   1          4.50          5.4           1.5          0.2
2   2          4.55          5.5           1.4          0.3
3   3          4.55          5.7           1.5          0.4
4   4          4.30          5.6           1.0          0.2

>>> idadf = idadf + idadf['sepal_length'].var()
>>> idadf.head() # Possible because all columns are numeric
         ID  sepal_length  sepal_width  petal_length  petal_width
0  0.171423      4.721423     5.671423      1.571423     0.371423
1  1.171423      4.671423     5.571423      1.671423     0.371423
2  2.171423      4.721423     5.671423      1.571423     0.471423
3  3.171423      4.721423     5.871423      1.671423     0.571423
4  4.171423      4.471423     5.771423      1.171423     0.371423

These examples show what you can do with IdaDataFrame/IdaSeries instances. However, such chaining operations may slow down the processing of the IdaDataFrame, because the values of the new columns are calculated on the fly and are not physically available in the database.

Use ``IdaDataFrame.save_as`` after aggregating the columns of the IdaDataFrame several times to rely on physical instead of virtual data. Morever, by using the ``IdaDataFrame.save_as`` function, all modifications will be permanently backed up in the database. Otherwise, all changes are lost when the connection terminates.

In nzpyida, it is not possible to directly aggreate columns from other tables. This would require a join operation. Some work has to be done in this direction later.

Machine Learning
================

nzpyida provides a wrapper for several machine learning algorithms that are developed for in-database use. These algorithms are implemented in PL/SQL and C++. 
Currently, there are wrappers for the following algorithms: Decision Trees, Naive Bayes, KNN, Linear Regression, Regression Trees and K-means. 

.. note:: To learn how to create IRIS table, read :ref:`Upload a DataFrame`

The following example uses K-means:

>>> idadf = IdaDataFrame(idadb, 'IRIS')
>>> idadb.add_column_id(idadf)
# In-DataBase Kmeans needs an 'id' column identify each row

>>> from nzpyida.analytics import KMeans
>>> kmeans = KMeans(idadb, model_name='kmeans_model') 

>>> from nzpyida.analytics import AutoDeleteContext
>>> with AutoDeleteContext(idadb):
>>>   kmeans.fit(idadf, k=3) # configure clustering with 3 cluters
>>>   outdf = kmeans.predict(idadf)

>>> print(kmeans.describe())
| CLUSTERID | NAME | SIZE | RELSIZE          | WITHINSS        | DESCRIPTION |
+-----------+------+------+------------------+-----------------+-------------+
| 1         | 1    | 44   | 0.29333333333333 | 48.205047126212 |             |
| 2         | 2    | 56   | 0.37333333333333 | 63.566407545082 |             |
| 3         | 3    | 50   | 0.33333333333333 | 47.350621105571 |             |
+-----------+------+------+------------------+-----------------+-------------+


In the second example, a simple classification algorithm is shown together with input data sampling.
The algorithm is called K-Nearest Neighbors.

>>> idadf = IdaDataFrame(idadb, 'IRIS')
>>> idadb.add_column_id(idadf)
# In-DataBase Kmeans needs an 'id' column identify each row

>>> from nzpyida.analytics import random_sample
>>> from nzpyida.analytics import KNeighborsClassifier
>>> from nzpyida.analytics import AutoDeleteContext
>>>
>>> with AutoDeleteContext(idadb):
>>>   # sample 50% of rows for training
>>>   sample_df=random_sample(in_df=idadf, fraction=0.5)
>>>
>>>   # create and train the model with a data sample
>>>   model = KNeighborsClassifier(idadb, model_name='knn_model')
>>>   model.fit(in_df=sample_df, target_column='"species"')
>>>
>>>   # test and score the model using all the data
>>>   print(model.score(in_df=idadf, target_column='"species"'))


To learn how to use other machine learning algorithms, refer to the detailed documentation.


Database administration
=======================

.. _Upload a DataFrame:
Upload a DataFrame
------------------

It is possible to upload a local Pandas DataFrame to a Netezza instance. A few data sets are also included in nzpyida. For example, to upload the data set iris, issue the following command:

>>> from nzpyida.sampledata.iris import iris
>>> idadb.as_idadataframe(iris, 'IRIS')

The column data types of the Pandas DataFrame are detected and then mapped to database types, such as DOUBLE and VARCHAR. The mapping is quite basic, but it can handle most use cases. More work has to be done to improve storage and to include special data types, such as datetime and timestamp. Currently, the Boolean data type and all string and numeric data types are supported.

If a table or a view called 'IRIS' already exists, an error message occurs, the ``clear_existing`` argument drops the table before it is uploaded if it already exists.

>>> idadb.as_idadataframe(iris, 'IRIS', clear_existing=True)

Note that the function returns an IdaDataFrame object pointing to the newly uploaded data set, so that we can directly start using it.

nzpyida uses a sophisticated chunking mechanism to improve the performance of this operation. However, the speed depends on the network connection. You can upload several million DataFrame rows in reasonable time using this function.

Download a data set
-------------------

It is possible to download a data set from a Netezza instance.

>>> idadf = IdaDataFrame(idadb, 'IRIS')
>>> iris = idadf.as_dataframe()


Database types are mapped to Pandas data types, such as objects for strings and floats for numeric values. However, if the data set is too big, this may take a long time. If the connection is lost, it fails and throws an error.

Explore the Database
--------------------

To get a list of existing tables in the database, use the ``IdaDataBase.show_tables()`` function.

>>> idadb.show_tables()
     TABSCHEMA           TABNAME       OWNER TYPE
0    DASHXXXXXX            SWISS  DASHXXXXXX    T
1    DASHXXXXXX             IRIS  DASHXXXXXX    T
2    DASHXXXXXX     VIEW_TITANIC  DASHXXXXXX    V

Several other Database administration features are available. For more information, see the IdaDataBase object documentation.
