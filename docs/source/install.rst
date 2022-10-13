Install
*******

Installing
----------

Install ibmdbpy from pip by issuing this statement::

	> pip install ibmdbpy

Updating
--------

Ibmdbpy changes a lot. We regularly fix bugs and add new features. To update your ibmdbpy installation, issue this statement::

	> pip install ibmdbpy --update --no-deps

Introduction
------------

This statement builds the project from source::

 	> python setup.py install

These statements build the documentation::

	> cd docs
	> make html
	> make latex

These statements run the test::

	> cd tests
	> py.test

By default, pytest assumes that a database named "BLUDB" is reachable via an ODBC connection and that its credentials are stored in your ODBC settings. This may be not the case for most users, so several options are avaiable:

	* ``--dsn`` : Data Source Name
	* ``--uid``, ``--pwd`` : Database login and password
	* ``--jdbc`` : jdbc url string
	* ``--table`` : Table to use to test (default: iris)

For testing, all tables from ibmdbpy.sampledata can be used. These include the tables iris, swiss, and titanic.

Strict dependencies
-------------------

Ibmdbpy uses data structures and methods from two common Python libraries - Pandas and Numpy - and additionally depends on a few pure-python libraries, which makes ibmdbpy easy to install:

	* pandas
	* numpy
	* future
	* six
	* lazy
	* pypyodbc

Optional dependencies
---------------------

Some optional libraries can be installed to benefit from extra features, for example:

	* JayDeBeApi with JPype1 (Connection via JDBC)
	* pytest (for running tests)
	* sphinx (for building the documentation)

You can install ibmdbpy together with the required versions of the JayDeBeApi and JPype1 packages
for the JDBC support by executing the following statement::

  > pip install ibmdbpy[jdbc]

JayDeBeApi and JPype1 require a C++ compiler, which may make it difficult to install for some users.

Package structure
-----------------

.. code-block:: none

	ibmdbpy-master
	├── LICENSE.txt
	├── MANIFEST.in
	├── README.rst
	├── setup.cfg
	├── setup.py
	├── docs
	│   ├── ...
	└── ibmdbpy
	    ├── __init__.py
	    ├── aggregation.py
	    ├── base.py
	    ├── exceptions.py
	    ├── filtering.py
	    ├── frame.py
	    ├── geoFrame.py
        ├── geoSeries.py
	    ├── indexing.py
	    ├── internals.py
	    ├── series.py
	    ├── sql.py
	    ├── statistics.py
	    ├── internals.py
	    ├── utils.py
	    ├── benchmark
	    │   ├── __init__.py
	    │   ├── benchmark.py
	    ├── learn
	    │   ├── __init__.py
	    │   ├── association_rules.py
	    │   ├── kmeans.py
	    │   ├── naive_bayes.py
	    ├── sampledata
	    │   ├── ...
	    └── tests
	    	├── conftest.py
	        └── ...