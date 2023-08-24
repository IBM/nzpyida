Install
*******

Installing
----------

Install nzpyida from pip3 by issuing this statement::

	> pip3 install nzpyida

Updating
--------

To update your nzpyida installation, issue this statement::

	> pip3 install nzpyida --upgrade --no-deps

Ensure you have INZA version 11.2.1.0 or later installed in your Netezza database.

Building and Installing from Source
-----------------------------------

This statement builds the project from source::

 	> python setup.py install

These statements build the documentation::

	> cd docs
	> make html
	> make latex

These statements run the test::

	> cd tests
	> py.test --hostname HOSTNAME --dsn DB --uid USERID --pwd PASS

Where HOSTNAME is your Netezza server hostname or IP, DB is the database name and USERID and PASS are login credentials.
The database must have INZA enabled, otherwise some tests will fail.


Strict dependencies
-------------------

nzpyida uses data structures and methods from two common Python libraries - Pandas and Numpy - and additionally depends on a few pure-python libraries, which makes nzpyida easy to install:

	* pandas
	* numpy
	* six
	* lazy

In addition, for establishing connection to Netezza database one of the following libraries is needed:

	* nzpy - for nzpy connectivity
	* pypyodbc - for ODBC connectivity
	* jaydebeapi - for JDBC connectivity

Optional dependencies
---------------------

Some optional libraries can be installed to benefit from extra features, for example:

	* pytest (for running tests)
	* sphinx (for building the documentation)
