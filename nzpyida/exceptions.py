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
Custom exceptions module
"""

#----------------------------------------------------------------------
# Custom Exception Classes

class Error(Exception):
    """
    This is the base class of all other exceptions thrown by ibmdbpy. It can 
    be used to catch all exceptions with a single except statement.
    """
    def __init__(self, message):
        """
        This is the constructor which take one string argument.
        """
        self._message = message
    def __str__(self):
        """Converts the message to a string."""
        return('ibmdbpy::'+str(self.__class__.__name__)+': '+str(self._message))

class IdaDataBaseError(Error):
    """
    This exception is raised when an error occurs while you manipulate the IdaDataBase interface. 
    """
    pass

class IdaDataFrameError(Error):
    """
    This exception is raised when an error occurs while you manipulate the IdaDataFrame interface. 
    """
    pass

class PrimaryKeyError(Error):
    """
    This exception is raised when an error occurs because of a missing or
    eroneous primary key column.
    """
    pass

class IdaKMeansError(Error):
    """
    This exception is raised when an error related to the ibmdbpy KMeans
    clustering occurs.
    """
    pass

class IdaAssociationRulesError(Error):
    """
    This exception is raised when an error related to ibmdbpy Association Rules occurs.
    """
    pass

class IdaNaiveBayesError(Error):
    """
    This exception is raised when an error related to ibmdbpy Naive Bayes occurs.
    """
    pass

class IdaGeoDataFrameError(Error):
    """
    This exception is raised when an error occurs while you manipulate the IdaDataFrame
    """