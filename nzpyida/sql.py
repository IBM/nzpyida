#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Copyright (c) 2015, IBM Corp.
# All rights reserved.
#
# Distributed under the terms of the BSD Simplified License.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

""" Utility functions """

# Python 2 compatibility
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import int
from future import standard_library
standard_library.install_aliases()

import decimal
import numpy as np
import os

import pandas as pd
from pandas.io.sql import read_sql

def _prepare_query(query_string, silent = False):
    """
    Return a formatted query string and print query if verbose mode activated

    Parameters
    ----------
    query_string : str
        String to be printed
    silent : bool, default: False
        If True, the query will not be printed at all

    Returns
    -------
    querystring : str
    """
    if silent is False:
        if os.getenv('VERBOSE') == 'True':
            print("> " + query_string)
    return query_string

def _prepare_and_execute(idaobject, query, autocommit = True, silent = False):
    """
    See IdaDataBase._prepare_and_execute
    """
    # Open a cursor
    cursor = idaobject._con.cursor()

    try:
        query = _prepare_query(query, silent)
        #print(query)
        cursor.execute(query)
        if autocommit is True:
            idaobject._autocommit()
    except:
        if idaobject._is_netezza_system():
            idaobject.reconnect()
        raise
    else:
        return True
    finally:
        cursor.close()

def ida_query(idadb, query, silent=False, first_row_only=False, autocommit = False):
    """
    See IdaDataBase.ida_query
    
    Notes
    -----
    This method calls as appropriate either 
    _ida_query_ODBC(), or 
    _ida_query_JDBC()
    """
    if idadb._con_type == 'odbc':
        return _ida_query_ODBC_new(idadb, query, silent, first_row_only, autocommit)
    elif idadb._con_type == 'nzpy':
        return _ida_query_NZPY(idadb, query, silent, first_row_only, autocommit)
    else:
        return _ida_query_JDBC(idadb, query, silent, first_row_only, autocommit)

def _ida_query_ODBC_new(idadb, query, silent, first_row_only, autocommit):
    """
       For ODBC connections no further work needs to be done regarding
       CLOB retrieval because it's fixed with a configuration keyword at
       connection creation point. See IdaDataBase.__init__
       """

    if (query.strip()[:6].upper() == "SELECT") & first_row_only is True:
            query = "select * from (" + query + ") as T LIMIT 1"

    query = _prepare_query(query, silent)

    try:
        result = read_sql(query, idadb._con)
        if first_row_only is True:
            if result.shape[0] > 0:
                tuple_as_list = list(result.values[0])
                for index, element in enumerate(tuple_as_list):
                    if element is None:
                        tuple_as_list[index] = np.nan
                    if isinstance(element, decimal.Decimal):
                        tuple_as_list[index] = int(element)
                result = tuple(tuple_as_list)
            else:
                #first_row_only is True but the query retuned nothing
                result = tuple()
        else:
            if result.shape[1] == 1:
                #convert to Series if only one column
                # Note: This may solve problem in case the interface does not
                # get properly the column names. This is just a suggestion.
                # Uncomment it if you feel you need it, but so far it worked well without
                # result.columns = [column[0] for column in cursor.description]
                result = result[result.columns[0]]

        return result
    except Exception as e:

        str_to_check = "\'NoneType\' object is not iterable"

        if str(e) == str_to_check:
            # query is not a select-statement, but its execution succeeded
            if autocommit is True:
                idadb.commit()
                return None
        else:
            raise e


def _ida_query_ODBC(idadb, query, silent, first_row_only, autocommit):
    """
    For ODBC connections no further work needs to be done regarding
    CLOB retrieval because it's fixed with a configuration keyword at
    connection creation point. See IdaDataBase.__init__
    """
    cursor = idadb._con.cursor()
    try:
        query = _prepare_query(query, silent)
        cursor.execute(query)

        if autocommit is True:
            idadb._autocommit()            
        try:
            firstRow = cursor.fetchone()
        except:
            return None #non-SELECT query, didn't return anything
        else:
            #query with SELECT statement, mind that resultset might be empty
            if first_row_only is True:
                if firstRow is not None:
                    #this following processing was proposed by Edouard
                    tuple_as_list = list(tuple(firstRow))
                    for index, element in enumerate(tuple_as_list):
                        if element is None:
                            tuple_as_list[index] = np.nan
                        if isinstance(element, decimal.Decimal):
                            tuple_as_list[index] = int(element)
                    result = tuple(tuple_as_list)
                else:
                    #first_row_only is True but the query retuned nothing
                    return tuple()
            else:
                result = read_sql(query, idadb._con) 
                #convert to Series if only one column   
                
                # Note: This may solve problem in case the interface does not
                # get properly the column names. This is just a suggestion. 
                # Uncomment it if you feel you need it, but so far it worked well without
                # result.columns = [column[0] for column in cursor.description]
                
                if len(result.columns) == 1:
                    result = result[result.columns[0]]                        
            return result
    except:
        raise
    finally:
        cursor.close()

def _ida_query_NZPY(idadb, query, silent, first_row_only, autocommit):
    """
    """
    cursor = idadb._con.cursor()
    try:
        query = _prepare_query(query, silent)
        cursor.execute(query)

        if autocommit is True:
            idadb._autocommit()
        try:
            firstRow = cursor.fetchone()
        except:
            return None #non-SELECT query, didn't return anything
        else:
            #query with SELECT statement, mind that resultset might be empty
            if first_row_only is True:
                if firstRow is not None:
                    #this following processing was proposed by Edouard
                    tuple_as_list = list(tuple(firstRow))
                    for index, element in enumerate(tuple_as_list):
                        if element is None:
                            tuple_as_list[index] = np.nan
                        if isinstance(element, decimal.Decimal):
                            tuple_as_list[index] = int(element)
                    result = tuple(tuple_as_list)
                else:
                    #first_row_only is True but the query retuned nothing
                    return tuple()
            else:
                colnames=[]
                if type(cursor.description[0][0]) == bytes:
                    colnames = [column[0].decode() for column in cursor.description]
                else:
                    colnames = [column[0] for column in cursor.description]
                if firstRow is None:
                    result = pd.DataFrame([], columns= colnames)
                else:
                    data = [firstRow]
                    data.extend(cursor.fetchall())
                    result = pd.DataFrame(data, columns= colnames)
                #convert to Series if only one column
                if len(result.columns) == 1:
                    result = result[result.columns[0]]
            return result
    except:
        raise
    finally:
        cursor.close()

def _ida_query_JDBC(idadb, query, silent, first_row_only, autocommit):
    """
    For JDBC connections, the CLOBs are retrieved as handles from which
    strings need to be manually extracted. 

    The retrieval is done row by row due to progressiveStreaming feature of 
    Db2 Warehouse which closes a CLOB handle as soon as the cursor moves to
    the next row.
    
    Efforts to disable the progressiveStreaming feature where done but had
    no success. See IdaDataBase.__init__
    Were it possible to disable the feature, no separate method for ida_query 
    for JDBC would be needed, as the CLOB would be retrieved as actual strings
    instead of handles.
    
    If there are no CLOB columns, Pandas' read_sql method is used
    """
    cursor = idadb._con.cursor()
    try:
        query = _prepare_query(query, silent)
        cursor.execute(query)
        if autocommit is True:
            idadb._autocommit()
        try:
            firstRow = cursor.fetchone()
        except:
            return None #non-SELECT query, didn't return anything
        else:        
            #query with SELECT statement, mind that resultset might be empty
            colNumbersWithCLOBs = []
            if firstRow is not None:
               #identify CLOB columns               
               for index, col in enumerate(firstRow):
                   if hasattr(col, "getSubString") and hasattr(col, "length"):
                       colNumbersWithCLOBs.append(index)
               
               firstRow = list(firstRow)
               #replace CLOB's (if any) in the first row
               if colNumbersWithCLOBs:
                   for colNum in colNumbersWithCLOBs:
                       firstRow[colNum] = firstRow[colNum].getSubString(1, firstRow[colNum].length())
            if first_row_only is True:
                if firstRow is None:
                    return tuple()
                else:
                    #this following processing was proposed by Edouard
                    for index, element in enumerate(firstRow):
                        if element is None:
                            firstRow[index] = np.nan
                        if isinstance(element, decimal.Decimal):
                            firstRow[index] = int(element)
                    result = tuple(firstRow)
            else:
                #first_row_only is False
                #get the column names for the DataFrame
                colNames = [column[0] for column in cursor.description]
                data = [firstRow]
                if firstRow is None:
                    # return an empty data frame
                    result = pd.DataFrame(columns=colNames)
                elif colNumbersWithCLOBs:
                    #use the already retrieved row and retrieve the remaining
                    data = []
                    row = firstRow
                    while row is not None:
                        data.append(row)
                        row = cursor.fetchone() #this returns a tuple
                        if row is not None:
                            row = list(row)
                            for colNum in colNumbersWithCLOBs:
                                try:
                                    # Check needed because some DB2GSE functions
                                    # return Null, which is then interpreted as
                                    # None, which doesn't have getSubString method
                                    row[colNum] = row[colNum].getSubString(
                                                        1, row[colNum].length())
                                except:
                                    pass
                    result = pd.DataFrame(data, columns = colNames)
                else:
                    # fetch the remaining rows
                    data.extend(cursor.fetchall())
                    result = pd.DataFrame(data, columns= colNames)

                #convert to Series if only one column
                if len(result.columns) == 1:
                    result = result[result.columns[0]]
            return result                       
            
    except:
        if idadb._is_netezza_system():
            idadb.reconnect()
        raise
    finally:
        cursor.close()
            
def ida_scalar_query(idadb, query, silent = False, autocommit = False):
    """
    See IdaDataBase.ida_scalar_query
    """
    # Open a cursor
    cursor = idadb._con.cursor()

    try:
        query = _prepare_query(query, silent)
        cursor.execute(query)
        
        if autocommit is True:
            idadb._autocommit()
            
        result = cursor.fetchone()[0]
        if result is None:
            result = np.nan
    except:
        if idadb._is_netezza_system():
            idadb.reconnect()
        raise
    finally:
        cursor.close()
    return result
