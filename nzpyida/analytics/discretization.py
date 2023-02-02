#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Copyright (c) 2023, IBM Corp.
# All rights reserved.
#
# Distributed under the terms of the BSD Simplified License.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from nzpyida.frame import IdaDataFrame
from nzpyida.base import IdaDataBase
from nzpyida.analytics.utils import map_to_props, materialize_df, make_temp_table_name
from nzpyida.analytics.utils import get_auto_delete_context


class Discretization:
    """
    Generic class for handling data discretization.
    """

    def __init__(self, idadb: IdaDataBase):
        """
        Creates the discretization class.

        Parameters:
        -----------
        idadb : IdaDataBase
            database connector
        """

        self.idadb = idadb
        self.params = {}
        self.proc = ""

    def fit(self, in_df: IdaDataFrame, out_table: str=None) -> IdaDataFrame:
        """
        Create bins limits based on the given data frame.

        Parameters:
        -----------
        in_df : IdaDataFrame
            the input data frame

        out_table : str, optional
            the output table with dicretization bins

        Returns:
        --------
        IdaDataFrame
            the data frame with discretization bins
        """

        temp_view_name, need_delete = materialize_df(in_df)
        auto_delete_context = None
        if not out_table:
            auto_delete_context = get_auto_delete_context('out_table')
            out_table = make_temp_table_name()
        in_columns = ';'.join(in_df.columns)

        params_dict = {
            'intable': temp_view_name,
            'incolumn': in_columns,
            'outtabletype': 'table',
            'outtable': out_table
        }
        params_dict.update(self.params)
        params = map_to_props(params_dict)

        try:
            self.idadb.ida_query(f'call NZA..{self.proc}(\'{params}\')')
        finally:
            if need_delete:
                self.idadb.drop_view(temp_view_name)

        out_df = IdaDataFrame(self.idadb, out_table)

        if auto_delete_context:
            auto_delete_context.add_table_to_delete(out_table)

        return out_df

    def apply(self, in_df: IdaDataFrame, in_bin_df: IdaDataFrame, keep_org_values: bool=False,
        out_table: str=None):
        """
        Apply discretization limits to the given data frame.

        Parameters:
        -----------
        in_df : IdaDataFrame
            the input data frame

        in_bin_df : IdaDataFrame
            the data frame with discretization bins

        keep_org_values : bool, optional
            a flag indicating whether the discretized columns should replace the original
            columns (False) or should be added with another name (True).
            The name of the columns is then prefixed with 'disc_'

        out_table : str, optional
            the output table or view to store the discretized data into
        """

        temp_view_name, need_delete = materialize_df(in_df)
        bin_view_name, bin_view_need_delete = materialize_df(in_bin_df)

        auto_delete_context = None
        if not out_table:
            auto_delete_context = get_auto_delete_context('out_table')
            out_table = make_temp_table_name()

        params = map_to_props({
            'intable': temp_view_name,
            'outtable': out_table,
            'btable': bin_view_name,
            'outtabletype': 'table',
            'replace': not keep_org_values
        })

        try:
            self.idadb.ida_query(f'call NZA..APPLY_DISC(\'{params}\')')
        finally:
            if need_delete:
                self.idadb.drop_view(temp_view_name)
            if bin_view_need_delete:
                self.idadb.drop_view(bin_view_name)

        out_df = IdaDataFrame(self.idadb, out_table)

        if auto_delete_context:
            auto_delete_context.add_table_to_delete(out_table)

        return out_df


class EWDisc(Discretization):
    """
    Discretization with equal width and the given number of bins.
    """

    def __init__(self, idadb: IdaDataBase, bins: int=10):
        """
        Creates the discretization class.

        Parameters:
        -----------
        idadb : IdaDataBase
            database connector

        bins : int, optional
            the default number of discretization bins to be calculated
        """

        super().__init__(idadb)
        self.proc = 'EWDISC'
        self.params = {'bins': bins}


class EFDisc(Discretization):
    """
    Discretization with equal frequency of data.
    """

    def __init__(self, idadb: IdaDataBase, bins: int=10, bin_precision: float=0.1):
        """
        Creates the discretization class.

        Parameters:
        -----------
        idadb : IdaDataBase
            database connector

        bins : int, optional
            the default number of discretization bins to be calculated

        bin_precision : float, optional
            the precision allowed for considering an even distribution of data records in
            the calculated discretization bins. The number of data records in each bin
            must be within [iw-<binprec>*iw,iw+<binprec>*iw] where iw is the size of the
            input table divided by the number of requested discretization bin limits.
        """

        super().__init__(idadb)
        self.proc = 'EFDISC'
        self.params = {'bins': bins, 'binprec': bin_precision}


class EMDisc(Discretization):
    """
    Discretization based on minimizing entropy of the data in the target column.
    """

    def __init__(self, idadb: IdaDataBase, target: str):
        """
        Creates the discretization class.

        Parameters:
        -----------
        idadb : IdaDataBase
            database connector

        target : str
            the input table column containing a class label
        """

        super().__init__(idadb)
        self.proc = 'EMDISC'
        self.params = {'target': target}
