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
import threading
from nzpyida.base import IdaDataBase

class AutoDeleteContext:
    """
    Context manager to lazy removal of temporary tables associated to data frames.
    """
    active_group = threading.local()

    def __init__(self, idadb: IdaDataBase) -> None:
        """
        Creates the context.

        Parameters
        ----------
        idadb : IdaDataBase
            the database connector
        """

        self.idadb = idadb
        self.temp_out_tables = set()

    @staticmethod
    def current():
        """
        Returns an instance of AutoDeleteContext assigned to the current thread or None
        if nothing is assigned.

        Returns
        -------
        AutoDeleteContext
            the current thread AutoDeleteContext or None
        """

        if getattr(AutoDeleteContext.active_group, 'auto_delete_context', False):
            return AutoDeleteContext.active_group.auto_delete_context
        return None

    def add_table_to_delete(self, table_name: str):
        """
        Register a table to be deleted when this context exits.

        Parameters
        ----------
        table_name : str
            the table name
        """
        self.temp_out_tables.add(table_name)

    def __enter__(self):
        if getattr(AutoDeleteContext.active_group, 'auto_delete_context', False):
            AutoDeleteContext.active_group.active_groups += 1
            return AutoDeleteContext.active_group.auto_delete_context
        AutoDeleteContext.active_group.auto_delete_context = self
        AutoDeleteContext.active_group.active_groups = 1
        return self

    def __exit__(self, ex_type, value, traceback):
        AutoDeleteContext.active_group.active_groups -= 1
        if AutoDeleteContext.active_group.active_groups == 0:
            curr = AutoDeleteContext.current()
            for table_name in curr.temp_out_tables:
                if curr.idadb.exists_table(table_name):
                    curr.idadb.drop_table(table_name)
            curr.temp_out_tables = set()
            delattr(AutoDeleteContext.active_group, 'auto_delete_context')
            