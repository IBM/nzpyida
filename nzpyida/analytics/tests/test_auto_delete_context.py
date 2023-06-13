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

from nzpyida import IdaDataBase, IdaDataFrame
from nzpyida.analytics.auto_delete_context import AutoDeleteContext
import pytest
from nzpy.core import ProgrammingError
import importlib

new_table_command = """
drop table test_table if exists;
CREATE TABLE test_table (col1 int, col2 int, col3 int);
insert into test_table (col1, col2, col3) values (11, 22, 33);
"""
new_table_command_2="""
DROP TABLE TEST_TABLE_2 IF EXISTS;
CREATE TABLE test_table_2 (col1 int, col2 int, col3 int);
insert into test_table_2 (col1, col2, col3) values (12, 23, 34);
"""


@pytest.fixture
def create_test_table(idadb: IdaDataBase):
    idadb.ida_query(new_table_command)
    yield
    idadb.ida_query("DROP TABLE TEST_TABLE IF EXISTS")

@pytest.fixture
def create_test_table_2(idadb: IdaDataBase):
    idadb.ida_query(new_table_command)
    idadb.ida_query(new_table_command_2)
    yield
    idadb.ida_query("DROP TABLE TEST_TABLE IF EXISTS")
    idadb.ida_query("DROP TABLE TEST_TABLE_2 IF EXISTS")


class TestCurrentAutoDeleteContext:
    def test_context_present(self, idadb: IdaDataBase):
        with AutoDeleteContext(idadb) as adc:
            assert adc.current()

    def test_context_not_present(self, idadb):
        with AutoDeleteContext(idadb) as adc:
            pass
        assert not adc.current()

    def test_nested_contexts(self, idadb):
        with AutoDeleteContext(idadb) as adc1:
            with AutoDeleteContext(idadb) as adc2:
                assert adc1 != adc2
                assert adc2 == AutoDeleteContext.current()
            assert adc1 == AutoDeleteContext.current()


class TestAddTableToDelete:
    def test_table_not_deleted(self, idadb, create_test_table):
        with AutoDeleteContext(idadb) as adc:
            df = IdaDataFrame(idadb, 'TEST_TABLE')
            adc.add_table_to_delete('TEST_TABLE')
            assert df

    def test_table_deleted(self, idadb, create_test_table):
        with pytest.raises(ProgrammingError, match=r".*relation does not exist*"):
            with AutoDeleteContext(idadb) as adc:
                df = IdaDataFrame(idadb, 'TEST_TABLE')
                adc.add_table_to_delete('TEST_TABLE')
            assert df
    
    def test_double_tables_deleted(self, idadb, create_test_table_2):
        with AutoDeleteContext(idadb) as adc:
            df = IdaDataFrame(idadb, 'TEST_TABLE')
            df2 = IdaDataFrame(idadb, 'TEST_TABLE_2')
            adc.add_table_to_delete('TEST_TABLE')
            adc.add_table_to_delete('TEST_TABLE_2')
        assert not any([df.exists(), df2.exists()])

    def test_double_table_one_not_deleted(self, idadb, create_test_table_2):
        with AutoDeleteContext(idadb) as adc:
            df = IdaDataFrame(idadb, 'TEST_TABLE')
            df2 = IdaDataFrame(idadb, 'TEST_TABLE_2')
            adc.add_table_to_delete('TEST_TABLE')
        assert not df.exists() and df2.exists()

    def test_double_tables_deleted(self, idadb, create_test_table_2):
        with AutoDeleteContext(idadb) as adc:
            df = IdaDataFrame(idadb, 'TEST_TABLE')
            df2 = IdaDataFrame(idadb, 'TEST_TABLE_2')
            adc.add_table_to_delete('TEST_TABLE')
            adc.add_table_to_delete('TEST_TABLE_2')
        assert not any([df.exists(), df2.exists()])

    