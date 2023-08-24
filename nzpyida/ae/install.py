#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright (c) 2015-2023, IBM Corp.
# All rights reserved.
#
# Distributed under the terms of the BSD Simplified License.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

"""
In-Database User defined functions
"""
import inspect
import textwrap
from nzpyida import IdaDataBase, IdaDataFrame

from nzpyida.ae import shaper, result_builder


class NZInstall(object):

    def __init__(self, idadb, package_name):
        """
        Constructor for install
        """
        self.package_name=package_name
        self.db =idadb



    def  getResultCode(self):

        
        ae_name = "nzpyida..py_udtf_install"

        output_signature = {'ResultCode': 'int'}
        base_code = shaper.get_base_shaper_install(output_signature, self.package_name)

        run_string = textwrap.dedent(""" BaseShaperUdtf.run()""")

        final_code = base_code + "\n" + run_string

        columns_string = "'CODE_TO_EXECUTE=" + "\"" + final_code + "\"" + "'"
        query = "select * from table with final (" + ae_name + "(" + columns_string + ")) "


        result = self.db.ida_query(query)

        if(len(result.values)>0):
            return result.values[0]






