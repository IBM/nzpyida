#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Copyright (c) 2023, IBM Corp.
# All rights reserved.
#
# Distributed under the terms of the BSD Simplified License.
#
# The full license is in the LICENSE file, distributed with this software.
#----------------------------------------------------------------------------- 

from nzpyida.base import IdaDataBase
from typing import List

class ModelManager:
    def __init__(self, idadb: IdaDataBase):
        self.idadb = idadb

    def list_models(self, query: str=''):
        if query:
            return self.idadb.ida_query('select * from V_NZA_MODELS where {}'.format(query))
        else:
            return self.idadb.ida_query('select * from V_NZA_MODELS')

    def model_exists(self, name: str) -> bool:
        ret = self.idadb.ida_query('call NZA..MODEL_EXISTS(\'model={}\')'.format(name))
        return not ret.empty and ret[0]

    def drop_model(self, name: str):
        if self.model_exists(name):
            self.idadb.ida_query('call nza..DROP_MODEL(\'model={}\')'.format(name))


