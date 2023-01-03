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

def map_to_props(data: dict) -> str:
    ret = []
    for k, v in data.items():
        if v is not None:
            ret.append("{}={}".format(k, v))
    return ",".join(ret)
