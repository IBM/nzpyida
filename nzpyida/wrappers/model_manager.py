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
from nzpyida.base import IdaDataBase
from nzpyida.frame import IdaDataFrame

class ModelManager:
    """
    Manages in-database models.
    """

    def __init__(self, idadb: IdaDataBase):
        """
        Creates the context.

        Parameters:
        -----------
        idadb : IdaDataBase
            the database connector
        """

        self.idadb = idadb

    def list_models(self) -> IdaDataFrame:
        """
        Retrieve existing models (in the current database) and returns result as a data frame.

        Returns:
        --------
        IdaDataFrame
            the data frame with the list of models
        """

        return IdaDataFrame(idadb=self.idadb, tablename='INZA.V_NZA_MODELS')

    def model_exists(self, name: str) -> bool:
        """
        Checks if a model with the given name exists.

        Parameters:
        -----------
        name : str
            the name of model

        Returns:
        --------
        bool
            True if the model with the given name exists, otherwise False
        """

        ret = self.idadb.ida_query(f'call NZA..MODEL_EXISTS(\'model={name}\')')
        return not ret.empty and ret[0]

    def drop_model(self, name: str):
        """
        Drops the model with the given name. Does noting if there is no such madel in the database.

        Parameters:
        -----------
        name : str
            the name of model
        """

        if self.model_exists(name):
            self.idadb.ida_query(f'call NZA..DROP_MODEL(\'model={name}\')')
