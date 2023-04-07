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
from nzpyida.analytics.utils import map_to_props, call_proc_df_in_out
from typing import List

class ModelManager:
    """
    Manages in-database models.
    """

    def __init__(self, idadb: IdaDataBase):
        """
        Creates the context.

        Parameters
        ----------
        idadb : IdaDataBase
            the database connector
        """

        self.idadb = idadb

    def list_models(self) -> IdaDataFrame:
        """
        Retrieve existing models (in the current database) and returns result as a data frame.

        Returns
        -------
        IdaDataFrame
            the data frame with the list of models
        """

        return IdaDataFrame(idadb=self.idadb, tablename='INZA.V_NZA_MODELS')

    def model_exists(self, name: str) -> bool:
        """
        Checks if a model with the given name exists.

        Parameters
        ----------
        name : str
            the name of model

        Returns
        -------
        bool
            True if the model with the given name exists, otherwise False
        """

        ret = self.idadb.ida_query(f'call NZA..MODEL_EXISTS(\'model={name}\')')
        return not ret.empty and ret[0]

    def drop_model(self, name: str):
        """
        Drops the model with the given name. Does noting if there is no such madel in the database.

        Parameters
        ----------
        name : str
            the name of model
        """

        if self.model_exists(name):
            self.idadb.ida_query(f'call NZA..DROP_MODEL(\'model={name}\')')
    
    def copy_model(self, name: str, copy_name: str):
        """
        Duplicates the given analytics model to a new model with the given new name. 
        The source model can be in another database.

        Parameters
        ----------
        name : str
            the model to be duplicated. It can be qualified by a database name (<database>..<model>)
            if not in the current database.
        
        copy_name : str
            The name of the copy of the model to be created in the current database
        """
        
        self.idadb.ida_query(f'call NZA..COPY_MODEL(\'model={name},copy={copy_name}\')')
    

    def alter_model(self, model: str, name: str=None, owner: str=None, description: str=None, 
                    copyright: str=None, app_name: str=None, app_version: str=None, category: str=None):
        """
        Alters the properties of the given model

        Parameters
        ----------
        model : str
            the model to be altered
        name : str, optional
            the new name of the model. If not specified, the model is not renamed. 
            Otherwise, the model name and the names of the managed tables are changed
        owner : str, optional
            the new owner of the model. If not specified, the owner of the model does not change. 
            Otherwise, the new owner will get all model privileges.
        description : str, optional
            the description for the model. If not specified, the description of the model does not change.
            Otherwise, the given description will be associated to the model
        copyright : str, optional
            the copyright statement for the model, used when exporting the model to PMML. 
            If not specified, the copyright statement of the model does not change. 
            Otherwise, the given copyright statement will be associated to the model.
        app_name : str, optional
            the application that created the model, used when exporting the model to PMML. 
            If not specified, the application that created the model does not change. 
            Otherwise, the given application name will be associated to the model
        app_version : str, optional
            the version of the application that created the model, used when exporting the model to PMML. 
            If not specified, the application version of the model does not change. 
            Otherwise, the given application version will be associated to the model
        category : str, optional
            the user-defined category for the model. If not specified, the category of the model does not change. 
            Otherwise, the given category will be associated to the mode
        """
        params = {
            'model': model,
            'name': name,
            'owner': owner,
            'description': description,
            'copyright': copyright,
            'appname': app_name,
            'appversion': app_version,
            'category': category
        }
        params_s = map_to_props(params)
        if self.model_exists(model):
            self.idadb.ida_query(f'call NZA..ALTER_MODEL(\'{params_s}\')')
        
    def grant_model(self, model: str, privilege: str, user: List[str]=None, 
                        group: List[str]=None, grant_option: bool=False):
        """
        Grants one or more privileges on an analytics model to users and/or groups

        Parameters
        ----------
        model : str
            the model to be granted privileges
        privelige : List[str]
            list of privileges. Allowed privileges are: list, select, alter, update, drop
        user : List[str], optional
            list of users to grant privileges to. If not specified, no user is granted 
            the privileges, and the parameter group has to be specified
        group : List[str], optional
            list of groups to grant privileges to. If not specified, no group is granted 
            the privileges, and the parameter user has to be specified
        grant_option : bool, optional
            flag indicating if the users or groups can further grant the given privileges 
            to other users or groups. Default value - False
        """
        params = {
            'model': model,
            'privilege': privilege,
            'user': user,
            'group': group,
            'grantoption': grant_option
        }
        params_s = map_to_props(params)
        if self.model_exists(model):
            self.idadb.ida_query(f'call NZA..GRANT_MODEL(\'{params_s}\')')

    def revoke_model(self, model: str, privilege: List[str], user: List[str]=None, group: List[str]=None):
        """
        Revokes one or more privileges on an analytics model from users and/or groups

        Parameters
        ----------
        model : str
            the model to revoke privileges
        privelige : List[str]
            list of privileges. Allowed privileges are: list, select, alter, update, drop
        user : List[str], optional
            list of users to revoke privileges to. If not specified, no user privileges
            are revoked, and the parameter group has to be specified
        group : List[str], optional
            list of groups to revoke privileges to. If not specified, no group privileges
            are revoked, and the parameter user has to be specified
        """
        params = {
            'model': model,
            'privilege': privilege,
            'user': user,
            'group': group
        }
        params_s = map_to_props(params)
        if self.model_exists(model):
            self.idadb.ida_query(f'call NZA..REVOKE_MODEL(\'{params_s}\')')

    def list_privileges(self, user: str=None, grant: bool=False):
        """
        Lists the effective privileges of a selected or all users on all models 
        for which you have the LIST privilege

        Parameters
        ----------
        user : str, optional
            the user whose privileges are listed. If not specified, the privileges of all users are 
            listed
        grant : bool, optional
            if true, the grant permissions are displayed. If false (default), 
            the regular object permissions are displayed
        """
        params = {
            'user': user,
            'grant': grant
        }
        cursor = self.idadb._con.cursor()
        params_s = map_to_props(params)
        cursor.execute(f'call NZA..LIST_PRIVILEGES(\'{params_s}\')')
        return ' ' + cursor.notices[0]
        # return self.idadb.ida_query(f'call NZA..LIST_PRIVILEGES(\'{params_s}\')')
