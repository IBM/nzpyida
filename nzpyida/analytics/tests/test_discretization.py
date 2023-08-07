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
from nzpyida.analytics.auto_delete_context import AutoDeleteContext
from nzpyida.analytics.transform.discretization import EFDisc, EMDisc, EWDisc


def test_ewdisc(idadb: IdaDataBase, idf_train):
    idf = idf_train[:10]['A']
    with AutoDeleteContext(idadb):
        ewdisc = EWDisc(idadb, bins=5)
        bin_df = ewdisc.fit(idf)
        assert bin_df
        assert len(bin_df) == 4
        assert all(bin_df.columns == idadb.to_def_case(['COLNAME', 'BREAK']))

        ew_df = ewdisc.apply(idf, in_bin_df=bin_df, keep_org_values=True)
        assert ew_df
        assert len(ew_df) == len(idf)
        assert all(ew_df.columns == ['A', idadb.to_def_case('DISC') + '_A'])
        assert set(ew_df[idadb.to_def_case('DISC') + '_A'].head(10).values) == {'1', '2', '3', '4', '5'}


def test_efdisc(idadb: IdaDataBase, idf_train):
    idf = idf_train[:10]['A']
    with AutoDeleteContext(idadb):
        efdisc = EFDisc(idadb, bins=2)
        bin_df = efdisc.fit(idf)
        assert bin_df
        assert len(bin_df) == 1
        assert all(bin_df.columns == idadb.to_def_case(['COLNAME', 'BREAK']))

        ef_df = efdisc.apply(idf, in_bin_df=bin_df, keep_org_values=True)
        assert ef_df
        assert len(ef_df) == len(idf)
        assert all(ef_df.columns == ['A', idadb.to_def_case('DISC') + '_A'])
        assert set(ef_df[idadb.to_def_case('DISC') + '_A'].head(10).values) == {'1', '2'}


def test_emdisc(idadb: IdaDataBase, idf_train):
    idf = idf_train[:10]['A']
    with AutoDeleteContext(idadb):
        emdisc = EMDisc(idadb, target='A')
        bin_df = emdisc.fit(idf)
        assert bin_df
        assert all(bin_df.columns == idadb.to_def_case(['COLNAME', 'BREAK']))

        em_df = emdisc.apply(idf, in_bin_df=bin_df, keep_org_values=True)
        assert em_df
        assert len(em_df) == len(idf)
        assert all(em_df.columns == ['A', idadb.to_def_case('DISC') + '_A'])
        assert len(set(em_df[idadb.to_def_case('DISC') + '_A'].head(10).values)) < len(idf)
