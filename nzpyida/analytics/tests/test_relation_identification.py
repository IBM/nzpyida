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

import pytest
import pandas as pd
import numpy as np
from nzpyida.base import IdaDataBase
from nzpyida.analytics.exploration.relation_identification import *
from nzpyida.analytics.tests.conftest import TAB_NAME_TRAIN, OUT_TABLE_PRED

TAB_NAME = "TEST_TAB_NAME"

@pytest.fixture(scope='module')
def idf(idadb):
    df_train = pd.DataFrame.from_dict(
    {
        "ID": range(1000),
        "A": range(1,1001),
        "B": range(1000, 0, -1),
        "C": ['p','n']*500,
        "D": [1, 0]*500,
        "E": ['a']*500 + ['b']*500,
        "F": map(lambda l: l**2, range(1000))
    }
    )
    idf = idadb.as_idadataframe(df_train, tablename=TAB_NAME, clear_existing=True)
    yield idf
    if idadb.exists_table(TAB_NAME):
        idadb.drop_table(TAB_NAME)
    
@pytest.fixture(scope='function')
def clean_up(idadb: IdaDataBase):
    if idadb.exists_table_or_view(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)
    yield
    if idadb.exists_table_or_view(OUT_TABLE_PRED):
        idadb.drop_table(OUT_TABLE_PRED)

class TestCorr:
    def test_strong_correlation(self, idadb, idf, clean_up):
        out_df = corr(idf, in_column=["A", "B"], by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('CORRELATION'), "C"])
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case("CORRELATION")].head().values < -0.99)

    def test_weak_correlation(self, idadb, idf, clean_up):
        out_df = corr(idf, in_column=["A", "D"], out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['CORRELATION']))
        assert len(out_df) == 1
        assert 0.01 > out_df.head()[0] > -0.01
    
    def test_no_correlation(self, idadb, idf, clean_up):
        out_df = corr(idf, in_column=["A", "D"], by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('CORRELATION'), "C"])
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case("CORRELATION")].head().values == [0, 0])

class TestCov:
    def test_strong_covariance(self, idadb, idf, clean_up):
        out_df = cov(idf, in_column=['ID', 'A'], out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['COVARIANCE']))
        assert len(out_df) == 1
        max_cov = np.cov(range(1, 1001),range(1, 1001))[0][0]
        assert all(0.99*max_cov < out_df[idadb.to_def_case("COVARIANCE")].head().values < 1.01*max_cov)
    
    def test_weak_covariance(self, idadb, idf, clean_up):
        out_df = cov(idf, in_column=['A', 'D'], out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['COVARIANCE']))
        assert len(out_df) == 1
        assert np.abs(out_df[idadb.to_def_case("COVARIANCE")].head().values) < 0.5


    def test_no_covariance(self, idadb, idf, clean_up):
        out_df = cov(idf, in_column=['A', 'D'], by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('COVARIANCE'), "C"])
        assert len(out_df) == 2
        assert all( out_df[idadb.to_def_case("COVARIANCE")].head().values == [0, 0])

class TestMutualInfo:
    def test_strong_corelation(self, idadb, idf, clean_up):
        out_df = mutual_info(idf, ['A', 'B'], by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('MUTUALINFO'), 'Over_C'])
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case("MUTUALINFO")].head().values < 10)
    
    @pytest.mark.skip # "Attribute ... must be GROUPed or used in an aggregate function" error
    def test_weak_correlation(self, idadb, idf, clean_up):
        out_df = mutual_info(idf, ['A', 'D'], out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('MUTUALINFO'), 'Over_C'])
        assert len(out_df) == 1

    @pytest.mark.skip # "Attribute ... must be GROUPed or used in an aggregate function" error
    def test_no_correlation(self, idadb, idf, clean_up):
        out_df = mutual_info(idf, ['A', 'D'], by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('MUTUALINFO'), 'Over_C'])
        assert len(out_df) == 2

# TODO: poorly readable output
class TestCovarianceMatrix:
    def test_strong_corelation(self, idadb, idf, clean_up):
        out_df = covariance_matrix(idf, ['A', 'B'], by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('COVARIANCE'), 'C'])
        assert len(out_df) == 2
    
    def test_weak_correlation(self, idadb, idf, clean_up):
        out_df = covariance_matrix(idf, ['A', 'D'], out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['COVARIANCE']))
        assert len(out_df) == 1

    def test_no_correlation(self, idadb, idf, clean_up):
        out_df = covariance_matrix(idf, ['A', 'D'], by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('COVARIANCE'), 'C'])
        assert len(out_df) == 2

class TestSpearmanCorr:
    def test_strong_corelation(self, idadb, idf, clean_up):
        out_df = spearman_corr(idf, ['A', 'B'], by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == ['C', idadb.to_def_case('RHO'), idadb.to_def_case('N')])
        assert len(out_df) == 2
        assert all( out_df[idadb.to_def_case("RHO")].head().values == -1)
    
    def test_weak_correlation(self, idadb, idf, clean_up):
        out_df = spearman_corr(idf, ['A', 'D'], out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['SPEARMAN_CORR']))
        assert len(out_df) == 1
        assert all(abs(out_df[idadb.to_def_case("SPEARMAN_CORR")].head().values) < 0.05)
        
    def test_no_correlation(self, idadb, idf, clean_up):
        out_df = spearman_corr(idf, ['A', 'D'], by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == ['C', idadb.to_def_case('RHO'), idadb.to_def_case('N')])
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case("RHO")].head().values == 0)

class TestChisq:
    # I believe last assertsion should be stronger (... > 0.95)
    def test_strong_correlation(self, idadb, idf, clean_up):
        out_df = chisq(idf, ['A', 'B'], out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['PERCENTAGE', 'CHI2STATISTIC', 'DF', 'OVER_NO_GROUP']))
        assert len(out_df) == 1
        assert out_df[idadb.to_def_case('PERCENTAGE')].head().values[0] > 0.75

    @pytest.mark.skip # "Attribute ... must be GROUPed or used in an aggregate function" error
    def test_weak_correlation(self, idadb, idf, clean_up):
        out_df = chisq(idf, ['A', 'D'], out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['PERCENTAGE', 'CHI2STATISTIC', 'DF', 'OVER_NO_GROUP']))
        assert len(out_df) == 1
        assert out_df[idadb.to_def_case('PERCENTAGE')].head().values[0] < 0.05
    
    @pytest.mark.skip # "Attribute ... must be GROUPed or used in an aggregate function" error
    def test_no_correlation(self, idadb, idf, clean_up):
        out_df = chisq(idf, ['A', 'D'], by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['PERCENTAGE', 'CHI2STATISTIC', 'DF', 'OVER_NO_GROUP']))
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case('PERCENTAGE')].head().values == 0)


class TestTMeTest:
    def test_linear_column_correct_value(self, idadb, idf, clean_up):
        out_df = t_me_test(idf, in_column="A", mean_value=500, out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['PERCENTAGE', 'T_ME_TEST']))
        assert len(out_df) == 1
        assert 0.05 < out_df[idadb.to_def_case('PERCENTAGE')].head().values[0] < 0.95
    
    def test_linear_column_incorrect_value(self, idadb, idf, clean_up):
        out_df = t_me_test(idf, in_column="A", mean_value=400, out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['PERCENTAGE', 'T_ME_TEST']))
        assert len(out_df) == 1
        assert out_df[idadb.to_def_case('PERCENTAGE')].head().values[0] > 0.95
    
    def test_linear_column_incorrect_value_with_by_column(self, idadb, idf, clean_up):
        out_df = t_me_test(idf, in_column="A", mean_value=550, by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['PERCENTAGE', 'T_ME_TEST']) + ["C"])
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case('PERCENTAGE')].head().values < 0.05)
    
    def test_constant_column(self, idadb, idf, clean_up):
        out_df = t_me_test(idf, in_column="D", mean_value=1, by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['PERCENTAGE', 'T_ME_TEST']) + ["C"])
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case('PERCENTAGE')].head().values == None)

class TestTUmdTest:
    def test_linear_column_correct_value(self, idadb, idf, clean_up):
        out_df = t_umd_test(idf, in_column="A", class_column='C:p:n', out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['PERCENTAGE', 'T_UMD_TEST']))
        assert len(out_df) == 1
        assert 0.45 < out_df[idadb.to_def_case('PERCENTAGE')].head().values[0] < 0.55
    
    def test_linear_column_incorrect_value(self, idadb, idf, clean_up):
        out_df = t_umd_test(idf, in_column="A", class_column='E:a:b', by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('PERCENTAGE'), idadb.to_def_case('T_UMD_TEST'), "C"])
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case('PERCENTAGE')].head().values < 0.05)
    
class TestTPmdTest:
    def test_good_diff(self, idadb, idf, clean_up):
        out_df = t_pmd_test(idf, in_column=["A:X", "B:Y"], expected_diff=0, out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['PERCENTAGE', 'TSTUDPAIREDMEANDIFF']))
        assert len(out_df) == 1
        assert 0.45 < out_df[idadb.to_def_case('PERCENTAGE')].head().values[0] < 0.55
    
    def test_slightly_wrong_diff(self, idadb, idf, clean_up):
        out_df = t_pmd_test(idf, in_column=["A:X", "B:Y"], expected_diff=1, out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['PERCENTAGE', 'TSTUDPAIREDMEANDIFF']))
        assert len(out_df) == 1
        assert 0.05 < out_df[idadb.to_def_case('PERCENTAGE')].head().values[0] < 0.95
    
    def test_wrong_diff(self, idadb, idf, clean_up):
        out_df = t_pmd_test(idf, in_column=["A:X", "B:Y"], by_column="E", expected_diff=10, out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('PERCENTAGE'), idadb.to_def_case('TSTUDPAIREDMEANDIFF'), "E"])
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case('PERCENTAGE')].head().values == [0, 1])


class TestTLsTest:
    @pytest.mark.skip # TODO: this function is failing, when slope is exactly right
    def test_good_slope(self, idadb, idf, clean_up):
        out_df = t_ls_test(idf, in_column=["A:X", "B:Y"], slope=-1, out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['PERCENTAGE', 'T_LS_TEST']))
        assert len(out_df) == 1
        assert 0.05 < out_df[idadb.to_def_case('PERCENTAGE')].head().values[0] < 0.95
    
    def test_wrong_slope(self, idadb, idf, clean_up):
        out_df = t_ls_test(idf, in_column=["A:X", "B:Y"], slope=1, by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('PERCENTAGE'), idadb.to_def_case('T_LS_TEST'), 'C'])
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case('PERCENTAGE')].head().values == 0)

    @pytest.mark.skip # TODO: this function is not working correctly, as slope close to correct value is treated as very bad one
    def test_slightly_wrong_slope(self, idadb, idf, clean_up):
        out_df = t_ls_test(idf, in_column=["A:X", "B:Y"], slope=-0.99999, out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['PERCENTAGE', 'T_LS_TEST']))
        assert len(out_df) == 1
        assert 0.05 < out_df[idadb.to_def_case('PERCENTAGE')].head().values[0] < 0.95


class TestMwwTest:
    def test_linear_column_similar_values_with_by_column(self, idadb, idf, clean_up):
        out_df = mww_test(idf, in_column="A", class_column="C", by_column="E", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == ['E'] + idadb.to_def_case(['N', 'N1', 'N2', 'USTAT0', 'U2STAT0', 'ALPHASUM', 'NORM1',
                                      'USTAT1', 'NORM2', 'U2STAT1', 'MU_U1', 'SIGMA_U', 'ZSTAT', 'PP',
                                      'USTAT', 'U2STAT', 'MU_U', 'NOGROUPS', 'MESSAGE', 'LOWER']))
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case('PP')].head().values > 0.05)
        assert all(out_df[idadb.to_def_case('LOWER')].head().values == 'p')
    
    def test_linear_column_similar_values_without_by_column(self, idadb, idf, clean_up):
        out_df = mww_test(idf, in_column="A", class_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == ['USTAT', 'U2STAT', 'MUU', 'SIGMAU', 'ZSTAT', 'PP', 'LOWER'])
        assert len(out_df) == 1
        assert out_df['PP'].head().values[0] > 0.05
        assert out_df['LOWER'].head().values[0] == 'p'
    
    def test_linear_column_different_values_with_by_column(self, idadb, idf, clean_up):
        out_df = mww_test(idf, in_column="A", class_column="E", by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == ['C'] + idadb.to_def_case(['N', 'N1', 'N2', 'USTAT0', 'U2STAT0', 'ALPHASUM', 'NORM1',
                                      'USTAT1', 'NORM2', 'U2STAT1', 'MU_U1', 'SIGMA_U', 'ZSTAT', 'PP',
                                      'USTAT', 'U2STAT', 'MU_U', 'NOGROUPS', 'MESSAGE', 'LOWER']))
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case('PP')].head().values < 0.05)
        assert all(out_df[idadb.to_def_case('LOWER')].head().values == 'a')


class TestWilcoxonTest:
    # function wrogly indicates column with lower values, need to change last assertions
    def test_similar_columns(self, idadb, idf, clean_up):
        out_df = wilcoxon_test(idf, in_column=["A", "B"], out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == ['SSTAT', 'WSTAT', 'ZSTAT', 'NOITEM', 'PP', 'LOWER'])
        assert len(out_df) == 1
        assert out_df['PP'].head().values[0] > 0.05
    
    # function wrogly indicates column with lower values, need to change last assertions
    def test_similar_columns_with_by_column(self, idadb, idf, clean_up):
        out_df = wilcoxon_test(idf, in_column=["A", "B"], by_column="E", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == ['E'] + idadb.to_def_case(['N', 'SSTAT', 'WSTAT', 'ZSTAT', 'PP', 'LOWER']))
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case('PP')].head().values < 0.05)
        assert out_df[out_df["E"]=='a'][idadb.to_def_case("LOWER")].head().values[0] == '"B"'
        assert out_df[out_df["E"]=='b'][idadb.to_def_case("LOWER")].head().values[0] == '"A"'
    
    # function wrogly indicates column with lower values, need to change last assertion
    def test_different_columns(self, idadb, idf, clean_up):
        out_df = wilcoxon_test(idf, in_column=["A", "D"], out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == ['SSTAT', 'WSTAT', 'ZSTAT', 'NOITEM', 'PP', 'LOWER'])
        assert len(out_df) == 1
        assert out_df['PP'].head().values[0] < 0.05
        assert out_df['LOWER'].head().values[0] == '"A"'

# TODO: poorly readable output
class TestCanonicalCorr:
    def test_strong_correlation(self, idadb, idf, clean_up):
        out_df = canonical_corr(idf, in_column=["A", "B"], out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['CANONICAL_CORRELATION']))
        assert len(out_df) == 1
    
    def test_weak_correlation(self, idadb, idf, clean_up):
        out_df = canonical_corr(idf, in_column=["A", "D"], out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['CANONICAL_CORRELATION']))
        assert len(out_df) == 1
    
    def test_no_correlation(self, idadb, idf, clean_up):
        out_df = canonical_corr(idf, in_column=["A", "D"], by_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('CANONICAL_CORRELATION'), "C"])
        assert len(out_df) == 2


class TestAnovaCrdTest:
    def test_simmilar_columns(self, idadb, idf, clean_up):
        out_df = anova_crd_test(idf, in_column=['D'], treatment_column="E", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['TOTNO', 'TOTSU', 'TOTMEAN', 'TOTSS', 'SSCTOT', 'SSCBETWEEN', 
                                      'DFBETWEEN', 'SSCWITHIN', 'DFWITHIN', 'F', 'P']))
        assert len(out_df) == 1
        assert out_df[idadb.to_def_case('P')].head().values[0] < 0.95 
    
    def test_dfferent_columns(self, idadb, idf, clean_up):
        out_df = anova_crd_test(idf, in_column=['A'], treatment_column="E", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['TOTNO', 'TOTSU', 'TOTMEAN', 'TOTSS', 'SSCTOT', 'SSCBETWEEN', 
                                      'DFBETWEEN', 'SSCWITHIN', 'DFWITHIN', 'F', 'P']))
        assert len(out_df) == 1
        assert out_df[idadb.to_def_case('P')].head().values[0] > 0.95
    
    def test_multiple_columns_with_by_column(self, idadb, idf, clean_up):
        out_df = anova_crd_test(idf, in_column=['A', 'B'], treatment_column="E", by_column='C', out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == [idadb.to_def_case('INCOLUMN'), 'C'] + idadb.to_def_case(['TOTNO', 'TOTSU', 'TOTMEAN', 'TOTSS', 'SSCTOT', 
                                      'SSCBETWEEN', 'DFBETWEEN', 'SSCWITHIN', 'DFWITHIN', 'F', 'P']))
        assert len(out_df) == 4


class TestAnovaRbdTest:
    def test_one_column(self, idadb, idf, clean_up):
        out_df = anova_rbd_test(idf, in_column=['A'], treatment_column="E", block_column="C", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['BLSSCBETWEEN', 'BLDFBETWEEN','SSCWITHIN', 'DFWITHIN', 'FBL', 'PBL', 
                                      'GRSSCBETWEEN', 'GRDFBETWEEN', 'FGR', 'PGR']))
        assert len(out_df) == 1
        assert out_df[idadb.to_def_case('PGR')].head().values[0] > 0.95  
        assert out_df[idadb.to_def_case('PBL')].head().values[0] < 0.95 
    
    def test_multiple_columns(self, idadb, idf, clean_up):
        out_df = anova_rbd_test(idf, in_column=['A', 'B'], treatment_column="C", block_column='E', out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['INCOLUMN', 'BLSSCBETWEEN', 'BLDFBETWEEN','SSCWITHIN', 'DFWITHIN', 
                                      'FBL', 'PBL', 'GRSSCBETWEEN', 'GRDFBETWEEN', 'FGR', 'PGR']))
        assert len(out_df) == 2
        assert all(out_df[idadb.to_def_case('PGR')].head().values < 0.95)
        assert all(out_df[idadb.to_def_case('PBL')].head().values > 0.95)

class TestManovaOneWayTest:
    def test_idependent_columns(self, idadb, idf, clean_up):
        out_df = manova_one_way_test(idf, in_column=["A","D"], factor1_column="E", id_column="ID", 
                                     table_type="columns", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['ID_TASK', 'ID_MATRIX', 'ROW', 'COL', 'VAL', 'EXPLANATION',
                                      'BONFERRONI_CORR', 'TASK_NAME', 'VARIABLE_NAME']))
        assert len(out_df) == 21
        assert len(set(out_df[idadb.to_def_case('ID_MATRIX')].as_dataframe().values)) == 4
    
    def test_correlated_columns(self, idadb, idf, clean_up):
        out_df = manova_one_way_test(idf, in_column=["A","F"], factor1_column="C", id_column="ID", 
                                     table_type="columns", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['ID_TASK', 'ID_MATRIX', 'ROW', 'COL', 'VAL', 'EXPLANATION',
                                      'BONFERRONI_CORR', 'TASK_NAME', 'VARIABLE_NAME']))
        assert len(out_df) == 21
        assert len(set(out_df[idadb.to_def_case('ID_MATRIX')].as_dataframe().values)) == 4

class TestManovaTwoWayTest:
    def test_correlated_columns(self, idadb, idf, clean_up):
        out_df = manova_two_way_test(idf, in_column=["A","F"], factor1_column="E", factor2_column="C", id_column="ID", 
                                     table_type="columns", out_table=OUT_TABLE_PRED)
        assert out_df
        assert all(out_df.columns == idadb.to_def_case(['ID_TASK', 'ID_MATRIX', 'ROW', 'COL', 'VAL', 'EXPLANATION',
                                      'BONFERRONI_CORR', 'TASK_NAME', 'VARIABLE_NAME']))
        assert len(out_df) == 59
        assert len(set(out_df[idadb.to_def_case('ID_MATRIX')].as_dataframe().values)) == 8