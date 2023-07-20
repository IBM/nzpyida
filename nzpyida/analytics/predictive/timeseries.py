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
"""

Many types of business-relevant or scientific data have values that change over time. 
Some typical examples are:
- Daily sales figures for a store
- Energy consumption readings from household electric meters 
- Price per gallon at a local gas station
It is often useful to analyze the behavior of such changes, both to describe the development 
over time, specifically for a particular trend and seasonality, as well as to predict unknown 
values of the series, usually for the future. A typical area of application is supply chain
management, where future needs may be predicted based on past trends.

A time series is a sequence of numerical data values, measured at successive, but not necessarily 
equidistant—points in time. Examples are daily stock prices, monthly unemployment counts, 
or annual changes in global temperature. The two main goals of time series analysis are to 
understand the underlying patterns which are represented by the observed data and to make forecasts.
"""
from typing import List
from nzpyida.frame import IdaDataFrame
from nzpyida.base import IdaDataBase
from nzpyida.analytics.predictive.predictive_modeling import PredictiveModeling
from nzpyida.analytics.utils import call_proc_df_in_out
from nzpyida.analytics.model_manager import ModelManager
from nzpyida.analytics.utils import q as q0


class TimeSeries(PredictiveModeling):
    """
    Time Series Model
    """
    def __init__(self, idadb: IdaDataBase, model_name: str):
        """
        Creates Time Series
        """
        super().__init__(idadb, model_name)
        self.fit_proc = "TIMESERIES"
        self.has_print_proc = True
    
    def fit_predict(self, in_df: IdaDataFrame, time_column: str, target_column: str, by_column: str=None,
            out_table: str=None, description_table: str=None, algorithm: str='ExponentialSmoothing', 
            interpolation_method: str='linear', from_time=None, to_time=None, forecast_horizon: str=None,
            forecast_times: str=None, trend: str=None, seasonality: str=None, period: float=None, 
            unit: str=None, p: int=None, d: int=None, q: int=None, sp: int=None, sd: int=None, sq: int=None, 
            saesonally_adjusted_table: str=None ) -> IdaDataFrame:
        """
        Predicts future values of series of timed numeric values

        Parameters
        ----------
        in_df : IdaDataFrame
            the input data frame
        
        time_column : str
            the input data frame column which define an order on the numeric values
        
        target_columns : str
            the input data frame column which contains the numeric values

        by_column : str
            the input data frame column which uniquely identifies a serie of values.
            If not specified, all numeric values belong to only one time series.
        
        out_table : str
            the output data frmae containing predicted future values. This parameter 
            is not allowed for algorithm = SpectralAnalysis. If not specified, 
            no output table is written out
        
        description_table : str
            the optional input data frame containing the name and descriptions of the 
            time series. The table must contain following columns: <by_column>, 'NAME'=str, 
            'DESCRIPTION'=str. If not specified, the series do not have a name or a description
        
        algorithm : str
            the time series algorithm to use. Allowed values are: ExponentialSmoothing, 
            ARIMA, SeasonalTrendDecomposition, SpectralAnalysis

        interpolation_method : str
            the interpolation method. Allowed values are: linear, cubicspline, exponentialspline

        from_time : same as type of <time column>
            the value of column time to start the analysis from. If not specified, the analysis 
            starts from the first value of the time series in the input table

        to_time : same as type of <time column>
            the value of column time to stop the analysis at. If not specified, the analysis 
            stops at the last value of the time series in the input table

        forecast_horizon : str
            the value of column time until which to predict. This parameter is not allowed for 
            algorithm=SpectralAnalysis. If not specified, the algorithm determines itself 
            until which time it predicts values

        forecast_times : str
            list of semi-column separated values of column time to predict at. This parameter 
            is not allowed for algorithm=SpectralAnalysis. If not specified, the times to predict 
            values at is determined by the algorithm

        trend : str
            the trend type for algorithm=ExponentialSmoothing. Allowed values are: N (none), 
            A (addditive), DA (damped additive), M (multiplicative), DM (damped multiplicative). 
            If not specified, the trend type is determined by the algorithm

        seasonality : str
            the seasonality type for algorithm=ExponentialSmoothing. Allowed values are: N (none), 
            A (addditive), M (multiplicative). If not specified, the seasonality type is 
            determined by the algorithm

        period : float
            the seasonality period. This parameter is not allowed for algorithm=SpectralAnalysis. 
            If not specified, the seasonality period is determined by the algorithm. If set to 0, 
            no seasonality period will be considered by the algorithm
        
        unit : str
            the seasonality period unit. This parameter is not allowed for algorithm=SpectralAnalysis. 
            This parameter must be specified if the parameter period is specified and the <time_column>  
            is of type date, time or timestamp. Otherwise, it must not be spe- cified. Allowed values are: 
            ms, s, min, h, d, wk, qtr, q, a, y

        p : int
            the parameter p for algorithm=ARIMA, either equal to or below specified value. 
            If not specified, the algorithm will determine its best value automatically

        d : int
            the parameter d for algorithm=ARIMA, either equal to or below specified value. 
            If not specified, the algorithm will determine its best value automatically
        
        q : int
            the parameter q for algorithm=ARIMA, either equal to or below specified value. 
            If not specified, the algorithm will determine its best value automatically

        sp : int
            the seasonal parameter SP for algorithm=ARIMA, either equal to or below specified value. 
            If not specified, the algorithm will determine its best value automatically

        sd : int
            the seasonal parameter SD for algorithm=ARIMA, either equal to or below specified value. 
            If not specified, the algorithm will determine its best value automatically

        sq : int
            the seasonal parameter SQ for algorithm=ARIMA, either equal to or below specified value. 
            If not specified, the algorithm will determine its best value automatically

        saesonally_adjusted_table : str
            the output table containing seasonally adjusted values. This parameter is not allowed 
            for algorithm=SpectralAnalysis or algorithm=ARIMA. If not specified, no output table 
            is written out
        """

        params = {
            'model': self.model_name,
            'time': q0(time_column),
            'target': q0(target_column),
            'by': q0(by_column),
            'desctable': description_table,
            'algorithm': algorithm,
            'interpolationmethod': interpolation_method,
            'from': from_time,
            'to': to_time,
            'forecasthorizon': forecast_horizon,
            'forecasttimes': forecast_times,
            'trend': trend,
            'seasonality': seasonality,
            'period': period,
            'unit': unit,
            'p': p,
            'd': d,
            'q': q,
            'SP': sp,
            'SD': sd,
            'SQ': sq,
            'seasadjtable': saesonally_adjusted_table,
        }

        if not isinstance(in_df, IdaDataFrame):
            raise TypeError("Argument in_df should be an IdaDataFrame")

        ModelManager(self.idadb).drop_model(self.model_name)

        return call_proc_df_in_out(proc=self.fit_proc, in_df=in_df, params=params,
            out_table=out_table)[0]
