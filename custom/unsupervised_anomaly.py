# *****************************************************************************
# © Copyright IBM Corp. 2018-2020.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
import logging

import numpy as np
import pandas as pd
import datetime as dt

#modeling
import stumpy

from iotfunctions import ui, util
from iotfunctions.base import (BaseTransformer)

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install
PACKAGE_URL = 'git+https://github.com/singhshraddha/custom-functions@development'


def min_delta(df):
    # minimal time delta for merging

    if len(df.index.names) > 1:
        df2 = df.copy()
        df2.index = df2.index.droplevel(list(range(1, df.index.nlevels)))
    else:
        df2 = df

    try:
        mindelta = df2.index.to_series().diff().min()
    except Exception as e:
        logger.debug('Min Delta error: ' + str(e))
        mindelta = pd.Timedelta('5 seconds')

    if mindelta == dt.timedelta(seconds=0) or pd.isnull(mindelta):
        mindelta = pd.Timedelta('5 seconds')

    return mindelta, df2


def merge_score(dfEntity, dfEntityOrig, column_name, score, mindelta):
    """
    Fit interpolated score to original entity slice of the full dataframe
    """

    # equip score with time values, make sure it's positive
    score[score < 0] = 0
    dfEntity[column_name] = score

    # merge
    dfEntityOrig = pd.merge_asof(dfEntityOrig, dfEntity[column_name], left_index=True, right_index=True,
                                 direction='nearest', tolerance=mindelta)

    if column_name + '_y' in dfEntityOrig:
        merged_score = dfEntityOrig[column_name + '_y'].to_numpy()
    else:
        merged_score = dfEntityOrig[column_name].to_numpy()

    return merged_score


class MatrixProfileAnomalyScoreTest(BaseTransformer):
    """
    TODO: add description
    """
    NOT_ENOUGH_DATAPOINTS = 1e-15

    def __init__(self, input_item, output_item, window_size=12):
        super().__init__()
        logger.debug(f'Input item: {input_item}')
        self.input_item = input_item
        # use 12 by default
        self.window_size = window_size
        self.output_item = output_item
        self.whoami = 'MatrixProfile'

    def execute(self, df):
        df_copy = df.copy()
        entities = np.unique(df_copy.index.levels[0])
        logger.debug(f'Entities: {str(entities)}')
        df_copy[self.output_item] = 0

        # check data type
        if df_copy[self.input_item].dtype != np.float64:
            return df_copy

        for entity in entities:
            # per entity - copy for later inplace operations
            dfe = df_copy.loc[[entity]].dropna(how='all')
            dfe_orig = df_copy.loc[[entity]].copy()

            # get rid of entityid part of the index
            # do it inplace as we copied the data before
            dfe.reset_index(level=[0], inplace=True)
            dfe.sort_index(inplace=True)
            dfe_orig.reset_index(level=[0], inplace=True)
            dfe_orig.sort_index(inplace=True)

            # minimal time delta for merging
            mindelta, dfe_orig = min_delta(dfe_orig)
            util.log_data_frame(dfe.head())

            matrix_profile = stumpy.aamp(dfe[self.input_item], m=self.window_size)[:, 0]
            # fill in small value for newer data points with < window_size num data points following them
            fillers = np.array([self.NOT_ENOUGH_DATAPOINTS] * (self.window_size - 1))
            matrix_profile = np.append(matrix_profile, fillers)

            anomaly_score = merge_score(dfe, dfe_orig, self.output_item, matrix_profile, mindelta)

            idx = pd.IndexSlice
            df_copy.loc[idx[entity, :], self.output_item] = anomaly_score

        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = [ui.UISingleItem(name="input_item", datatype=float, description="Time series data item to analyze", ),
                  ui.UISingle(name="window_size", datatype=int,
                              description="Size of each sliding window in data points. Typically set to 12.")]

        # define arguments that behave as function outputs
        outputs = [ui.UIFunctionOutSingle(name="output_item", datatype=float,
                                          description="Anomaly score (MatrixProfileAnomalyScore)", )]
        return inputs, outputs
