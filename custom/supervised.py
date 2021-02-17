#  <insert copyright>
import datetime as dt
import logging

import numpy as np
from sklearn import linear_model
from sklearn import metrics
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.utils import check_array


from iotfunctions.base import (BaseTransformer, BaseRegressor, BaseEstimatorFunction, BaseSimpleAggregator)
from iotfunctions.ui import (UISingle, UIMultiItem, UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti)

logger = logging.getLogger(__name__)
PACKAGE_URL = 'git+https://github.com/singhshraddha/custom-functions@development'


class BayesRidgeRegressor(BaseEstimatorFunction):
    """
    Linear regressor based on a probabilistic model as provided by sklearn
    """
    eval_metric = staticmethod(metrics.r2_score)

    # class variables
    train_if_no_model = True
    num_rounds_per_estimator = 3

    def BRidgePipeline(self):
        steps = [('scaler', StandardScaler()), ('bridge', linear_model.BayesianRidge(compute_score=True))]
        return Pipeline(steps)

    def set_estimators(self):
        params = {}
        self.estimators['bayesianridge'] = (self.BRidgePipeline, params)

        logger.info('Bayesian Ridge Regressor start searching for best model')

    def __init__(self, features, targets, predictions=None):
        super().__init__(features=features, targets=targets, predictions=predictions, stddev=True)

        self.experiments_per_execution = 1
        self.auto_train = True
        self.correlation_threshold = 0
        self.stop_auto_improve_at = -2

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df_copy.index.levels[0])
        logger.debug(str(entities))

        missing_cols = [x for x in self.predictions + self.pred_stddev if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        for entity in entities:
            try:
                check_array(df_copy.loc[[entity]][self.features].values)
                dfe = super()._execute(df_copy.loc[[entity]], entity)
                print(df_copy.columns)

                df_copy.loc[entity, self.predictions] = dfe[self.predictions]
                df_copy.loc[entity, self.pred_stddev] = dfe[self.pred_stddev]

                print(df_copy.columns)
            except Exception as e:
                logger.info('Bayesian Ridge regressor for entity ' + str(entity) + ' failed with: ' + str(e))
                df_copy.loc[entity, self.predictions] = 0
        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))

        # define arguments that behave as function outputs
        outputs = []
        return (inputs, outputs)