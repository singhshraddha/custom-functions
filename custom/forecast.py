#  | * IBM Confidential
#  | * OCO Source Materials
#  | * 5737-M66
#  | * © Copyright IBM Corp. 2020
#  | * The source code for this program is not published or otherwise divested of its
#  | * trade secrets, irrespective of what has been deposited with the U.S.
#  | * Copyright Office.

import logging

import numpy as np

#modeling
import pyrenn as prn

from iotfunctions import ui
from iotfunctions.base import (BaseTransformer)

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install
PACKAGE_URL = 'git+https://github.com/singhshraddha/custom-functions@development'
_IS_PREINSTALLED = False


class Cognio_NeuralNetwork_Forecaster(BaseTransformer):
    """
    Provides a forecast for 'target' variable using 'features'
    Assumes a trained model exists in the db.
    Uses the same model for all entities
    More information about training and saving the model: TODO add link to notebook

    :param features list should be the same as features used to train the model
    :param target str name of the dependent variable
    """

    def __init__(self, features, target):
        super().__init__()

        self.features = features
        self.target = target

        self.whoami = 'Cognio_NeuralNetwork_Forecaster'

    def check_feature_vector(self, model_feature_vector=None):
        """
        Checks if input feature vector is the same as pre-trained model's feature vector
        :param model_feature_vector
        """
        if not set(model_feature_vector) == set(self.features):
            logger.debug(f'Pre-trained model features {model_feature_vector} are different from input features '
                         f'{self.features}')
            return False
        return True

    def one_hot_encode_time_feature(self, df, one_hot_encoder, time_period):
        """
        Performs one hot encoding on user specified 'time_period' (weekday/hour) using a pre-trained onehotencoder
        :param df pd.Datarame has timestamp we use to one hot encode a time_period
        :param one_hot_encoder sklearn.preprocessing.OneHotEncoder
        :param time_period str 'hour' or 'month'

        returns df with one hot encoded time feature
        """
        # get timestamp values from timestamp index
        # in the pipeline index levels will always be = ['entity_id', 'timestamp']
        logger.debug(locals())
        indexes = df.index.names
        timestamp_index = indexes[1]
        dfe = df.reset_index()

        # add column we have to one hot encode
        if time_period == 'hour':
            dfe.loc[:, time_period] = dfe[timestamp_index].dt.hour
        elif time_period == 'weekday':
            dfe.loc[:, time_period] = dfe[timestamp_index].dt.weekday
        else:
            logger.debug(f'time_period {time_period} will not be one hot encoded')
            return df

        # one hot encode and append to the dataframe
        categories = [f'{time_period}_{n}' for n in range(len(one_hot_encoder.categories_[0]))]
        encoded_values = one_hot_encoder.transform(dfe[time_period].values.reshape(-1, 1)).toarray()
        dfe.loc[:, tuple(categories)] = encoded_values
        self.features.extend(categories)

        # remove the time period column
        dfe.drop(columns=[time_period])

        return dfe.set_index(indexes)

    def get_model_for_predict(self):
        """
        Get the pre-trained model "Cognio_NeuralNetwork_Forecaster"
        """
        db = self._entity_type.db
        model_name = "shraddha_cognio_nn_lm_test"  #TODO
        model = db.model_store.retrieve_model(model_name)
        if model is not None:
            msg = 'Retrieved existing model from ModelStore'
        else:
            msg = 'Unable to retrieve model from ModelStore'

        logger.debug(msg)
        return model

    def execute(self, df):

        df_copy = df.copy()

        # get model for predict
        predict_model = self.get_model_for_predict()
        if predict_model is None:  # no pre-tained model
            df_copy[self.target] = 0
            return df_copy

        # form input features from input feature vectors
        df_copy = self.one_hot_encode_time_feature(df_copy, predict_model['OneHotEncoderHour'], 'hour')
        df_copy = self.one_hot_encode_time_feature(df_copy, predict_model['OneHotEncoderWeekday'], 'weekday')

        # input feature vector must be the same as pre-trained models feature vector
        if not self.check_feature_vector(predict_model['FeatureVector']):
            df_copy[self.target] = 0
            return df_copy

        logger.debug('Successfully performed set-up prior to prediction')

        entities = np.unique(df_copy.index.levels[0])
        logger.debug(str(entities))

        for entity in entities:
            # while we can perform predictions on all entities at once
            # under the assumption that there is one model for all entities
            # we use this architecture in case we decide to have a model per entity

            #convert features to numpy
            try:
                dfe = df_copy.loc[entity]
                predicted_values = prn.NNOut(dfe[self.features].T.to_numpy(), predict_model)
                df_copy.loc[entity, self.target] = predicted_values
            except Exception as e:
                df_copy.loc[entity, self.target] = 0
                logger.error(f'{self.whoami} prediction failed with ' + str(e))

        return df_copy

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs
        inputs = [ui.UIMultiItem(name='features', datatype=float, description='Predictive features')]

        # define arguments that behave as function outputs
        outputs = [ui.UIFunctionOutSingle(name='target', datatype=float, description='Predicted output')]
        return inputs, outputs
