import logging
import re

from iotfunctions import ui
from iotfunctions.base import (BaseTransformer,
                               BaseSimpleAggregator,
                               BaseComplexAggregator)
from iotfunctions.ui import (UISingleItem,
                             UIMulti,
                             UIFunctionOutMulti,
                             UIMultiItem,
                             UIExpression)

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install
PACKAGE_URL = 'git+https://github.com/singhshraddha/custom-functions@development'


class SS_HelloWorld(BaseTransformer):
    """
    The docstring of the function will show as the function description in the UI.
    """

    def __init__(self, input_item, output_item_append, cardinalityForm):
        # a function is expected to have at least one parameter that acts
        # a function is expected to have at lease one parameter that describes
        # the output data items produced by the function
        # always create an instance variable with the same name as your arguments

        self.input_item = input_item
        self.output_item_append = output_item_append
        super().__init__()

    def execute(self, df):
        # the execute() method accepts a dataframe as input and returns a dataframe as output
        # the output dataframe is expected to produce at least one new output column

        output_columns = ['A', 'B', 'C']
        for i,o in enumerate(output_columns):
            df[o + self.output_item_append] = i


        logger.debug(f'df: {df}')

        return df

    @classmethod
    def build_ui(cls):
        # Your function will UI built automatically for configuring it
        # This method describes the contents of the dialog that will be built
        # Account for each argument - specifying it as a ui object in the "inputs" or "outputs" list

        inputs = [(ui.UISingleItem(name='input_item', datatype=None,
                                  description='Data items that have conditional values, e.g. temp and pressure'))]
        outputs = [ui.UIFunctionOutMulti(name='_min', cardinality_from='', datatype=str,
                                          description='Output item produced by function')]
        return inputs, outputs


class SS_HelloWorldAggregator(BaseSimpleAggregator):
    '''
    The docstring of the function will show as the function description in the UI.
    '''

    def __init__(self, source=None, expression=None):
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.source = source
        self.expression = expression

    def execute(self, group):
        return eval(re.sub(r"\$\{GROUP\}", r"group", self.expression))

    @classmethod
    def build_ui(cls):
        inputs = []
        # Input variable name must be kept 'source'
        # Output variable name must be kept 'name'
        inputs.append(UIMultiItem(name='source', datatype=None, description=('Choose the data items'
                                                                             ' that you would like to'
                                                                             ' aggregate'),
                                  output_item='name', is_output_datatype_derived=True))

        inputs.append(UIExpression(name='expression', description='Use ${GROUP} to reference the current grain.'
                                                                  'All Pandas Series methods can be used on the grain.'
                                                                  'For example, ${GROUP}.max() - ${GROUP}.min().'))
        return (inputs, [])


class SS_SimpleAggregator(BaseSimpleAggregator):
    '''
    Create aggregation using expression. The calculation is evaluated for
    each data_item selected. The data item will be made available as a
    Pandas Series. Refer to the Pandas series using the local variable named
    "x". The expression must return a scalar value.
    Example:
    x.max() - x.min()
    '''

    def __init__(self, source=None, expression=None):
        super().__init__()

        self.input_items = source
        self.expression = expression

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UIMultiItem(name='source', datatype=None, description=('Choose the data items'
                                                                             ' that you would like to'
                                                                             ' aggregate'),
                                  output_item='name', is_output_datatype_derived=True))

        inputs.append(UIExpression(name='expression', description='Paste in or type an AS expression'))

        return (inputs, [])

    def _calc(self, df):
        """
        If the function should be executed separately for each entity, describe the function logic in the _calc method
        """
        return eval(re.sub(r"x", r"df", self.expression))


class SS_ComplexAggregator(BaseComplexAggregator):
    '''
    Create aggregation using expression. The calculation is evaluated for
    each data_item selected. The data item will be made available as a
    Pandas Series. Refer to the Pandas series using the local variable named
    "x". The expression must return a scalar value.
    Example:
    x.max() - x.min()
    '''

    def __init__(self, source=None, quality_checks=None):
        super().__init__()

        self.input_items = source
        self.quality_checks = quality_checks

    @classmethod
    def build_ui(cls):
        inputs = [UISingleItem(name='source', datatype=None,
                               description='Choose data item to run data quality checks on'),
                  UIMulti(name='quality_checks', datatype=str, description='Choose quality checks to run',
                          output_item='name', values=['check_1', 'check_2', 'check_3'],
                          is_output_datatype_derived=True, output_datatype=float)]

        return inputs, []

    def execute(self, group):
        """
        Called on df.groupby 
        """
        return group.mean()

