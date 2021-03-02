#  Licensed Materials - Property of IBM 
#  5737-M66, 5900-AAA, 5900-A0N, 5725-S86, 5737-I75
#  (C) Copyright IBM Corp. 2020 All Rights Reserved.
#  US Government Users Restricted Rights - Use, duplication, or disclosure
#  restricted by GSA ADP Schedule Contract with IBM Corp.

from custom.functions import SS_ComplexAggregator
import pandas as pd

fn = SS_ComplexAggregator(source='input', quality_checks=['check_1', 'check_2'], name=['output_1', 'output_2'])

data = {
    'id' : [0, 0, 0, 0, 0, 1, 1, 1, 1, 1],
    'input': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
}


df = pd.DataFrame(data)
groups = df.groupby(['id'])

out = groups.apply(fn.execute)