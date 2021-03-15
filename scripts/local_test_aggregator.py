#  Licensed Materials - Property of IBM 
#  5737-M66, 5900-AAA, 5900-A0N, 5725-S86, 5737-I75
#  (C) Copyright IBM Corp. 2020 All Rights Reserved.
#  US Government Users Restricted Rights - Use, duplication, or disclosure
#  restricted by GSA ADP Schedule Contract with IBM Corp.

from custom.data_quality import SS_DataQualityChecks
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

plot_print = False

QUALITY_CHECKS = ['constant_value',
                  'sample_entropy',
                  'stationarity',
                  'stuck_at_zero',
                  'white_noise'
                  ]
fn = SS_DataQualityChecks(source='input', quality_checks=QUALITY_CHECKS, name=QUALITY_CHECKS)

data_size_per_id = 100
time_axis = np.linspace(0, 20, data_size_per_id)
id_0_data = np.random.normal(0, 1, size=data_size_per_id) # white noise
id_1_data = np.sin(time_axis) # sin wave
id_2_data = np.ones(data_size_per_id) # constant value
id_3_data = np.zeros(data_size_per_id) # stuck at zero
id_4_data = np.add(id_0_data, id_1_data) # sin wave with white noise
id_5_data = [1]
data = {
    'id': np.concatenate(([6]*data_size_per_id, [1]*data_size_per_id, [2]*data_size_per_id,
                          [3]*data_size_per_id, [4]*data_size_per_id, [5])),
    'input': np.concatenate((id_0_data, id_1_data, id_2_data, id_3_data, id_4_data, id_5_data))
}


df = pd.DataFrame(data)
groups = df.groupby(['id'])

out = groups.apply(fn.execute)

print(pd.__version__)
if plot_print:
    plt.plot(time_axis, id_0_data, label="id6")
    plt.plot(time_axis, id_1_data, label="id1")
    plt.plot(time_axis, id_2_data, label="id2")
    plt.plot(time_axis, id_3_data, label="id3")
    plt.plot(time_axis, id_4_data, label="id4")
    plt.legend()
    plt.show(block=False)
else:
    print('---------------------------------------')
    print(out)
    print('---------------------------------------')

exit(0)