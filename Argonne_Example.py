import numpy as np
from Data_Standard_2 import DataPoint2   
import pandas as pd
import os
import yaml
import h5py

batch_dim = 0
data_dict = {}
with h5py.File('DYG12_1759956735.h5', 'r') as f:
    for key in f.keys():
        data_dict[key] = np.array(f[key])

units = {'AWA:Bira3Ctrl:Ch09': 'unitless',
         'AWA:Drive:DS1:RB': 'unitless',
         'AWA:Drive:DS3:RB': 'unitless',
         'AWA:Drive:DS4:RB': 'unitless',
         'AWA:Drive:DS6:RB': 'unitless',
         'AWALLRF:K1:Phase': 'degrees', 
         'AWALLRF:K2:Phase': 'degrees', 
         'AWAVXI11ICT:Ch1': 'unitless', 
         'AWAVXI11ICT:Ch2': 'unitless', 
         'AWAVXI11ICT:Ch3': 'unitless', 
         'AWAVXI11ICT:Ch4': 'unitless', 
         'AWAVXI11ICT:wf1': 'unitless', 
         'AWAVXI11ICT:wf2': 'unitless', 
         'AWAVXI11ICT:wf3': 'unitless', 
         'AWAVXI11ICT:wf4': 'unitless', 
         'AWAVXI11ICT:x': 'unitless', 
         'Cx': 'unitless', 
         'Cy': 'unitless', 
         'Sx': 'm', 
         'Sy': 'm', 
         'bb_penalty': 'unitless', 
         'images': 'unitless', 
         'log10_total_intensity': 'unitless', 
         'total_intensity': 'unitless'}

control_keys = {'AWA:Bira3Ctrl:Ch09': True,
         'AWA:Drive:DS1:RB': False,
         'AWA:Drive:DS3:RB': False,
         'AWA:Drive:DS4:RB': False,
         'AWA:Drive:DS6:RB': False,
         'AWALLRF:K1:Phase': False, 
         'AWALLRF:K2:Phase': False, 
         'AWAVXI11ICT:Ch1': False, 
         'AWAVXI11ICT:Ch2': False, 
         'AWAVXI11ICT:Ch3': False, 
         'AWAVXI11ICT:Ch4': False, 
         'AWAVXI11ICT:wf1': False, 
         'AWAVXI11ICT:wf2': False, 
         'AWAVXI11ICT:wf3': False, 
         'AWAVXI11ICT:wf4': False, 
         'AWAVXI11ICT:x': False, 
         'Cx': False, 
         'Cy': False, 
         'Sx': False, 
         'Sy': False, 
         'bb_penalty': False, 
         'images': False, 
         'log10_total_intensity': False, 
         'total_intensity': False}

pxcal = 1e-6  # meters per pixel

lattice_location = 'unknown'

screen_location = 'Final Screen'

metadata = {'source': 'Argonne Wakefield Accelerator',
            'date': '2023-10-01',
            'notes': 'Example data point from AWA experiment'}


summary_keys = ['total_intensity']

def strip_last_colon(s):
    if ':' in s:
        return s.rsplit(':', 1)[0]
    return s

D = DataPoint2()

for key in data_dict.keys():
    # print(key)
    if key != 'images' and 'wf' not in key and 'ICT:x' not in key:
        # print('scalar')
        # print(np.shape(data_dict[key]))
        D.add_observable(batch_dims=batch_dim, feature_dims=0, location=[strip_last_colon(key)], data=data_dict[key], data_names=key, units=units[key],location_primary=True,control=control_keys[key])
    elif key == 'images':
        # print('image')
        # print(np.shape(data_dict[key]))
        D.add_observable(batch_dims=batch_dim, feature_dims=2, location=[screen_location], data=data_dict[key], data_names=key, units=units[key],location_primary=True,control=control_keys[key],attrs={'pxcal': pxcal})
    elif 'wf' in key or 'ICT:x' in key:
        # print('waveform')
        # print(np.shape(data_dict[key]))
        D.add_observable(batch_dims=batch_dim, feature_dims=1, location=[strip_last_colon(key)], data=data_dict[key], data_names=key, units=units[key],location_primary=True,control=control_keys[key])
D.add_lattice(lattice_location=lattice_location)    

D.add_run_information(source=metadata['source'], date=metadata['date'], notes=metadata['notes'])
D.finalize()
D.add_summary(summary_keys, summary_location='final')

# for item in D.observables:
#     print(item.data_names, np.shape(item.data))

os.makedirs('./AWA_Example/', exist_ok=True)
# Save data point to HDF5
D.saveHDF5('./AWA_Example/')