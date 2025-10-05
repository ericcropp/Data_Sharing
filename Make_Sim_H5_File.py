import numpy as np
from Data_Standard import DataPoint, SimulatedDataPoint

fileloc = '/sdf/data/ad/ard-online/Measurements/FACET-II/Injector/Processed_Data/From_NERSC/2024-01-27/'
import pandas as pd
import os
import yaml

# Load some real data into memory
all_data = pd.read_pickle(fileloc + "total_data_stack_" + '571' + '.pkl')

VCC_all = np.load(fileloc + 'VCC_stack_571.npy')

all_images = np.load(fileloc + 'total_images_stack_571.npy')

os.makedirs('./Test_Data/', exist_ok=True)

cols = ['SOLN:IN10:121:BACT',
 'SOLN:IN10:111:BACT',
 'QUAD:IN10:121:BACT',
 'QUAD:IN10:122:BACT',
 'QUAD:IN10:361:BACT',
 'QUAD:IN10:371:BACT',
 'QUAD:IN10:425:BACT',
 'QUAD:IN10:441:BACT',
 'QUAD:IN10:511:BACT',
 'QUAD:IN10:525:BACT',
 'SOLN:IN10:121:BCTRL',
 'SOLN:IN10:111:BCTRL',
 'QUAD:IN10:121:BCTRL',
 'QUAD:IN10:122:BCTRL',
 'QUAD:IN10:361:BCTRL',
 'QUAD:IN10:371:BCTRL',
 'QUAD:IN10:425:BCTRL',
 'QUAD:IN10:441:BCTRL',
 'QUAD:IN10:511:BCTRL',
 'QUAD:IN10:525:BCTRL',
 'KLYS:LI10:21:PDES',
 'KLYS:LI10:21:ADES',
 'KLYS:LI10:21:AMPL',
 'KLYS:LI10:21:PHAS',
 'KLYS:LI10:21:SFB_PDIS',
 'KLYS:LI10:31:PDES',
 'KLYS:LI10:31:ADES',
 'KLYS:LI10:31:AMPL',
 'KLYS:LI10:31:PHAS',
 'KLYS:LI10:41:PDES',
 'KLYS:LI10:41:ADES',
 'KLYS:LI10:41:AMPL',
 'KLYS:LI10:41:PHAS',
 'KLYS:LI10:51:PHAS',
 'KLYS:LI10:51:AMPL',
 'LASR:LT10:930:PWR',
 'PMTR:HT10:950:PWR',
 'IOC:SYS1:MP01:LSHUTCTL',
 'KLYS:LI10:51:PDES',
 'KLYS:LI10:51:AMPL',
 'TCAV:IN20:490:TC0_C_1_TCTL',
 'KLYS:LI20:51:BEAMCODE1_TCTL']



scalar_output_cols = ['BPMS:IN10:221:X',
 'BPMS:IN10:371:X',
 'BPMS:IN10:425:X',
 'BPMS:IN10:511:X',
 'BPMS:IN10:525:X',
 'BPMS:IN10:581:X',
 'BPMS:IN10:631:X',
 'BPMS:IN10:651:X',
 'BPMS:IN10:731:X',
 'BPMS:IN10:771:X',
 'BPMS:IN10:781:X',
 'BPMS:IN10:221:Y',
 'BPMS:IN10:371:Y',
 'BPMS:IN10:425:Y',
 'BPMS:IN10:511:Y',
 'BPMS:IN10:525:Y',
 'BPMS:IN10:581:Y',
 'BPMS:IN10:631:Y',
 'BPMS:IN10:651:Y',
 'BPMS:IN10:731:Y',
 'BPMS:IN10:771:Y',
 'BPMS:IN10:781:Y',
 'BPMS:IN10:221:TMIT',
 'BPMS:IN10:371:TMIT',
 'BPMS:IN10:425:TMIT',
 'BPMS:IN10:511:TMIT',
 'BPMS:IN10:525:TMIT',
 'BPMS:IN10:581:TMIT',
 'BPMS:IN10:631:TMIT',
 'BPMS:IN10:651:TMIT',
 'BPMS:IN10:731:TMIT',
 'BPMS:IN10:771:TMIT',
 'BPMS:IN10:781:TMIT',
 'TORO:IN10:431:1:TMIT_P',
 'TORO:IN10:591:1:TMIT_P',
 'TORO:IN10:591:TMIT_PC',
 'TORO:IN10:791:1:TMIT_P',
 'TORO:IN10:791:TMIT_PC',
 'TORO:IN10:791:0:TMIT_PC',
  'CAMR:LT10:900:XRMS',
 'CAMR:LT10:900:YRMS',
 'CAMR:LT10:900:X',
 'CAMR:LT10:900:Y',
 'PROF:IN10:571:XRMS',
 'PROF:IN10:571:YRMS',
 'PROF:IN10:571:X',
 'PROF:IN10:571:Y']

summary_keys = [ 'PROF:IN10:571:XRMS',
'PROF:IN10:571:YRMS']
summary_keys += cols

# for i in range(10):
# # Try to format it as a DataPoint
#     scalar_inputs = all_data[cols].iloc[i]
#     lattice_location = 'https://github.com/slaclab/facet2-lattice'
    

#     VCC = VCC_all[i,:,:]

#     metadata = {'source':'FACET-II Injector','date':'2024-01-27','notes':'Processed data from NERSC'}
#     # Save as H5 file




#     D = DataPoint(scalar_inputs=scalar_inputs, input_distribution=VCC, input_distribution_attrs={'pixel_calibration':all_data['CAMR:LT10:900:RESOLUTION'].iloc[0]}, lattice_location=lattice_location, summary_keys=summary_keys, run_information=metadata)


#     for col in scalar_output_cols:
#         D.add_data(location=col, datum=all_data[col].iloc[i], attrs={}, datum_name=col, datum_type='scalar')

#     # D.add_data(location='PROF:IN10:571', datum=all_data['PROF:IN10:571:XRMS'].iloc[0],attrs={},datum_name='PROF:IN10:571:XRMS', datum_type='scalar')
#     D.add_data(location='PROF:IN10:571', datum=all_images[i,:,:], attrs={'pixel_calibration':all_data['PROF:IN10:571:RESOLUTION'].iloc[i]}, datum_name='PROF:IN10:571:Image',datum_type='image')
    
#     D.saveHDF5('./Test_Data/')

summary_table = []
for i in range(10):
    scalar_inputs = all_data[cols].iloc[i]
    lattice_location = 'https://github.com/slaclab/facet2-lattice'
    VCC = VCC_all[i,:,:]
    metadata = {'source':'FACET-II Injector','date':'2024-01-27','notes':'Processed data from NERSC'}
    D = DataPoint(
        scalar_inputs=scalar_inputs,
        input_distribution=VCC,
        input_distribution_attrs={'pixel_calibration':all_data['CAMR:LT10:900:RESOLUTION'].iloc[0]},
        lattice_location=lattice_location,
        summary_keys=summary_keys,
        run_information=metadata
    )
    for col in scalar_output_cols:
        D.add_data(location=col, datum=all_data[col].iloc[i], attrs={}, datum_name=col, datum_type='scalar')
    D.add_data(location='PROF:IN10:571', datum=all_images[i,:,:], attrs={'pixel_calibration':all_data['PROF:IN10:571:RESOLUTION'].iloc[i]}, datum_name='PROF:IN10:571:Image',datum_type='image')
    D.saveHDF5('./Test_Data/')
    entry = {
        **D.summary,
        **D.run_information
    }
    summary_table.append(entry)

with open('./Test_Data/summary_table.yaml', 'w') as f:
    yaml.dump(summary_table, f)