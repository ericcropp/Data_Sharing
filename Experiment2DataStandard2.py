import numpy as np
from Data_Standard_2 import DataPoint2   

fileloc = '/sdf/data/ad/ard-online/Measurements/FACET-II/Injector/Processed_Data/From_NERSC/2024-01-27/'
import pandas as pd
import os
import yaml

# Load some real data into memory
all_data = pd.read_pickle(fileloc + "total_data_stack_" + '571' + '.pkl')

VCC_all = np.load(fileloc + 'VCC_stack_571.npy')

all_images = np.load(fileloc + 'total_images_stack_571.npy')

# os.makedirs('./Test_Data/', exist_ok=True)

# List of columns for scalar inputs (magnet and RF settings, etc.)
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



# List of columns for scalar outputs (beam position, charge, camera, etc.)
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

# List of keys to include in summary output
summary_keys = [ 'PROF:IN10:571:XRMS',
'PROF:IN10:571:YRMS']
summary_keys += cols

# Lattice location (URL to lattice definition)
lattice_location = 'https://github.com/slaclab/facet2-lattice'
# Metadata for run information
metadata = {'source':'FACET-II Injector','date':'2024-01-27','notes':'Processed data from NERSC'}

summary_table = []
# Loop over first 10 shots in the dataset
for i in range(10):
    VCC = VCC_all[i,:,:]  # VCC image for shot i
    D = DataPoint2()      # Create new data point object
    scalar_inputs = {}
    # Populate scalar inputs for this shot
    for col in cols:
        scalar_inputs[col] = {
            "value": float(all_data[col].iloc[i]) if pd.notnull(all_data[col].iloc[i]) else np.nan,
            "location": col,
            "units": "",  # Fill in units if available
            "description": ""  # Fill in description if available
        }
    D.add_inputs(scalar_inputs=scalar_inputs)
    # Add input distribution (camera image) and calibration
    D.add_inputs(input_distribution=VCC, input_distribution_attrs={'pixel_calibration':all_data['CAMR:LT10:900:RESOLUTION'].iloc[i]})
    # Add lattice info
    D.add_lattice(lattice_location=lattice_location)
    # Add run metadata
    D.add_run_information(source=metadata['source'], date=metadata['date'], notes=metadata['notes'])

    # Group scalar outputs by suffix (e.g., X, Y, TMIT)
    unique_suffix_dict = {}
    for col in scalar_output_cols:
        parts = col.split(':')
        suffix = parts[-1]
        prefix = ':'.join(parts[:-1])
        if suffix not in unique_suffix_dict:
            unique_suffix_dict[suffix] = []
        unique_suffix_dict[suffix].append(prefix)

    # Add grouped scalar outputs to data point
    for unique_suffix, prefixes in unique_suffix_dict.items():
        data = np.array([
            float(all_data.get(prefix + ':' + unique_suffix, np.nan).iloc[i])
            if (prefix + ':' + unique_suffix) in all_data.columns and pd.notnull(all_data[prefix + ':' + unique_suffix].iloc[i])
            else np.nan
            for prefix in prefixes
        ], dtype=float)
        D.add_output(location=prefixes, datum=data, attrs={}, datum_name=unique_suffix, datum_type='scalar')

    # Add image output (profile camera)
    D.add_output(location='PROF:IN10:571', datum=all_images[i,:,:], attrs={'pixel_calibration':all_data['PROF:IN10:571:RESOLUTION'].iloc[i]}, datum_name='PROF:IN10:571:Image',datum_type='image')

    # Add summary info for this shot
    D.add_summary(summary_keys, summary_location='final')

    # Ensure output directory exists
    os.makedirs('./Test_Data2/', exist_ok=True)
    # Save data point to HDF5
    D.saveHDF5('./Test_Data2/')

    # Add summary entry for this shot
    entry = {
        **D.summary.summary
    }
    summary_table.append(entry)
# Write summary table to YAML file
with open('./Test_Data2/summary_table.yaml', 'w') as f:
    yaml.dump(summary_table, f)