"""
This script processes experimental data from the FACET-II Injector and organizes it into a standardized format using the DataPoint2 class. 
It loads scalar and image data from files, structures inputs and outputs, and saves each shot's data to HDF5 files. 
Additionally, it generates a summary table of selected parameters and exports it as a YAML file.

Main Steps:
1. Load scalar and image data from pickle and numpy files.
2. Define lists of columns for scalar inputs and outputs.
3. For each shot in the dataset:
    - Create a DataPoint2 object.
    - Populate scalar inputs with values, locations, units, and descriptions.
    - Add input distribution (VCC image) and pixel calibration attribute.
    - Attach lattice information and run metadata.
    - Group scalar outputs by suffix and add them to the data point.
    - Add image output from the screen camera with calibration.
    - Add summary information for selected keys.
    - Save the data point to an HDF5 file.
    - Append summary information to a summary table.
4. Write the summary table to a YAML file.

Parameters:
- fileloc (str): Directory containing the data files.
- cols (list): List of column names for scalar inputs.
- scalar_output_cols (list): List of column names for scalar outputs.
- summary_keys (list): Keys to include in the summary output.
- lattice_location (str): URL to the lattice definition.
- metadata (dict): Metadata for run information.

Outputs:
- HDF5 files for each shot in './Test_Data2/'.
- YAML summary table in './Test_Data2/summary_table.yaml'.

Dependencies:
- numpy
- pandas
- os
- yaml
- Data_Standard_2.DataPoint2
"""

import numpy as np
from Data_Standard_2 import DataPoint2   

fileloc = './Ex_Experimental_Data2/'  # Location of data files
import pandas as pd
import os
import yaml

# Load some real data into memory
all_data = pd.read_pickle(fileloc + "total_data_stack_" + '571' + '.pkl')

VCC_all = np.load(fileloc + 'VCC_stack_571.npy')

all_images = np.load(fileloc + 'total_images_stack_571.npy')

# os.makedirs('./Test_Data/', exist_ok=True)

# List of columns for scalar inputs (magnet and RF settings, etc.)
cols = {'SOLN:IN10:121:BACT': 'kGm',
 'SOLN:IN10:111:BACT': 'kG',
 'QUAD:IN10:121:BACT': 'kG',
 'QUAD:IN10:122:BACT': 'kG',
 'QUAD:IN10:361:BACT': 'kG',
 'QUAD:IN10:371:BACT': 'kG',
 'QUAD:IN10:425:BACT': 'kG',
 'QUAD:IN10:441:BACT': 'kG',
 'QUAD:IN10:511:BACT': 'kG',
 'QUAD:IN10:525:BACT': 'kG',
 'SOLN:IN10:121:BCTRL': 'kGm',
 'SOLN:IN10:111:BCTRL': 'kGm',
 'QUAD:IN10:121:BCTRL': 'kG',
 'QUAD:IN10:122:BCTRL': 'kG',
 'QUAD:IN10:361:BCTRL': 'kG',
 'QUAD:IN10:371:BCTRL': 'kG',
 'QUAD:IN10:425:BCTRL': 'kG',
 'QUAD:IN10:441:BCTRL': 'kG',
 'QUAD:IN10:511:BCTRL': 'kG',
 'QUAD:IN10:525:BCTRL': 'kG',
 'KLYS:LI10:21:PDES': 'unitless',
 'KLYS:LI10:21:ADES': 'unitless',
 'KLYS:LI10:21:AMPL': 'unitless',
 'KLYS:LI10:21:PHAS': 'unitless',
 'KLYS:LI10:21:SFB_PDIS': 'unitless',
 'KLYS:LI10:31:PDES': 'unitless',
 'KLYS:LI10:31:ADES': 'unitless',
 'KLYS:LI10:31:AMPL': 'unitless',
 'KLYS:LI10:31:PHAS': 'unitless',
 'KLYS:LI10:41:PDES': 'unitless',
 'KLYS:LI10:41:ADES': 'unitless',
 'KLYS:LI10:41:AMPL': 'unitless',
 'KLYS:LI10:41:PHAS': 'unitless',
 'KLYS:LI10:51:PHAS': 'unitless',
 'KLYS:LI10:51:AMPL': 'unitless',
 'LASR:LT10:930:PWR': 'MW',
 'PMTR:HT10:950:PWR': 'MW',
 'IOC:SYS1:MP01:LSHUTCTL': 'unitless',
 'KLYS:LI10:51:PDES': 'unitless',
 'KLYS:LI10:51:AMPL': 'unitless',
 'TCAV:IN20:490:TC0_C_1_TCTL': 'unitless',
 'KLYS:LI20:51:BEAMCODE1_TCTL': 'unitless'}



# List of columns for scalar outputs (beam position, charge, camera, etc.)
scalar_output_cols = {'BPMS:IN10:221:X': 'mm',
 'BPMS:IN10:371:X': 'mm',
 'BPMS:IN10:425:X': 'mm',
 'BPMS:IN10:511:X': 'mm',
 'BPMS:IN10:525:X': 'mm',
 'BPMS:IN10:581:X': 'mm',
 'BPMS:IN10:631:X': 'mm',
 'BPMS:IN10:651:X': 'mm',
 'BPMS:IN10:731:X': 'mm',
 'BPMS:IN10:771:X': 'mm',
 'BPMS:IN10:781:X': 'mm',
 'BPMS:IN10:221:Y': 'mm',
 'BPMS:IN10:371:Y': 'mm',
 'BPMS:IN10:425:Y': 'mm',
 'BPMS:IN10:511:Y': 'mm',
 'BPMS:IN10:525:Y': 'mm',
 'BPMS:IN10:581:Y': 'mm',
 'BPMS:IN10:631:Y': 'mm',
 'BPMS:IN10:651:Y': 'mm',
 'BPMS:IN10:731:Y': 'mm',
 'BPMS:IN10:771:Y': 'mm',
 'BPMS:IN10:781:Y': 'mm',
 'BPMS:IN10:221:TMIT': 'nC',
 'BPMS:IN10:371:TMIT': 'nC',
 'BPMS:IN10:425:TMIT': 'nC',
 'BPMS:IN10:511:TMIT': 'nC',
 'BPMS:IN10:525:TMIT': 'nC',
 'BPMS:IN10:581:TMIT': 'nC',
 'BPMS:IN10:631:TMIT': 'nC',
 'BPMS:IN10:651:TMIT': 'nC',
 'BPMS:IN10:731:TMIT': 'nC',
 'BPMS:IN10:771:TMIT': 'nC',
 'BPMS:IN10:781:TMIT': 'nC',
 'TORO:IN10:591:TMIT_PC': 'pC',
 'TORO:IN10:791:TMIT_PC': 'pC',
  'CAMR:LT10:900:XRMS': 'mm',
 'CAMR:LT10:900:YRMS': 'mm',
 'CAMR:LT10:900:X': 'mm',
 'CAMR:LT10:900:Y': 'mm',
 'PROF:IN10:571:XRMS': 'mm',
 'PROF:IN10:571:YRMS': 'mm',
 'PROF:IN10:571:X': 'mm',
 'PROF:IN10:571:Y': 'mm'}

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
for i in range(len(all_data)):
    VCC = VCC_all[i,:,:]  # VCC image for shot i
    D = DataPoint2()      # Create new data point object
    scalar_inputs = {}
    # Populate scalar inputs for this shot
    for col in cols.keys():
        scalar_inputs[col] = {
            "value": float(all_data[col].iloc[i]) if pd.notnull(all_data[col].iloc[i]) else np.nan,
            "location": col,
            "units": cols[col],
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
    for col in scalar_output_cols.keys():
        parts = col.split(':')
        suffix = parts[-1]
        prefix = ':'.join(parts[:3])
        # print(prefix,suffix)
        if suffix not in unique_suffix_dict:
            unique_suffix_dict[suffix] = []
        unique_suffix_dict[suffix].append(prefix)

    # Add grouped scalar outputs to data point
    for unique_suffix, prefixes in unique_suffix_dict.items():
        # Group by unique units for this suffix
        units_set = set([scalar_output_cols.get(prefix + ':' + unique_suffix, '') for prefix in prefixes])
        for unit in units_set:
            # Filter prefixes with this unit
            unit_prefixes = [prefix for prefix in prefixes if scalar_output_cols.get(prefix + ':' + unique_suffix, '') == unit]
            data = np.array([
            float(all_data.get(prefix + ':' + unique_suffix, np.nan).iloc[i])
            if (prefix + ':' + unique_suffix) in all_data.columns and pd.notnull(all_data[prefix + ':' + unique_suffix].iloc[i])
            else np.nan
            for prefix in unit_prefixes
            ], dtype=float)
            # print(unit_prefixes)
            D.add_output(location=unit_prefixes, datum=data, attrs={},units=unit, datum_name=unique_suffix, datum_type='scalar',location_primary=True)

    # Add image output (profile camera)
    D.add_output(location='PROF:IN10:571', datum=all_images[i,:,:], attrs={'pixel_calibration':all_data['PROF:IN10:571:RESOLUTION'].iloc[i]}, datum_name='PROF:IN10:571:Image',datum_type='image',location_primary=True)

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