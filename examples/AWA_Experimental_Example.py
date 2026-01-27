"""Process AWA (Argonne Wakefield Accelerator) experimental data into standardized format.

This script processes HDF5 files containing AWA experimental measurements and converts them
into a standardized DataPoint2 format. The AWA data includes:
- Scalar measurements (RF phases, drive laser power, charge monitors)
- 1D waveforms (ICT measurements from beam current monitors)
- 2D images (screen images from cameras)

Main Steps:
1. Load data from HDF5 files in the input directory
2. Classify data by type (scalar, waveform, or image)
3. Create a DataPoint2 object for each file
4. Add observables with appropriate dimensions:
   - Scalars: feature_dims=0 (single values)
   - Waveforms: feature_dims=1 (time series)
   - Images: feature_dims=2 (2D arrays)
5. Add lattice information and run metadata
6. Save each processed file to HDF5 format

Command-line Arguments:
    --input_dir: Directory containing AWA .h5 files [default: 'Ex_AWA_Data']
    --output_dir: Directory to save processed files [default: './AWA_Example/']
    --lattice_dir: Directory containing lattice files [default: 'Lattice_Files']

Usage:
    python Argonne_Example.py --input_dir <path> --output_dir <path>

Dependencies:
    - numpy, pandas, h5py, yaml, argparse
    - Data_Standard_2.DataPoint2

Output:
    - Standardized HDF5 files for each input file in the output directory
"""

import numpy as np
from data_standard.Data_Standard_2 import DataPoint2
import pandas as pd
import os
import yaml
import h5py
import argparse

# ========================================
# Configuration
# ========================================
# Number of batch dimensions (0 = no batching, single shot per file)
batch_dim = 0

# Dictionary to hold loaded data from HDF5 file
data_dict = {}

def parse_args():
    """
    Parse command-line arguments for the AWA experimental data processing script.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments containing:
            - input_dir: Directory with AWA experimental archive files (.h5)
            - output_dir: Directory to save processed HDF5 files and summary
            - lattice_dir: Directory containing lattice files (rfdata*, yaml, etc.)
    """
    parser = argparse.ArgumentParser(description='Process AWA experimental data into standardized format.')
    parser.add_argument('--input_dir', type=str, default='./examples/data/input/AWA_Experimental_Data/',
                        help='Directory containing AWA experimental data files')
    parser.add_argument('--output_dir', type=str, default='./examples/data/output/AWA_Experimental_Example/',
                        help='Directory to save processed HDF5 files and summary')
    parser.add_argument('--lattice_dir', type=str, default='./examples/data/input/AWA_Experimental_Data/Lattice_Files/',
                        help='Directory containing lattice files (rfdata*, yaml, etc.)')
    return parser.parse_args()

# ========================================
# Units and control flags for each parameter
# ========================================
# Dictionary mapping parameter names to their physical units
# These are the measurements recorded by the AWA control system
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

# Dictionary mapping parameter names to control flags
# True = input/control parameter (what we set)
# False = output/measured parameter (what we observe)
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

# ========================================
# Experiment configuration
# ========================================
# Pixel calibration for camera images (meters per pixel)
pxcal = 1e-6  # meters per pixel

# Lattice location (URL or path to lattice definition)
lattice_location = 'unknown'

# Location identifier for the screen camera
screen_location = 'Final Screen'

# Run metadata
metadata = {'source': 'Argonne Wakefield Accelerator',
            'date': '2023-10-01',
            'notes': 'Example data point from AWA experiment'}

# Parameters to include in summary output for quick querying
summary_keys = ['total_intensity']

# ========================================
# Helper functions
# ========================================
def strip_last_colon(s):
    """
    Remove the last colon-separated segment from a parameter name.
    
    This is used to extract the device location from a full parameter name.
    Example: 'AWA:Drive:DS1:RB' -> 'AWA:Drive:DS1'
    
    Args:
        s (str): Input string with colon separators.
    
    Returns:
        str: String with last segment removed, or original if no colon present.
    """
    if ':' in s:
        return s.rsplit(':', 1)[0]
    return s

# ========================================
# Main processing function
# ========================================
def main():
    """
    Main execution function for processing AWA experimental data.
    
    This function:
    1. Parses command-line arguments
    2. Discovers all .h5 files in the input directory
    3. For each file:
       - Loads all datasets from the HDF5 file
       - Creates a DataPoint2 object
       - Classifies and adds observables by type (scalar, waveform, or image)
       - Adds lattice information and metadata
       - Saves to standardized HDF5 format
    
    Command-line Arguments:
        --input_dir: Directory containing AWA .h5 files
        --output_dir: Directory for output HDF5 files
        --lattice_dir: Directory containing lattice files
    """
    # Parse command-line arguments
    args = parse_args()
    
    # Find all HDF5 files in the input directory
    files = os.listdir(args.input_dir)
    print(f"Processing AWA data files from {args.input_dir}...")
    
    # Process each HDF5 file
    for file in files:
        if file.endswith('.h5'):
            print(f"\n  Processing file: {file}")
            
            # Load all datasets from the HDF5 file
            with h5py.File(os.path.join(args.input_dir, file), 'r') as f:
                for key in f.keys():
                    data_dict[key] = np.array(f[key])
            
            # Create new data point object for this file
            D = DataPoint2()
            
            # Add observables based on data type
            # Classify each dataset and add with appropriate dimensions
            for key in data_dict.keys():
                if key != 'images' and 'wf' not in key and 'ICT:x' not in key:
                    # Scalar measurements (0D) - single values per shot
                    # Examples: RF phases, power levels, charge readings
                    D.add_observable(
                        batch_dims=batch_dim, 
                        feature_dims=0,  # Scalar (0D)
                        location=[strip_last_colon(key)], 
                        data=data_dict[key], 
                        data_names=key, 
                        units=units[key],
                        location_primary=True,
                        control=control_keys[key]
                    )
                elif key == 'images':
                    # 2D images from screen cameras
                    # Includes pixel calibration in attributes
                    D.add_observable(
                        batch_dims=batch_dim, 
                        feature_dims=2,  # 2D image
                        location=[screen_location], 
                        data=data_dict[key], 
                        data_names=key, 
                        units=units[key],
                        location_primary=True,
                        control=control_keys[key],
                        attrs={'pxcal': pxcal}
                    )
                elif 'wf' in key or 'ICT:x' in key:
                    # 1D waveforms from diagnostics
                    # Examples: ICT traces, time-resolved current measurements
                    D.add_observable(
                        batch_dims=batch_dim, 
                        feature_dims=1,  # 1D waveform
                        location=[strip_last_colon(key)], 
                        data=data_dict[key], 
                        data_names=key, 
                        units=units[key],
                        location_primary=True,
                        control=control_keys[key]
                    )
            D.add_lattice(lattice_location=lattice_location)    

            D.add_run_information(source=metadata['source'], date=metadata['date'], notes=metadata['notes'])
            D.finalize()
            D.add_summary(summary_keys, summary_location='final')

            os.makedirs(args.output_dir, exist_ok=True)
            # Save data point to HDF5
            D.saveHDF5(args.output_dir)

    print(f"\nProcessing complete. Processed {len([f for f in files if f.endswith('.h5')])} files.")

if __name__ == '__main__':
    main()