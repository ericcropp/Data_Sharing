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
7. Optionally combine all processed files into a single HDF5 file

Command-line Arguments:
    --input_dir: Directory containing AWA .h5 files
                 [default: './examples/data/input/AWA_Experimental_Data/']
    --output_dir: Directory to save processed files
                  [default: './examples/data/output/AWA_Experimental_Example/']
    --lattice_dir: Directory containing lattice files
                   [default: './examples/data/input/AWA_Experimental_Data/Lattice_Files/']
    --Combine_Files: Combine all processed files into single HDF5 file after processing
                     [default: 'True']

Usage:
    # Process with default paths and combine files
    python AWA_Experimental_Example.py
    
    # Process with custom paths without combining
    python AWA_Experimental_Example.py \
        --input_dir /path/to/data \
        --output_dir /path/to/output \
        --Combine_Files False

Dependencies:
    - numpy, pandas, h5py, yaml, argparse, shutil
    - data_standard.DataPoint2
    - data_standard.combine_files

Output:
    - Individual standardized HDF5 files for each input file (if Combine_Files=False)
    - summary_table.yaml containing summary statistics from all files
    - Combined_Data.h5 single merged file (if Combine_Files=True)
"""

import numpy as np
from data_standard import DataPoint2
from data_standard import combine_files
import pandas as pd
import os
import yaml
import h5py
import argparse
import shutil 

# ========================================
# Configuration
# ========================================
# Batch dimensions - using 2 files means batch_dims=(2,)
batch_dims = None  # Will be set based on number of files loaded

# Dictionary to hold loaded data from HDF5 file
data_dict = {}

def parse_args():
    """
    Parse command-line arguments for the AWA experimental data processing script.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments containing:
            - input_dir (str): Directory with AWA experimental archive files (.h5)
                             Default: './examples/data/input/AWA_Experimental_Data/'
            - output_dir (str): Directory to save processed HDF5 files and summary
                              Default: './examples/data/output/AWA_Experimental_Example/'
            - lattice_dir (str): Directory containing lattice files (rfdata*, yaml, etc.)
                               Default: './examples/data/input/AWA_Experimental_Data/Lattice_Files/'
            - Combine_Files (str): Whether to combine all processed files ('True'/'False')
                                 Default: 'True'
    """
    parser = argparse.ArgumentParser(description='Process AWA experimental data into standardized format.')
    parser.add_argument('--input_dir', type=str, default='./examples/data/input/AWA_Experimental_Data/',
                        help='Directory containing AWA experimental data files')
    parser.add_argument('--output_dir', type=str, default='./examples/data/output/AWA_Experimental_Example/',
                        help='Directory to save processed HDF5 files and summary')
    parser.add_argument('--lattice_dir', type=str, default='./examples/data/input/AWA_Experimental_Data/Lattice_Files/',
                        help='Directory containing lattice files (rfdata*, yaml, etc.)')
    parser.add_argument('--Combine_Files',type=str,default='True',help='Combine all processed files into a single HDF5 file after processing')
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
    1. Parses command-line arguments for input/output directories
    2. Discovers all .h5 files in the input directory
    3. For each file:
       - Loads all datasets from the HDF5 file
       - Creates a DataPoint2 object
       - Classifies and adds observables by type (scalar, waveform, or image)
       - Adds lattice information and metadata
       - Saves to standardized HDF5 format
    4. Creates a summary_table.yaml with statistics from all processed files
    5. Optionally combines all processed files into a single HDF5 file:
       - Uses combine_files() to merge individual files
       - Moves combined file to output directory
       - Removes individual files to save space
    
    Command-line Arguments:
        --input_dir: Directory containing AWA .h5 files
                    Default: './examples/data/input/AWA_Experimental_Data/'
        --output_dir: Directory for output HDF5 files
                     Default: './examples/data/output/AWA_Experimental_Example/'
        --lattice_dir: Directory containing lattice files
                      Default: './examples/data/input/AWA_Experimental_Data/Lattice_Files/'
        --Combine_Files: Combine processed files into single file ('True'/'False')
                        Default: 'True'
    
    Output Files:
        - Individual HDF5 files: <output_dir>/<ID>.h5 (if Combine_Files=False)
        - summary_table.yaml: Summary statistics from all files
        - Combined_Data.h5: Single merged file (if Combine_Files=True)
    """
    # Parse command-line arguments
    args = parse_args()
    
    # Find all HDF5 files in the input directory
    files = os.listdir(args.input_dir)
    print(f"Processing AWA data files from {args.input_dir}...")
    
    # Process each HDF5 file - collect all data first
    summary_table = []
    h5_files = [f for f in files if f.endswith('.h5')]
    print(f"Found {len(h5_files)} HDF5 files to process")
    
    # Load all data at once - more efficient than per-key loops
    combined_data = {}
    for file in h5_files:
        print(f"\n  Loading file: {file}")
        with h5py.File(os.path.join(args.input_dir, file), 'r') as f:
            for key in f.keys():
                if key not in combined_data:
                    combined_data[key] = []
                combined_data[key].append(np.array(f[key]))
    
    print(f"\nConverting to arrays and creating DataPoint2...")
    # Convert lists to arrays once per key
    for key in combined_data.keys():
        combined_data[key] = np.array(combined_data[key])
    
    # Determine batch_dims from the data shape
    # AWA data has shape (num_files, num_shots_per_file, ...)
    # For scalars, this will be (num_files, num_shots_per_file)
    sample_key = list(combined_data.keys())[0]
    sample_shape = combined_data[sample_key].shape
    
    # For scalars: shape is (n_files, n_shots, ...)
    # For images: shape is (n_files, n_shots, height, width)
    # For waveforms: shape is (n_files, n_shots, n_points)
    # batch_dims should capture the (n_files, n_shots) dimensions
    
    # Determine batch_dims from first scalar (non-image, non-waveform) data
    for key in combined_data.keys():
        if key != 'images' and 'wf' not in key and 'ICT:x' not in key:
            # This is scalar data - shape should be (n_files, n_shots)
            batch_dims = tuple(combined_data[key].shape)
            break
    
    print(f"Using batch_dims={batch_dims} for data shape {sample_shape}")
            
    # Create new data point object
    D = DataPoint2()
    
    # Add observables based on data type
    # Classify each dataset and add with appropriate dimensions
    for key in combined_data.keys():
        if key != 'images' and 'wf' not in key and 'ICT:x' not in key:
            # Scalar measurements (0D) - single values per shot
            # Examples: RF phases, power levels, charge readings
            D.add_observable(
                batch_dims=batch_dims, 
                num_feature_dims=0,  # Scalar (0D)
                location=[strip_last_colon(key)], 
                data=combined_data[key], 
                data_name=key, 
                units=units[key],
                location_primary=True,
                control=control_keys[key]
            )
        elif key == 'images':
            # 2D images from screen cameras
            # Includes bin size and offset in attributes
            D.add_observable(
                batch_dims=batch_dims, 
                num_feature_dims=2,  # 2D image
                location=[screen_location], 
                data=combined_data[key], 
                data_name=key, 
                units=units[key],
                location_primary=True,
                control=control_keys[key],
                attrs={'bin_size': pxcal, 'offset': 0}
            )
        elif 'wf' in key or 'ICT:x' in key:
            # 1D waveforms from diagnostics
            # Examples: ICT traces, time-resolved current measurements
            D.add_observable(
                batch_dims=batch_dims, 
                num_feature_dims=1,  # 1D waveform
                location=[strip_last_colon(key)], 
                data=combined_data[key], 
                data_name=key, 
                units=units[key],
                location_primary=True,
                control=control_keys[key],
                attrs={'bin_size': 1, 'offset': 0}
            )
    D.add_lattice(lattice_location=lattice_location)    

    D.add_run_information(source=metadata['source'], date=metadata['date'], notes=metadata['notes'])
    D.finalize()

    os.makedirs(args.output_dir, exist_ok=True)
    # Save data point to HDF5
    D.saveHDF5(args.output_dir)
    
    # Convert numpy types to Python native types for YAML compatibility
    entry = {}
    for key, value in D.summary.summary.items():
        if isinstance(value, np.ndarray):
            entry[key] = value.tolist()
        elif isinstance(value, (np.integer, np.floating)):
            entry[key] = value.item()
        elif isinstance(value, list):
            entry[key] = [v.item() if isinstance(v, (np.integer, np.floating)) else v for v in value]
        else:
            entry[key] = value
    summary_table.append(entry)

    print(f"\nProcessing complete. Processed {len(h5_files)} files.")

    # Combine files into a single HDF5 file
    if args.Combine_Files.lower() == 'true':
        print("Combining processed files into a single HDF5 file...")
        
        # Get list of individual HDF5 files before combining
        individual_files = [f for f in os.listdir(args.output_dir) 
                          if f.endswith('.h5') and f != 'Combined_Data.h5']
        
        # Create combined file in output directory
        combined_path = os.path.join(args.output_dir, 'Combined_Data.h5')
        combine_files(args.output_dir, combined_path)
        
        # Delete individual files, keeping only Combined_Data.h5
        for filename in individual_files:
            file_path = os.path.join(args.output_dir, filename)
            try:
                os.remove(file_path)
            except OSError:
                pass
        
        print(f"Combined file created at {combined_path}")
        print(f"Removed {len(individual_files)} individual files")
    
if __name__ == '__main__':
    main()