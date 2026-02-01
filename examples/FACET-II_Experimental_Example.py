"""Process FACET-II Injector experimental data into standardized format.

This script processes experimental data from the FACET-II Injector and organizes it into a standardized format
using the DataPoint2 class. It loads scalar and image data from files, structures inputs and outputs,
and saves each shot's data to HDF5 files. Additionally, it generates a summary table of selected parameters.

Main Steps:
1. Load scalar and image data from pickle and numpy files.
2. Define lists of columns for scalar inputs and outputs.
3. For each shot (or batch of shots) in the dataset:
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
5. Optionally combine all processed files into a single HDF5 file.

Command-line Arguments:
    --input_dir: Directory containing FACET-II experimental data files
                 [default: './examples/data/input/FACET-II_Experimental_Data/']
    --output_dir: Directory to save processed HDF5 files and summary
                  [default: './examples/data/output/FACET-II_Experimental_Example/']
    --lattice_dir: Directory containing lattice files (rfdata*, yaml, etc.)
                   [default: './examples/data/input/FACET-II_Experimental_Data/Lattice_Files/']
    --Combine_Files: Combine all processed files into single HDF5 file after processing
                     [default: 'True']

Usage:
    # Process with default paths and combine files
    python FACET-II_Experimental_Example.py
    
    # Process with custom paths without combining
    python FACET-II_Experimental_Example.py \
        --input_dir /path/to/data \
        --output_dir /path/to/output \
        --Combine_Files False

Dependencies:
    - numpy, pandas, os, yaml, argparse, shutil
    - data_standard.DataPoint2
    - data_standard.combine_files

Output:
    - Individual standardized HDF5 files for each shot/batch (if Combine_Files=False)
    - summary_table.yaml: Summary of key parameters from all processed shots
    - Combined_Data.h5: Single merged file containing all shots (if Combine_Files=True)
"""

import numpy as np
from data_standard import DataPoint2
from data_standard import combine_files
import pandas as pd
import os
import yaml
import argparse
import shutil

def parse_args():
    """
    Parse command-line arguments for the FACET-II experimental data processing script.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments containing:
            - input_dir (str): Directory with FACET-II experimental data files
                             Default: './examples/data/input/FACET-II_Experimental_Data/'
            - output_dir (str): Directory to save processed HDF5 files and summary
                              Default: './examples/data/output/FACET-II_Experimental_Example/'
            - lattice_dir (str): Directory containing lattice files (rfdata*, yaml, etc.)
                               Default: './examples/data/input/FACET-II_Experimental_Data/Lattice_Files/'
            - Combine_Files (str): Whether to combine all processed files ('True'/'False')
                                 Default: 'True'
    """
    parser = argparse.ArgumentParser(description='Process FACET-II experimental data into standardized format.')
    parser.add_argument('--input_dir', type=str, default='./examples/data/input/FACET-II_Experimental_Data/',
                        help='Directory containing FACET-II experimental data files')
    parser.add_argument('--output_dir', type=str, default='./examples/data/output/FACET-II_Experimental_Example/',
                        help='Directory to save processed HDF5 files and summary')
    parser.add_argument('--lattice_dir', type=str, default='./examples/data/input/FACET-II_Experimental_Data/Lattice_Files/',
                        help='Directory containing lattice files (rfdata*, yaml, etc.)')
    parser.add_argument('--Combine_Files',type=str,default='True',help='Combine all processed files into a single HDF5 file after processing')
    return parser.parse_args()

# ========================================
# Load experimental data from files
# ========================================
def load_data(input_dir):
    """
    Load experimental data from input directory.
    
    Args:
        input_dir (str): Directory containing data files.
    
    Returns:
        tuple: (all_data, VCC_all, all_images) containing:
            - all_data: DataFrame with scalar measurements
            - VCC_all: Numpy array of VCC images
            - all_images: Numpy array of screen images
    """
    # Load scalar measurement data (BPMs, magnets, RF, etc.) from pickle file
    all_data = pd.read_pickle(os.path.join(input_dir, "total_data_stack_571.pkl"))
    
    # Load VCC (Virtual Cathode Camera) images for all shots
    VCC_all = np.load(os.path.join(input_dir, 'VCC_stack_571.npy'))
    
    # Load screen camera images (PROF:IN10:571) for all shots
    all_images = np.load(os.path.join(input_dir, 'total_images_stack_571.npy'))
    
    return all_data, VCC_all, all_images

# ========================================
# Device location mapping: EPICS PV name -> Lattice element name
# ========================================
# This dictionary maps EPICS Process Variable (PV) names to their corresponding
# lattice element names used in the beamline model. This allows correlation between
# experimental measurements and lattice positions.
# Consider moving this to a YAML file (e.g., 'device_locations.yaml') for easier maintenance
loc_dict = {
    # Solenoids
    'SOLN:IN10:121': "SOL10121",
    'SOLN:IN10:111': "SOL10111",
    # Quadrupoles
    'QUAD:IN10:121': "CQ10121",
    'QUAD:IN10:122': "SQ10122",
    'QUAD:IN10:361': "QA10361",
    'QUAD:IN10:371': "QA10371",
    'QUAD:IN10:425': "QE10425",
    'QUAD:IN10:441': "QE10441",
    'QUAD:IN10:511': "QE10511",
    'QUAD:IN10:525': "QE10525",
    # RF Klystrons
    'KLYS:LI10:21': "GUNF",
    'KLYS:LI10:31': "L0AF",
    'KLYS:LI10:41': "L0BF",
    # Transverse cavity
    'TCAV:IN20:490': "TCY10490",
    # Beam Position Monitors
    'BPMS:IN10:221': 'BPM10221',
    'BPMS:IN10:371': 'BPM10371',
    'BPMS:IN10:425': 'BPM10425',
    'BPMS:IN10:511': 'BPM10511',
    'BPMS:IN10:525': 'BPM10525',
    'BPMS:IN10:581': 'BPM10581',
    'BPMS:IN10:631': 'BPM10631',
    'BPMS:IN10:651': 'BPM10651',
    'BPMS:IN10:731': 'BPM10731',
    'BPMS:IN10:771': 'BPM10771',
    'BPMS:IN10:781': 'BPM10781',
    # Toroids (charge monitors)
    'TORO:IN10:591': 'IM10591',
    'TORO:IN10:791': 'IM10791',
    # Cameras
    'CAMR:LT10:900': 'VCCF',      # Virtual Cathode Camera
    'PROF:IN10:571': 'PR10571'     # Profile screen
}

# ========================================
# Input parameters: Control setpoints (what we set)
# ========================================
# Dictionary mapping input PV names to their units
# BCTRL = Magnetic field control setpoint
# PDES = RF phase desired setpoint
# ADES = RF amplitude desired setpoint
# Consider moving this to a YAML file (e.g., 'input_parameters.yaml') for easier maintenance
input_cols = {
    # Solenoid field setpoints
    'SOLN:IN10:121:BCTRL': 'kGm',
    'SOLN:IN10:111:BCTRL': 'kGm',
    # Quadrupole field setpoints
    'QUAD:IN10:121:BCTRL': 'kG',
    'QUAD:IN10:122:BCTRL': 'kG',
    'QUAD:IN10:361:BCTRL': 'kG',
    'QUAD:IN10:371:BCTRL': 'kG',
    'QUAD:IN10:425:BCTRL': 'kG',
    'QUAD:IN10:441:BCTRL': 'kG',
    'QUAD:IN10:511:BCTRL': 'kG',
    'QUAD:IN10:525:BCTRL': 'kG',
    # RF gun klystron setpoints
    'KLYS:LI10:21:PDES': 'unitless',
    'KLYS:LI10:21:ADES': 'unitless',
 
 'KLYS:LI10:31:PDES': 'unitless',
 'KLYS:LI10:31:ADES': 'unitless',
 
 'KLYS:LI10:41:PDES': 'unitless',
 'KLYS:LI10:41:ADES': 'unitless',
 
#  'KLYS:LI10:51:PHAS': 'unitless',
#  'KLYS:LI10:51:AMPL': 'unitless',
}
#  'KLYS:LI20:51:BEAMCODE1_TCTL': 'unitless'}



# ========================================
# Output parameters: Measured values (what we observe)
# ========================================
# Dictionary mapping output PV names to their units
# AMPL = RF amplitude actual
# PHAS = RF phase actual
# BACT = Magnetic field actual (readback)
# X, Y = Beam position
# TMIT = Transmitted intensity (charge)
# XRMS, YRMS = Beam RMS size from camera
# Consider moving this to a YAML file (e.g., 'output_parameters.yaml') for easier maintenance
scalar_output_cols = {
    # RF klystron readbacks
    'KLYS:LI10:21:AMPL': 'unitless',
    'KLYS:LI10:21:PHAS': 'unitless',
    'KLYS:LI10:21:SFB_PDIS': 'unitless',
    'KLYS:LI10:31:AMPL': 'unitless',
    'KLYS:LI10:31:PHAS': 'unitless',
    'KLYS:LI10:41:AMPL': 'unitless',
    'KLYS:LI10:41:PHAS': 'unitless',
    # Magnet field readbacks
    'SOLN:IN10:121:BACT': 'kGm',
    'SOLN:IN10:111:BACT': 'kG',
    'QUAD:IN10:121:BACT': 'kG',
    'QUAD:IN10:122:BACT': 'kG',
    'QUAD:IN10:361:BACT': 'kG',
    'QUAD:IN10:371:BACT': 'kG',
    'QUAD:IN10:425:BACT': 'kG',
    'QUAD:IN10:441:BACT': 'kG',
    'QUAD:IN10:511:BACT': 'kG',
    'QUAD:IN10:525:BACT': 'kG',
    # BPM X positions
    'BPMS:IN10:221:X': 'mm',
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
    'BPMS:IN10:221:TMIT': 'unitless',
    'BPMS:IN10:371:TMIT': 'unitless',
    'BPMS:IN10:425:TMIT': 'unitless',
    'BPMS:IN10:511:TMIT': 'unitless',
    'BPMS:IN10:525:TMIT': 'unitless',
    'BPMS:IN10:581:TMIT': 'unitless',
    'BPMS:IN10:631:TMIT': 'unitless',
    'BPMS:IN10:651:TMIT': 'unitless',
    'BPMS:IN10:731:TMIT': 'unitless',
    'BPMS:IN10:771:TMIT': 'unitless',
    'BPMS:IN10:781:TMIT': 'unitless',
    'TORO:IN10:591:TMIT_PC': 'pC',
    'TORO:IN10:791:TMIT_PC': 'pC',
    'CAMR:LT10:900:XRMS': 'mm',
    'CAMR:LT10:900:YRMS': 'mm',
    'CAMR:LT10:900:X': 'mm',
    'CAMR:LT10:900:Y': 'mm',
    'PROF:IN10:571:XRMS': 'mm',
    'PROF:IN10:571:YRMS': 'mm',
    'PROF:IN10:571:X': 'mm',
    'PROF:IN10:571:Y': 'mm',
    'LASR:LT10:930:PWR': 'MW',
    'PMTR:HT10:950:PWR': 'MW',
    'IOC:SYS1:MP01:LSHUTCTL': 'unitless',
    #  'KLYS:LI10:51:PDES': 'unitless',
    #  'KLYS:LI10:51:AMPL': 'unitless',
    'TCAV:IN20:490:TC0_C_1_TCTL': 'unitless'}

# ========================================
# Lattice and run metadata
# ========================================
# URL to the lattice definition repository
lattice_location = 'https://github.com/slaclab/facet2-lattice'

# Metadata for run information
# Consider moving this to a YAML file (e.g., 'run_metadata.yaml')
metadata = {
    'source': 'FACET-II Injector',
    'date': '2024-01-27',
    'notes': 'Processed data from NERSC'
}

# Initialize summary table to collect data from all processed shots
summary_table = []
# ========================================
# Data processing function
# ========================================

def add_datapoints(batch_dims, VCC, data_subset, image_subset, input_cols, scalar_output_cols, metadata,
                   output_dir, lattice_location=lattice_location, loc_dict=loc_dict,
                   summary_table=summary_table):
    """
    Process experimental data for one or more shots and save to HDF5 file.
    
    This function creates a DataPoint2 object containing:
    - Input camera image (VCC - Virtual Cathode Camera)
    - Output camera image (profile screen)
    - Scalar input parameters (magnet settings, RF setpoints)
    - Scalar output parameters (BPM positions, beam size, charge, etc.)
    - Lattice information and device location mapping
    - Run metadata
    
    Args:
        batch_dims (list): List of batch dimension sizes. Empty list [] for no batching.
        VCC (np.ndarray): VCC camera image(s) with shape (n_shots, height, width) or (height, width).
        data_subset (pd.DataFrame): DataFrame containing scalar measurements for the shot(s).
        image_subset (np.ndarray): Profile screen image(s) with shape (n_shots, height, width) or (height, width).
        input_cols (dict): Dictionary mapping input PV names to their units.
        scalar_output_cols (dict): Dictionary mapping output PV names to their units.
        metadata (dict): Run metadata containing 'source', 'date', and 'notes'.
        output_dir (str): Directory path where HDF5 files will be saved.
        lattice_location (str): URL or path to lattice definition.
        loc_dict (dict): Dictionary mapping PV names to lattice element names.
        summary_table (list): List to append summary information to (modified in-place).
    
    Returns:
        tuple: (D, summary_table) where:
            - D (DataPoint2): The populated data point object.
            - summary_table (list): Updated summary table with this shot's entry appended.
    """
    # Create new data point object
    D = DataPoint2()
    D.add_observable(batch_dims=batch_dims, num_feature_dims=2, location=[loc_dict['CAMR:LT10:900']], data=VCC, attrs={'pixel_calibration':data_subset['CAMR:LT10:900:RESOLUTION'].values.tolist()}, data_name='VCC_Image', units='um', location_primary=True, control=True)
    D.add_observable(batch_dims=batch_dims, num_feature_dims=2, location=[loc_dict['PROF:IN10:571']], data=image_subset, attrs={'pixel_calibration':data_subset['PROF:IN10:571:RESOLUTION'].values.tolist()}, data_name='Screen_Image', units='um', location_primary=True, control=False)
    # Populate scalar inputs for this shot
    for col in input_cols.keys():
        col_data = np.asarray(data_subset[col].values, dtype=np.float64)
        # For single shots (len(batch_dims)==0), extract scalar value; for batches, keep as array
        if len(batch_dims) == 0:
            col_data = col_data[0]
        
        D.add_observable(batch_dims=batch_dims, num_feature_dims=0, location=[loc_dict.get(':'.join(col.split(':')[:3]), col)], data=col_data, attrs={}, units=input_cols[col], data_name=':'.join(col.split(':')[3:]), location_primary=True, control=True)
    for col in scalar_output_cols.keys():
        col_data = np.asarray(data_subset[col].values, dtype=np.float64)
        # For single shots (len(batch_dims)==0), extract scalar value; for batches, keep as array
        if len(batch_dims) == 0:
            col_data = col_data[0]
        
        D.add_observable(batch_dims=batch_dims, num_feature_dims=0, location=[loc_dict.get(':'.join(col.split(':')[:3]), col)], data=col_data, attrs={}, units=scalar_output_cols[col], data_name=':'.join(col.split(':')[3:]), location_primary=True, control=False)

    # Add lattice information and device location mapping
    D.add_lattice(lattice_location=lattice_location, PV_table=loc_dict)
    
    # Add run metadata (source, date, notes)
    D.add_run_information(
        source=metadata['source'], 
        date=metadata['date'], 
        notes=metadata['notes']
    )
    
    # Save data point to HDF5 file
    os.makedirs(output_dir, exist_ok=True)
    D.saveHDF5(output_dir)
    
    # Extract summary data and append to summary table for cross-shot analysis
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
    
    return D, summary_table
# ========================================
# Main processing: Loop over shots and save to HDF5
# ========================================
def main():
    """
    Main execution function for processing FACET-II experimental data.
    
    This function:
    1. Parses command-line arguments for input/output directories
    2. Loads experimental data (scalars and images) from input directory
    3. Processes shots in two groups:
       - Individual shots 1-5: One HDF5 file per shot
       - Batch shots 6-10: Multiple shots in one HDF5 file
    4. For each shot or batch:
       - Creates a DataPoint2 object
       - Adds scalar inputs (control parameters)
       - Adds input distribution (VCC image)
       - Adds scalar outputs (measurements grouped by location)
       - Adds output image (screen camera)
       - Saves to standardized HDF5 format
    5. Creates summary_table.yaml with key parameters from all shots
    6. Optionally combines all processed files into a single HDF5 file:
       - Uses combine_files() to merge individual shot files
       - Moves combined file to output directory
       - Removes individual files to save space
    
    Command-line Arguments:
        --input_dir: Directory containing FACET-II experimental data files
                    Default: './examples/data/input/FACET-II_Experimental_Data/'
        --output_dir: Directory for output HDF5 files and summary
                     Default: './examples/data/output/FACET-II_Experimental_Example/'
        --lattice_dir: Directory containing lattice files
                      Default: './examples/data/input/FACET-II_Experimental_Data/Lattice_Files/'
        --Combine_Files: Combine processed files into single file ('True'/'False')
                        Default: 'True'
    
    Output Files:
        - Individual HDF5 files: <output_dir>/<ID>.h5 (if Combine_Files=False)
        - summary_table.yaml: Summary statistics and parameters from all shots
        - Combined_Data.h5: Single merged file (if Combine_Files=True)
    """
    # Parse command-line arguments
    args = parse_args()
    
    # Load experimental data
    print(f"Loading data from {args.input_dir}...")
    all_data, VCC_all, all_images = load_data(args.input_dir)
    print(f"Loaded {len(all_data)} shots.")
    
    summary_table = []
    
    # Process first 5 shots individually (one file per shot)
    print("\nProcessing individual shots (1-5)...")
    for i in range(5):
        # Extract data for shot i as 2D images (no shot dimension)
        VCC = VCC_all[i, :, :]  # Shape: (height, width)
        data_subset = all_data.loc[[i]]  # DataFrame with one row
        image_subset = all_images[i, :, :]  # Shape: (height, width)
        
        # Process and save this shot
        D, summary_table = add_datapoints(
            batch_dims=[],  # No batching (single shot per file)
            VCC=VCC,
            data_subset=data_subset,
            image_subset=image_subset,
            input_cols=input_cols,
            scalar_output_cols=scalar_output_cols,
            metadata=metadata,
            output_dir=args.output_dir,
            lattice_location=lattice_location,
            loc_dict=loc_dict,
            summary_table=summary_table
        )
        print(f"  Processed shot {i+1}/5")
    
    # Process shots 5-9 as a batch (multiple shots in one file)
    # This demonstrates batch processing capability
    print("\nProcessing batch of shots (5-9)...")
    VCC = VCC_all[5:10, :, :]  # Shape: (5, height, width)
    data_subset = all_data.loc[5:9]  # DataFrame with 5 rows
    image_subset = all_images[5:10, :, :]  # Shape: (5, height, width)
    
    D, summary_table = add_datapoints(
        batch_dims=[5],  # Batch of 5 shots
        VCC=VCC,
        data_subset=data_subset,
        image_subset=image_subset,
        input_cols=input_cols,
        scalar_output_cols=scalar_output_cols,
        metadata=metadata,
        output_dir=args.output_dir,
        lattice_location=lattice_location,
        loc_dict=loc_dict,
        summary_table=summary_table
    )
    print("  Processed batch of 5 shots")
    
    # Write summary table to YAML for easy review and querying
    summary_file = os.path.join(args.output_dir, 'summary_table.yaml')
    with open(summary_file, 'w') as f:
        yaml.dump(summary_table, f)
    
    print(f"\nProcessing complete. {len(summary_table)} shots processed.")
    print(f"HDF5 files saved to {args.output_dir}")
    print(f"Summary table saved to {summary_file}")
    # Combine files into a single HDF5 file
    if args.Combine_Files.lower() == 'true':
        print("Combining processed files into a single HDF5 file...")
        
        # Get list of individual HDF5 files before combining
        individual_files = [f for f in os.listdir(args.output_dir) 
                          if f.endswith('.h5') and f != 'Combined_Data.h5']
        
        # Create combined file in output directory
        combined_path = os.path.join(args.output_dir, 'Combined_Data.h5')
        combine_files(args.output_dir, combined_path)
        
        # Delete individual files, keeping only Combined_Data.h5 and summary_table.yaml
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
    