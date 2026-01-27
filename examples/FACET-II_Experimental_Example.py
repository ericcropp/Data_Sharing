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
- argparse
- Data_Standard_2.DataPoint2

Usage:
  python Experiment2DataStandard2.py --input_dir <path> --output_dir <path>
"""

import numpy as np
from data_standard.Data_Standard_2 import DataPoint2
import pandas as pd
import os
import yaml
import argparse

def parse_args():
    """
    Parse command-line arguments for the FACET-II experimental data processing script.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments containing:
            - input_dir: Directory with FACET-II experimental data files
            - output_dir: Directory to save processed HDF5 files and summary
            - lattice_dir: Directory containing lattice files (rfdata*, yaml, etc.)
    """
    parser = argparse.ArgumentParser(description='Process FACET-II experimental data into standardized format.')
    parser.add_argument('--input_dir', type=str, default='./examples/data/input/FACET-II_Experimental_Data/',
                        help='Directory containing FACET-II experimental data files')
    parser.add_argument('--output_dir', type=str, default='./examples/data/output/FACET-II_Experimental_Example/',
                        help='Directory to save processed HDF5 files and summary')
    parser.add_argument('--lattice_dir', type=str, default='./examples/data/input/FACET-II_Experimental_Data/Lattice_Files/',
                        help='Directory containing lattice files (rfdata*, yaml, etc.)')
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
# Summary configuration
# ========================================
# List of keys to include in summary output for quick overview
# These are extracted from the data and saved as file attributes for easy querying
# Consider moving this to a YAML file (e.g., 'summary_config.yaml')
summary_keys = ['XRMS', 'YRMS']  # Beam RMS sizes from camera

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

def add_datapoints(batch_dim, VCC, data_subset, image_subset, input_cols, scalar_output_cols, metadata,
                   output_dir, lattice_location=lattice_location, loc_dict=loc_dict, summary_keys=summary_keys,
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
    - Summary statistics
    
    Args:
        batch_dim (int): Number of batch dimensions (0 for no batching).
        VCC (np.ndarray): VCC camera image(s) with shape (n_shots, height, width).
        data_subset (pd.DataFrame): DataFrame containing scalar measurements for the shot(s).
        image_subset (np.ndarray): Profile screen image(s) with shape (n_shots, height, width).
        input_cols (dict): Dictionary mapping input PV names to their units.
        scalar_output_cols (dict): Dictionary mapping output PV names to their units.
        metadata (dict): Run metadata containing 'source', 'date', and 'notes'.
        output_dir (str): Directory path where HDF5 files will be saved.
        lattice_location (str): URL or path to lattice definition.
        loc_dict (dict): Dictionary mapping PV names to lattice element names.
        summary_keys (list): List of keys to include in summary output.
        summary_table (list): List to append summary information to (modified in-place).
    
    Returns:
        tuple: (D, summary_table) where:
            - D (DataPoint2): The populated data point object.
            - summary_table (list): Updated summary table with this shot's entry appended.
    """
    # Create new data point object
    D = DataPoint2()
    D.add_observable(batch_dims=batch_dim, feature_dims=2, location=[loc_dict['CAMR:LT10:900']], data=VCC, attrs={'pixel_calibration':data_subset['CAMR:LT10:900:RESOLUTION'].values.tolist()}, data_names='VCC_Image', units='um',location_primary=True,control=True)
    D.add_observable(batch_dims=batch_dim, feature_dims=2, location=[loc_dict['PROF:IN10:571']], data=image_subset, attrs={'pixel_calibration':data_subset['PROF:IN10:571:RESOLUTION'].values.tolist()}, data_names='Screen_Image', units='um',location_primary=True,control=False)
    # Populate scalar inputs for this shot
    for col in input_cols.keys():
        col_data = np.asarray(data_subset[col].values, dtype=np.float64)

        D.add_observable(batch_dims=batch_dim, feature_dims=0, location=[loc_dict.get(':'.join(col.split(':')[:3]), col)], data=col_data, attrs={}, units=input_cols[col], data_names=':'.join(col.split(':')[3:]),location_primary=True,control=True)
    for col in scalar_output_cols.keys():
        col_data = np.asarray(data_subset[col].values, dtype=np.float64)

        D.add_observable(batch_dims=batch_dim, feature_dims=0, location=[loc_dict.get(':'.join(col.split(':')[:3]), col)], data=col_data, attrs={}, units=scalar_output_cols[col], data_names=':'.join(col.split(':')[3:]),location_primary=True,control=False)

    # Add lattice information and device location mapping
    D.add_lattice(lattice_location=lattice_location, PV_table=loc_dict)
    
    # Add run metadata (source, date, notes)
    D.add_run_information(
        source=metadata['source'], 
        date=metadata['date'], 
        notes=metadata['notes']
    )
    
    # Add summary information for quick querying
    # This extracts specified keys and saves them as file attributes
    D.add_summary(summary_keys)
    
    # Save data point to HDF5 file
    os.makedirs(output_dir, exist_ok=True)
    D.saveHDF5(output_dir)
    
    # Extract summary data and append to summary table for cross-shot analysis
    entry = {
        **D.summary.summary
    }
    summary_table.append(entry)
    
    return D, summary_table
# ========================================
# Main processing: Loop over shots and save to HDF5
# ========================================
def main():
    """
    Main execution function for processing experimental data.
    
    Parses command-line arguments, loads data, processes shots, and saves results.
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
        # Extract data for shot i with singleton shot dimension for consistency
        VCC = VCC_all[i:i+1, :, :]  # Shape: (1, height, width)
        data_subset = all_data.loc[[i]]  # DataFrame with one row
        image_subset = all_images[i:i+1, :, :]  # Shape: (1, height, width)
        
        # Process and save this shot
        D, summary_table = add_datapoints(
            batch_dim=0,  # No batching (single shot per file)
            VCC=VCC,
            data_subset=data_subset,
            image_subset=image_subset,
            input_cols=input_cols,
            scalar_output_cols=scalar_output_cols,
            metadata=metadata,
            output_dir=args.output_dir,
            lattice_location=lattice_location,
            loc_dict=loc_dict,
            summary_keys=summary_keys,
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
        batch_dim=0,  # Still no batching at the batch_dims level (shots are in the data arrays)
        VCC=VCC,
        data_subset=data_subset,
        image_subset=image_subset,
        input_cols=input_cols,
        scalar_output_cols=scalar_output_cols,
        metadata=metadata,
        output_dir=args.output_dir,
        lattice_location=lattice_location,
        loc_dict=loc_dict,
        summary_keys=summary_keys,
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

if __name__ == '__main__':
    main()
    