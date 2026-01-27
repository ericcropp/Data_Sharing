"""Process FACET-II Impact-T simulation archives into standardized data format.

This script processes Impact simulation archives and converts them to a standardized data format.

For each simulation archive (.h5 file) in the input directory, the script:
1. Loads the simulation archive and compares it to a template YAML file (ImpactT.yaml).
2. Extracts differences in lattice elements and header data between the archive and template.
3. Collects run information, particle distributions, and simulation statistics.
4. Populates a SimulatedDataPoint2 object with:
    - Initial and final particle data
    - Screen/observer particle data at intermediate locations
    - Simulation statistics (emittance, beam size, energy, etc.) along the beamline
    - Input parameters that differ from the template
    - Lattice file contents
    - Simulation metadata and run information
5. Saves each processed simulation as an HDF5 file in the output directory.
6. Compiles a summary table of all processed simulations.
7. Optionally combines all processed files into a single HDF5 file.

Command-line Arguments:
    --input_dir: Directory containing Impact simulation archive files (.h5)
                 [default: './examples/data/input/FACET-II_Simulation_Data/']
    --output_dir: Directory to save processed HDF5 files and summary
                  [default: './examples/data/output/FACET-II_Simulation_Example/']
    --lattice_dir: Directory containing lattice files (rfdata*, ImpactT.yaml, etc.)
                   [default: './examples/data/input/FACET-II_Simulation_Data/Lattice_Files/']
    --Combine_Files: Combine all processed files into single HDF5 file after processing
                     [default: 'True']

Required Files in lattice_dir:
    - ImpactT.yaml: Template YAML file for lattice comparison
    - Lattice data files (e.g., rfdata4, rfdata5, rfdata6, rfdata7, rfdata201, rfdata102)

Usage:
    # Process with default paths and combine files
    python FACET-II_Simulation_Example.py
    
    # Process with custom paths without combining
    python FACET-II_Simulation_Example.py \
        --input_dir /path/to/archives \
        --output_dir /path/to/output \
        --lattice_dir /path/to/lattice \
        --Combine_Files False

Dependencies:
    - numpy, pandas, yaml, os, datetime, argparse, shutil
    - impact, pmd_beamphysics
    - data_standard.SimulatedDataPoint2
    - data_standard.combine_files

Output:
    - Individual standardized HDF5 files for each simulation (if Combine_Files=False)
    - summary_table.yaml: Summary of all processed simulations with key parameters
    - Combined_Data.h5: Single merged file containing all simulations (if Combine_Files=True)
"""

import numpy as np


from data_standard import SimulatedDataPoint2
from data_standard import combine_files

import pandas as pd
import os
import yaml
import impact
import datetime
from pmd_beamphysics import ParticleGroup
import argparse
import shutil

SIMULATION_VERSION = "impact-t=3.1.2; lume-impact=0.10.1"

def parse_args():
    """
    Parse command-line arguments for the Impact simulation processing script.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments containing:
            - input_dir (str): Directory with Impact simulation archive files (.h5)
                             Default: './examples/data/input/FACET-II_Simulation_Data/'
            - output_dir (str): Directory to save processed HDF5 files and summary
                              Default: './examples/data/output/FACET-II_Simulation_Example/'
            - lattice_dir (str): Directory containing lattice files (rfdata*, yaml, etc.)
                               Default: './examples/data/input/FACET-II_Simulation_Data/Lattice_Files/'
            - Combine_Files (str): Whether to combine all processed files ('True'/'False')
                                 Default: 'True'
    """
    parser = argparse.ArgumentParser(description='Process Impact simulation archives into standardized data format.')
    parser.add_argument('--input_dir', type=str, default='./examples/data/input/FACET-II_Simulation_Data/',
                        help='Directory containing FACET-II experimental data files')
    parser.add_argument('--output_dir', type=str, default='./examples/data/output/FACET-II_Simulation_Example/',
                        help='Directory to save processed HDF5 files and summary')
    parser.add_argument('--lattice_dir', type=str, default='./examples/data/input/FACET-II_Simulation_Data/Lattice_Files/',
                        help='Directory containing lattice files (rfdata*, yaml, etc.)')
    parser.add_argument('--Combine_Files',type=str,default='True',help='Combine all processed files into a single HDF5 file after processing')
    return parser.parse_args()

summary_table = []


# Define units for Impact-T input parameters; these are general to any Impact-T simulation
# Keys are parameter name suffixes, values are their physical units
unit_list = {'b1_gradient': 'T/m','theta0_deg': 'unitless','rf_field_scale': 'V/m','solenoid_field_scale': 'T'}

# Define units for Impact-T simulation output statistics; these are general to any Impact-T simulation
# These parameters are computed by the simulation at multiple z-locations along the beamline
output_unit_list = {
    'cov_x__px': 'm',
    'cov_y__py': 'm',
    'cov_z__pz': 'm',
    'loadbalance_max_n_particle': 'unitless',
    'loadbalance_min_n_particle': 'unitless',
    'max_amplitude_x': 'm',
    'max_amplitude_y': 'm',
    'max_amplitude_z': 'm',
    'max_r': 'm',
    'mean_beta': 'unitless',
    'mean_gamma': 'unitless',
    'mean_kinetic_energy': 'eV',
    'mean_x': 'm',
    'mean_y': 'm',
    'mean_z': 'm',
    'n_particle': 'unitless',
    'norm_emit_x': 'm',
    'norm_emit_y': 'm',
    'norm_emit_z': 'm',
    'sigma_gamma': 'unitless',
    'sigma_x': 'm',
    'sigma_y': 'm',
    'sigma_z': 'm',
    't': 's'
}



def lattice_comparison(lattice_I_dict, lattice_I_orig_dict, I, I_orig):
    """
    Compare simulation lattice and header data against a template to identify differences.
    Only differences from the template are recorded in data_dict and in the overall DataPoint.
    
    This function performs a comprehensive comparison between a simulation's lattice/header
    configuration and a template configuration. It identifies:
    - Missing elements in either the simulation or template
    - Differing parameter values between simulation and template
    - Header data differences
    
    Args:
        lattice_I_dict (dict): Dictionary of lattice elements from the simulation, keyed by element name.
        lattice_I_orig_dict (dict): Dictionary of lattice elements from the template, keyed by element name.
        I (impact.Impact): Impact simulation object containing the current simulation.
        I_orig (impact.Impact): Impact simulation object containing the template.
    
    Returns:
        tuple: (diff_dict, data_dict) where:
            - diff_dict (dict): Comprehensive record of all differences, including missing elements
                               and value comparisons. Keys use format "name:subkey:I" or "name:subkey:I_ORIG".
            - data_dict (dict): Dictionary containing only the simulation values that differ from
                               the template. Keys use format "name:subkey".
    """
    # Compare lattice elements to original lattice -- we will save original lattice, so only need to store changes.
    
    all_names = set(lattice_I_dict.keys()).union(lattice_I_orig_dict.keys())

    diff_dict = {}
    data_dict = {}
    # Record differences between lattice elements
    for name in all_names:
        if name not in lattice_I_dict:
            # Element missing in simulation lattice
            diff_dict[f"{name}:MISSING_IN_I"] = None
        elif name not in lattice_I_orig_dict:
            # Element missing in template lattice
            diff_dict[f"{name}:MISSING_IN_I_ORIG"] = None
        else:
            # Compare subkeys for elements present in both
            if lattice_I_dict[name] != lattice_I_orig_dict[name]:
                for subkey in lattice_I_dict[name]:
                    if subkey not in lattice_I_orig_dict[name]:
                        # Subkey missing in template
                        diff_dict[f"{name}:{subkey}:MISSING_IN_I_ORIG"] = lattice_I_dict[name][subkey]
                    elif lattice_I_dict[name][subkey] != lattice_I_orig_dict[name][subkey]:
                        # Subkey value differs
                        diff_dict[f"{name}:{subkey}:I"] = lattice_I_dict[name][subkey]
                        data_dict[f"{name}:{subkey}"] = lattice_I_dict[name][subkey]
                        diff_dict[f"{name}:{subkey}:I_ORIG"] = lattice_I_orig_dict[name][subkey]
                for subkey in lattice_I_orig_dict[name]:
                    if subkey not in lattice_I_dict[name]:
                        # Subkey missing in simulation
                        diff_dict[f"{name}:{subkey}:MISSING_IN_I"] = lattice_I_orig_dict[name][subkey]

    # Repeat lattice comparison (redundant, but preserves original logic)
    lattice_I_dict = {elem.get('name', f'idx_{i}'): elem for i, elem in enumerate(I.input['lattice'])}
    lattice_I_orig_dict = {elem.get('name', f'idx_{i}'): elem for i, elem in enumerate(I_orig.input['lattice'])}
    all_names = set(lattice_I_dict.keys()).union(lattice_I_orig_dict.keys())

    for name in all_names:
        if name not in lattice_I_dict:
            diff_dict[f"{name}:MISSING_IN_I"] = None
        elif name not in lattice_I_orig_dict:
            diff_dict[f"{name}:MISSING_IN_I_ORIG"] = None
        else:
            if lattice_I_dict[name] != lattice_I_orig_dict[name]:
                for subkey in lattice_I_dict[name]:
                    if subkey not in lattice_I_orig_dict[name]:
                        diff_dict[f"{name}:{subkey}:MISSING_IN_I_ORIG"] = lattice_I_dict[name][subkey]
                    elif lattice_I_dict[name][subkey] != lattice_I_orig_dict[name][subkey]:
                        diff_dict[f"{name}:{subkey}:I"] = lattice_I_dict[name][subkey]
                        data_dict[f"{name}:{subkey}"] = lattice_I_dict[name][subkey]
                        diff_dict[f"{name}:{subkey}:I_ORIG"] = lattice_I_orig_dict[name][subkey]
                for subkey in lattice_I_orig_dict[name]:
                    if subkey not in lattice_I_dict[name]:
                        diff_dict[f"{name}:{subkey}:MISSING_IN_I"] = lattice_I_orig_dict[name][subkey]

    # Compare header data and record differences
    header_I = I.input.get('header', {})
    header_I_orig = I_orig.input.get('header', {})
    header_keys = set(header_I.keys()).union(header_I_orig.keys())
    for key in header_keys:
        if key not in header_I:
            diff_dict[f"header:{key}:MISSING_IN_I"] = header_I_orig[key]
        elif key not in header_I_orig:
            diff_dict[f"header:{key}:MISSING_IN_I_ORIG"] = header_I[key]
            data_dict[f"header:{key}"] = header_I[key]
        elif header_I[key] != header_I_orig[key]:
            diff_dict[f"header:{key}:I"] = header_I[key]
            data_dict[f"header:{key}"] = header_I[key]
            diff_dict[f"header:{key}:I_ORIG"] = header_I_orig[key]
    return diff_dict, data_dict

def extract_run_info(I):
    """
    Extract and format run information from an Impact simulation.
    
    Extracts metadata about the simulation run, including start time, and formats
    it into a standardized dictionary. Handles various timestamp formats and converts
    them to YYYY-MM-DD date strings.
    
    Args:
        I (impact.Impact): Lume-Impact simulation object (contains all run information)
    
    Returns:
        dict: Dictionary containing run metadata:
            - source (str): Source identifier (always 'Impact simulation')
            - notes (str): Additional notes about the run
            - date (str): Formatted date string (YYYY-MM-DD) or original timestamp if parsing fails
            - Additional fields from I.output['run_info'] if available
    """
    # Extract and format run information
    start_time = I.output['run_info'].get('start_time')
    if start_time:
        try:
            # Parse start_time to date string
            if isinstance(start_time, (int, float)):
                date_str = datetime.datetime.fromtimestamp(start_time).strftime('%Y-%m-%d')
            else:
                date_obj = datetime.datetime.fromisoformat(str(start_time))
                date_str = date_obj.strftime('%Y-%m-%d')
        except Exception:
            date_str = str(start_time)
    else:
        date_str = ""
    run_info = {
        'source': 'Impact simulation',
        'notes': 'Test Batch',
        "date": date_str
    }
    run_info.update(I.output.get('run_info', {}))
    return run_info

def extract_input_file_contents(I, lattice_dir):
    """
    Generate and extract the Impact input file contents from a simulation object.
    
    This function writes the Impact simulation configuration to a temporary input file,
    then reads and returns its contents as a string. The temporary file is created in
    the specified lattice directory.
    
    Args:
        I (impact.Impact): Lume-Impact simulation object.
        lattice_dir (str): Directory path where the temporary input file will be written.
    
    Returns:
        str: Complete contents of the generated Impact input file.
    """
    # Write Impact input file to temporary location and read it back
    I.write_input(input_filename='Temp.in', path=lattice_dir)
    with open(os.path.join(lattice_dir, 'Temp.in'), 'r') as f_input:
        input_contents = f_input.read()
    return input_contents

def load_lattice_file_contents(lattice_dir):
    """
    Load all readable text files from a lattice directory.
    
    Reads all regular files in the specified directory and returns their contents
    as a dictionary. Skips files that cannot be read (binary files, permission issues, etc.).
    
    Args:
        lattice_dir (str): Path to directory containing lattice configuration files.
    
    Returns:
        dict: Dictionary mapping filename (str) to file contents (str) for all readable files.
              Returns empty dict if directory doesn't exist or contains no readable files.
    """
    # Read all files in the lattice directory
    file_contents = {}
    if os.path.isdir(lattice_dir):
        for filename in os.listdir(lattice_dir):
            filepath = os.path.join(lattice_dir, filename)
            # Only process regular files, skip directories
            if os.path.isfile(filepath):
                try:
                    with open(filepath, 'r') as f:
                        file_contents[filename] = f.read()
                except (UnicodeDecodeError, PermissionError, OSError):
                    # Skip files that cannot be read as text or have permission issues
                    continue
    return file_contents

def add_datapoints(batch_dim, 
                   I_list, 
                   data_dicts, 
                   run_info, 
                   input_contents, 
                   rfdata_contents, 
                   output_unit_list, 
                   unit_list, 
                   summary_table, 
                   output_dir):
    """
    Process simulation data and create a standardized HDF5 file.
    
    This function consolidates data from one or more Impact simulations into a single
    SimulatedDataPoint2 object, which is then saved as an HDF5 file. It handles:
    - Initial and final particle distributions (as ParticleGroup objects)
    - Intermediate particle data at screen/observer locations
    - Simulation statistics along the beamline (emittance, beam size, energy, etc.)
    - Input parameters that differ from the template
    - Lattice file contents and configuration
    - Run metadata and simulation information
    
    Args:
        batch_dim (int): Number of batch dimensions (0 for no batching; the usual case for simulations).
        I_list (list of impact.Impact): List of Impact simulation objects to process.
        data_dicts (list of dict): List of dictionaries containing input parameters that differ
                                    from template for each simulation.
        run_info (dict): Dictionary containing run metadata (source, date, notes).
        input_contents (str): Contents of the Impact input file as a string.
        rfdata_contents (dict): Dictionary mapping lattice filename to file contents.
        output_unit_list (dict): Dictionary mapping output parameter names to their units.
        unit_list (dict): Dictionary mapping input parameter suffixes to their units.
        summary_table (list): List to append summary information to (modified in-place).
        output_dir (str): Directory path where the HDF5 file will be saved.
    
    Returns:
        tuple: (D, summary_table) where:
            - D (SimulatedDataPoint2): The populated data point object.
            - summary_table (list): Updated summary table with this simulation's entry appended.
    """
    D = SimulatedDataPoint2()
    num_pts = len(I_list)
    
    # Add initial particles - create 1D object array to hold ParticleGroup objects
    initial_particles_data = np.empty(num_pts, dtype=object)
    for i in range(len(I_list)):
        initial_particles_data[i] = I_list[i].particles['initial_particles']
    D.add_observable(batch_dims=batch_dim, feature_dims=0, location='VCCF', data=initial_particles_data, attrs={}, data_names='VCC', units='m', location_primary=True,control=[True]*num_pts)
    
    # Add particle data at intermediate screen/observer locations
    # These are the particle distributions captured at various points along the beamline
    for key, value in I_list[0].output['particles'].items():
        if key != 'final_particles' and key != 'initial_particles':
            particle_data = np.empty(num_pts, dtype=object)
            for i in range(len(I_list)):
                particle_data[i] = I_list[i].output['particles'][key]
            D.add_observable(batch_dims=batch_dim, feature_dims=0, location=key, data=particle_data, attrs={}, data_names=key, units='m', location_primary=True,control=[False]*num_pts)
    
    # Add final particle distribution at end of beamline
    final_particles_data = np.empty(num_pts, dtype=object)
    for i in range(len(I_list)):
        final_particles_data[i] = I_list[i].particles['final_particles']
    D.add_observable(batch_dims=batch_dim, feature_dims=0, location='final_particles', data=final_particles_data, attrs={}, data_names='final_particles', units='m', location_primary=False,control=[False]*num_pts)
    
    # Add lattice configuration files (rfdata files, YAML templates, etc.)
    D.add_lattice(lattice_location='included', lattice_files=rfdata_contents)
    
    # Configure summary keys - include input parameters and key output metrics
    summary_keys = list(data_dicts[0].keys())
    if 'norm_emit_x' in I_list[0].output['stats']:
        summary_keys.append('norm_emit_x')
    D.add_summary(
        summary_keys=summary_keys,
        summary_location='final')
    
    # Add run metadata (source, date, notes)
    D.add_run_information(source=run_info['source'], date=run_info['date'], notes=run_info['notes'])
    
    # Add simulation-specific metadata (code version, input file, simulation range)
    D.add_simulation_data(
        simulation_start=I_list[0].particles['initial_particles']['mean_z'],
        simulation_end=I_list[0].particles['final_particles']['mean_z'],
        simulation_code='Impact',
        simulation_input_file=input_contents,
        simulation_version=SIMULATION_VERSION
        )
    
    # Add simulation statistics along the beamline
    # These include emittance, beam size, energy, etc. at multiple z-locations
    first_locations = None
    for key, value in I_list[0].output['stats'].items():
        if key != 'mean_z':
            # Reshape to 2D array: (num_simulations, num_z_locations)
            stat_data = np.empty((num_pts, len(I_list[0].output['stats'][key])))
            for i in range(len(I_list)):
                stat_data[i,:] = np.array(I_list[i].output['stats'][key]).reshape(1, -1)
            
                # Assert that location data is the same for every point in the batch
                if first_locations is None:
                    first_locations = I_list[i].output['stats']['mean_z'].tolist()
                else:
                    assert I_list[i].output['stats']['mean_z'].tolist() == first_locations, \
                        f"Location data mismatch at point {i}: expected {first_locations}, got {I_list[i].output['stats']['mean_z'].tolist()}"

            # Add observable with appropriate units
            if key in output_unit_list:
                D.add_observable(batch_dims=batch_dim, feature_dims=0, location=first_locations, data=stat_data, attrs={}, data_names=key, units=output_unit_list[key], location_primary=False,control=[False]*len(first_locations))
            else:
                D.add_observable(batch_dims=batch_dim, feature_dims=0, location=first_locations, data=stat_data, attrs={}, data_names=key, units="unitless", location_primary=False,control=[False]*len(first_locations))
    
    # Verify consistency across all simulations in the batch
    # Assert all data_dicts have the same keys (all simulations should vary the same parameters)
    if len(data_dicts) > 1:
        first_keys = set(data_dicts[0].keys())
        for idx, data_dict in enumerate(data_dicts[1:], start=1):
            assert set(data_dict.keys()) == first_keys, \
                f"data_dicts[{idx}] has different keys than data_dicts[0]"

    # Combine input parameters from all simulations into arrays
    # Combine each parameter key into arrays (one value per simulation)
    master_dict = {}
    for key in data_dicts[0].keys():
        master_dict[key] = np.array([data_dict[key] for data_dict in data_dicts])
    
    # Add scalar input parameters as observables with control=True (these are inputs)
    scalar_inputs = {}
    for col in master_dict:
        # Determine units based on suffix match with unit_list keys
        units = ""
        for key, val in unit_list.items():
            if col.endswith(key):
                units = val
                break
        if not units:  # If still None or blank, set default
            units = "unitless"
        scalar_inputs[col] = {
            "name": col,
            "value": master_dict[col],
            "location": col,
            "units": units,
            "description": ""   # Fill in description if available
        }
        
        # Get the data array for this parameter (one value per simulation)
        scalar_data = master_dict[col]
        
        # Add as observable with control=True to indicate it's an input parameter
        D.add_observable(batch_dims=batch_dim, feature_dims=0, location=scalar_inputs[col]['location'], data=scalar_data, attrs={'Description': scalar_inputs[col]['description']}, data_names=scalar_inputs[col]['name'], units=scalar_inputs[col]['units'], location_primary=True,control=[True]*num_pts)
    
    # Save the complete data point to HDF5 file
    os.makedirs(output_dir, exist_ok=True)
    D.saveHDF5(output_dir)
    
    # Extract summary data and append to summary table for cross-simulation analysis
    entry = {
        **D.summary.summary
    }
    summary_table.append(entry)
    
    return D, summary_table

def main():
    """
    Main execution function for processing FACET-II Impact-T simulation data.
    
    This function:
    1. Parses command-line arguments for input/output directories
    2. Discovers all .h5 simulation archive files in the input directory
    3. For each simulation archive:
       - Loads Impact-T simulation data and template configuration
       - Compares lattice elements to identify parameter variations
       - Extracts particle distributions and simulation statistics
       - Creates a SimulatedDataPoint2 object with all observables and metadata
       - Saves to standardized HDF5 format
    4. Creates summary_table.yaml with key parameters and statistics from all simulations
    5. Optionally combines all processed files into a single HDF5 file:
       - Uses combine_files() to merge individual simulation files
       - Moves combined file to output directory
       - Removes individual files to save space
    
    Command-line Arguments:
        --input_dir: Directory containing Impact .h5 archive files
                    Default: './examples/data/input/FACET-II_Simulation_Data/'
        --output_dir: Directory for output HDF5 files and summary
                     Default: './examples/data/output/FACET-II_Simulation_Example/'
        --lattice_dir: Directory containing lattice configuration files
                      Default: './examples/data/input/FACET-II_Simulation_Data/Lattice_Files/'
        --Combine_Files: Combine processed files into single file ('True'/'False')
                        Default: 'True'
    
    Output Files:
        - Individual HDF5 files: <output_dir>/<ID>.h5 (if Combine_Files=False)
        - summary_table.yaml: Summary statistics and parameters from all simulations
        - Combined_Data.h5: Single merged file (if Combine_Files=True)
    """
    args = parse_args()
    
    # Load the list of simulation archive filenames from YAML
    impact_filenames = {'impact_archive': [os.path.join(args.input_dir, fname) for fname in os.listdir(args.input_dir) if fname.endswith('.h5')]}

    
    # Discover all Impact simulation archive files (.h5) in the input directory
    impact_filenames = {
        'impact_archive': [
            os.path.join(args.input_dir, fname) 
            for fname in os.listdir(args.input_dir) 
            if fname.endswith('.h5')
        ]
    }
    summary_table = []
    
    # Process each simulation archive file
    for i in range(len(impact_filenames['impact_archive'])):
        print(f"Processing file {i+1}/{len(impact_filenames['impact_archive'])}: {impact_filenames['impact_archive'][i]}")

        # Load the Impact simulation archive
        I = impact.Impact()
        I.load_archive(impact_filenames['impact_archive'][i])

        # Load template YAML configuration for comparison
        I_orig = impact.Impact.from_yaml(os.path.join(args.lattice_dir, 'ImpactT.yaml'))

        # Convert lattice lists to dictionaries keyed by element name for comparison
        lattice_I_dict = {elem.get('name', f'idx_{i}'): elem for i, elem in enumerate(I.input['lattice'])}
        lattice_I_orig_dict = {elem.get('name', f'idx_{i}'): elem for i, elem in enumerate(I_orig.input['lattice'])}

        # Compare simulation to template and extract parameter differences
        diff_dict, data_dict = lattice_comparison(lattice_I_dict, lattice_I_orig_dict, I, I_orig)
        
        # Extract run metadata
        run_info = extract_run_info(I)

        # Generate and extract Impact input file contents
        input_contents = extract_input_file_contents(I, args.lattice_dir)

        # Load all lattice configuration files from the lattice directory
        rfdata_contents = load_lattice_file_contents(args.lattice_dir)

        # Create standardized data point and save to HDF5
        D, summary_table = add_datapoints(
            batch_dim=0,  # No batching (single simulation per file) 
            I_list=[I], 
            data_dicts=[data_dict], 
            run_info=run_info, 
            input_contents=input_contents, 
            rfdata_contents=rfdata_contents, 
            output_unit_list=output_unit_list, 
            unit_list=unit_list,
            summary_table=summary_table,
            output_dir=args.output_dir
        )
    
    # Write the complete summary table to YAML for easy review and analysis
    with open(os.path.join(args.output_dir, 'summary_table.yaml'), 'w') as f:
        yaml.dump(summary_table, f)
    
    print(f"Processing complete. Summary written to {os.path.join(args.output_dir, 'summary_table.yaml')}")

    # Combine files into a single HDF5 file
    if args.Combine_Files.lower() == 'true':
        print("Combining processed files into a single HDF5 file...")
        combine_files(args.output_dir, os.path.join(os.path.dirname(os.path.dirname(args.output_dir)), 'Combined_Data.h5'))

        print("Combined file created successfully.")
        shutil.rmtree(args.output_dir)

        os.makedirs(args.output_dir, exist_ok=True)
        shutil.move(os.path.join(os.path.dirname(os.path.dirname(args.output_dir)), 'Combined_Data.h5'), os.path.join(args.output_dir, 'Combined_Data.h5'))
    
if __name__ == '__main__':
    main()