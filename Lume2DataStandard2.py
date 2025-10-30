"""
This script processes a batch of Impact simulation archives, compares their lattice and header data to a template,
extracts differences, and organizes the results into a standardized data format using SimulatedDataPoint2.
For each simulation archive listed in 'Impact_Filenames.yaml', the script:
1. Loads the simulation archive and a template YAML file.
2. Compares lattice and header data between the archive and the template, recording differences.
3. Extracts run information, input distributions, and relevant lattice files.
4. Populates a SimulatedDataPoint2 object with inputs, lattice files, summary statistics, run information, and simulation data.
5. Saves the processed data in HDF5 format and compiles a summary table of all simulations.
6. Writes the summary table to a YAML file for further analysis.
Required files:
- Impact_Filenames.yaml: List of simulation archive filenames.
- ImpactT.yaml: Template YAML file for comparison.
- rfdata4, rfdata5, rfdata6, rfdata7, rfdata201, rfdata102: Lattice data files.
- ImpactT_template.in: Template input file for Impact.
- Impact simulation archives referenced in Impact_Filenames.yaml.
Dependencies:
- numpy, pandas, yaml, os, datetime, impact, pmd_beamphysics
- Data_Standard_2 (providing DataPoint2, SimulatedDataPoint2)
Output:
- HDF5 files for each processed simulation in './Test_Sim_Data2/'
- A summary table YAML file at './Test_Sim_Data2/summary_table.yaml'
"""

import numpy as np


from Data_Standard_2 import DataPoint2, SimulatedDataPoint2

import pandas as pd
import os
import yaml
import impact
import datetime
from pmd_beamphysics import ParticleGroup

# Load the list of simulation archive filenames from YAML
impact_filenames = {'impact_archive': [os.path.join('Ex_Simulation_Data2', fname) for fname in os.listdir('Ex_Simulation_Data2') if fname.endswith('.h5')]}

summary_table = []

unit_list = {'b1_gradient': 'T/m','theta0_deg': 'unitless','rf_field_scale': 'V/m','solenoid_field_scale': 'T'}
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
# Loop over all simulation archives listed in Impact_Filenames.yaml
for i in range(len(impact_filenames['impact_archive'])):

    # Load simulation archive
    I = impact.Impact()
    I.load_archive(impact_filenames['impact_archive'][i])

    # Load template YAML for comparison
    I_orig = impact.Impact.from_yaml('Lattice_Files/ImpactT.yaml')

    # Compare lattice elements to original lattice -- we will save original lattice, so only need to store changes.
    lattice_I_dict = {elem.get('name', f'idx_{i}'): elem for i, elem in enumerate(I.input['lattice'])}
    lattice_I_orig_dict = {elem.get('name', f'idx_{i}'): elem for i, elem in enumerate(I_orig.input['lattice'])}
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

    # Write Impact input file and reload to save to object
    I.write_input(input_filename='Temp.in',path='Lattice_Files/')
    with open('Lattice_Files/Temp.in', 'r') as f_input:
        input_contents = f_input.read()

    # Read lattice and template files
    with open('Lattice_Files/rfdata4', 'r') as f4, open('Lattice_Files/rfdata5', 'r') as f5, open('Lattice_Files/rfdata6', 'r') as f6, open('Lattice_Files/rfdata7', 'r') as f7, open('Lattice_Files/rfdata201', 'r') as f201, open('Lattice_Files/rfdata102', 'r') as f102, open('Lattice_Files/ImpactT.yaml', 'r') as f_yaml, open('Lattice_Files/ImpactT_template.in', 'r') as f_template:
        rfdata4_contents = f4.read()
        rfdata5_contents = f5.read()
        rfdata6_contents = f6.read()
        rfdata7_contents = f7.read()
        rfdata201_contents = f201.read()
        rfdata102_contents = f102.read()
        impact_yaml_contents = f_yaml.read()
        impact_template_contents = f_template.read()
    # Prepare summary keys for this simulation
    summary_keys = list(data_dict.keys())
    if 'norm_emit_x' in I.output['stats']:
        summary_keys.append('norm_emit_x')
    D = SimulatedDataPoint2()

    # Add scalar inputs to the data point
    scalar_inputs = {}
    for col in data_dict:
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
            "value": data_dict[col],
            "location": col,
            "units": units,
            "description": ""   # Fill in description if available
        }
    D.add_inputs(scalar_inputs=scalar_inputs)
    # Add input distribution (initial particles)
    input_dist = I.particles['initial_particles']
    if isinstance(input_dist, list):
        input_dist = input_dist[0]
    D.add_inputs(input_distribution=input_dist, input_distribution_attrs={})

    # Add lattice files to the data point
    D.add_lattice(lattice_location='included', lattice_files={
        'rfdata4': rfdata4_contents,
        'rfdata5': rfdata5_contents,
        'rfdata6': rfdata6_contents,
        'rfdata7': rfdata7_contents,
        'rfdata201': rfdata201_contents,
        'rfdata102': rfdata102_contents,
        'impactT.yaml': impact_yaml_contents,
        'impactT_template.in': impact_template_contents,
    })
    # Add summary information
    D.add_summary(
        summary_keys=summary_keys,
        summary_location='final')
    # Add run information
    D.add_run_information(source=run_info['source'], date=run_info['date'], notes=run_info['notes'])
    # Add simulation metadata
    D.add_simulation_data(
        simulation_start=I.particles['initial_particles']['mean_z'],
        simulation_end=I.particles['final_particles']['mean_z'],
        simulation_code='Impact',
        simulation_input_file=input_contents
        )

    # Add scalar outputs from stats
    for key, value in I.output['stats'].items():
        if key != 'mean_z':
            if key in output_unit_list:
                D.add_observable(location=I.output['stats']['mean_z'].tolist(), datum=value, attrs={}, units=output_unit_list[key], datum_name=key, datum_type='scalar',location_primary=False)
            else:
                D.add_observable(location=I.output['stats']['mean_z'].tolist(), datum=value, attrs={}, units='unitless', datum_name=key, datum_type='scalar',location_primary=False)
    # Add distribution outputs from particles
    for key, value in I.output['particles'].items():
        if key != 'final_particles' and key != 'initial_particles':
            D.add_observable(location=key, datum=value, attrs={}, datum_name=key, datum_type='distribution',location_primary=True)
    # Add final_particles as a distribution output
    D.add_observable(location=I.particles['final_particles']['mean_z'], datum=I.particles['final_particles'], attrs={}, datum_name='final_particles', datum_type='distribution',location_primary=False)
    # Ensure output directory exists for HDF5 files
    os.makedirs('./Test_Sim_Data2/', exist_ok=True)
    # Save the data point to HDF5
    D.saveHDF5('./Test_Sim_Data2/')
    # Add summary entry for this simulation
    entry = {
        **D.summary.summary
    }
    summary_table.append(entry)

# Write the summary table to a YAML file
with open('./Test_Sim_Data2/summary_table.yaml', 'w') as f:
    yaml.dump(summary_table, f)