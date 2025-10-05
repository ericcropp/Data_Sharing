"""
This script processes a batch of Impact simulation archives, compares their input lattice and header data to a reference YAML file,
extracts differences, and saves simulation results in a standardized HDF5 format. It also generates a summary table of key data points.

General Comments:
- The script expects a YAML file ('Impact_Filenames.yaml') listing Impact archive filenames.
- For each archive, it loads simulation data, compares input parameters to a reference ('ImpactT.yaml'), and records differences.
- Additional simulation files (rfdata4, rfdata5, etc.) are read and included in the output.
- Results are encapsulated in SimulatedDataPoint objects and saved to disk.
- A summary table of results is exported to YAML.

Main Steps:
1. Load Impact archive filenames from YAML.
2. For each archive:
    - Load simulation data and reference input.
    - Compare lattice and header data, recording differences.
    - Read auxiliary simulation files.
    - Create and populate SimulatedDataPoint objects.
    - Save results to HDF5.
    - Update summary table.
3. Export summary table to YAML.

Dependencies:
- numpy
- pandas
- yaml
- impact (custom module)
- Data_Standard (custom module)
- os

Note:
- The script assumes the existence of several input files in the working directory.
- Output is saved in './Test_Sim_Data/'.

"""

import numpy as np

from Data_Standard import DataPoint, SimulatedDataPoint

import pandas as pd
import os
import yaml
import impact

with open('Impact_Filenames.yaml', 'r') as f:
    impact_filenames = yaml.safe_load(f)
summary_table = []
for i in range(len(impact_filenames['impact_archive'])):

    I = impact.Impact()
    I.load_archive(impact_filenames['impact_archive'][i])

    I_orig = impact.Impact.from_yaml('ImpactT.yaml')

    lattice_I_dict = {elem.get('name', f'idx_{i}'): elem for i, elem in enumerate(I.input['lattice'])}
    lattice_I_orig_dict = {elem.get('name', f'idx_{i}'): elem for i, elem in enumerate(I_orig.input['lattice'])}

    all_names = set(lattice_I_dict.keys()).union(lattice_I_orig_dict.keys())

    diff_dict = {}
    data_dict = {}
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

    # Compare header data and add differences to data_dict and diff_dict
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

    run_info = {'source': 'Impact simulation', 'notes': 'Test Batch'}
    run_info.update(I.output.get('run_info', {}))

    I.write_input(input_filename='Temp.in',path='.')

    with open('Temp.in', 'r') as f_input:
        input_contents = f_input.read()

    with open('rfdata4', 'r') as f4, open('rfdata5', 'r') as f5, open('rfdata6', 'r') as f6, open('rfdata7', 'r') as f7, open('rfdata201', 'r') as f201, open('rfdata102', 'r') as f102, open('ImpactT.yaml', 'r') as f_yaml, open('ImpactT_template.in', 'r') as f_template:
        rfdata4_contents = f4.read()
        rfdata5_contents = f5.read()
        rfdata6_contents = f6.read()
        rfdata7_contents = f7.read()
        rfdata201_contents = f201.read()
        rfdata102_contents = f102.read()
        impact_yaml_contents = f_yaml.read()
        impact_template_contents = f_template.read()
    summary_keys = list(data_dict.keys())
    if 'norm_emit_x' in I.output['stats']:
        summary_keys.append('norm_emit_x')
    D = SimulatedDataPoint(
        input_distribution=I.particles['initial_particles'],
        input_distribution_attrs={},
        scalar_inputs=pd.Series(data_dict),
        lattice_location='included',
        lattice={
            'rfdata4': rfdata4_contents,
            'rfdata5': rfdata5_contents,
            'rfdata6': rfdata6_contents,
            'rfdata7': rfdata7_contents,
            'rfdata201': rfdata201_contents,
            'rfdata102': rfdata102_contents,
            'impactT.yaml': impact_yaml_contents,
            'impactT_template.in': impact_template_contents,
        },
        summary_keys=summary_keys,
        run_information=run_info,
        simulation_start=I.particles['initial_particles']['mean_z'],
        simulation_end=I.particles['final_particles']['mean_z'],
        simulation_code='Impact',
        simulation_input_file=input_contents
        )
    os.makedirs('./Test_Sim_Data/', exist_ok=True)
    for key, value in I.output['stats'].items():
        if key != 'mean_z':
            D.add_data(location=I.output['stats']['mean_z'], datum=value, attrs={}, datum_name=key, datum_type='scalar')
    for key, value in I.output['particles'].items():
        if key != 'final_particles' and key != 'initial_particles':
            D.add_data(location=key, datum=value, attrs={}, datum_name=key, datum_type='ParticleGroup')

    # I.output['run_info']
    D.add_data(location=I.particles['final_particles']['mean_z'], datum=I.particles['final_particles'], attrs={}, datum_name='final_particles', datum_type='ParticleGroup')
    D.saveHDF5('./Test_Sim_Data/')
    # print(data_dict)
    def to_python_type(val):
    # import numpy as np
    # import pandas as pd
        if isinstance(val, dict):
            return {k: to_python_type(v) for k, v in val.items()}
        elif isinstance(val, (list, tuple)):
            return [to_python_type(v) for v in val]
        elif isinstance(val, pd.Series):
            return to_python_type(val.to_dict())
        elif isinstance(val, pd.DataFrame):
            return val.to_dict(orient='list')
        elif isinstance(val, np.ndarray):
            # Convert to list and recursively process
            return [to_python_type(v) for v in val.tolist()]
        elif isinstance(val, np.generic):
            return to_python_type(val.item())
        elif isinstance(val, (bytes, np.bytes_)):
            return val.decode('utf-8')
        else:
            return val

    # Usage for summary_table:
    entry = to_python_type(D.summary)
    entry.update(to_python_type(D.run_information))
    summary_table.append(entry)

with open('./Test_Sim_Data/summary_table.yaml', 'w') as f:
    yaml.dump(summary_table, f)