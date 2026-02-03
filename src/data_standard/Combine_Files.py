"""
Combine_Files.py

This script combines multiple HDF5 files (each representing a single data point) into a single HDF5 file for easier sharing and analysis. 
It scans the input directory for all .h5 files, loads each file, and copies its contents into a group within the output file named by its ID. 
The script moves the first lattice group to the root, validates that all files have identical lattices and Data_Standard_Version, and removes duplicate lattice groups.

Usage:
    python Combine_Files.py <input_dir> <output_h5>
    - <input_dir>: Directory containing individual HDF5 files to combine.
    - <output_h5>: Path to the combined output HDF5 file.

"""

import os
import h5py
import argparse
import numpy as np


def copy_file_into_group(src_file: h5py.File,
                         dst_group: h5py.Group,
                         *,
                         expand_soft=False,
                         expand_external=False,
                         expand_refs=False):
    """
    Copy the entire contents of `src_file` (root "/") into `dst_group`,
    preserving all subgroups, datasets, links, and attributes.

    - File-level (root) attributes from `src_file` are copied onto `dst_group`.
    - By default, soft/external links are preserved as links; set expand_* to
      True to inline their targets.
    """
    # Copy file-level (root) attributes onto the destination group
    for k, v in src_file["/"].attrs.items():
        dst_group.attrs.create(k, v)

    # Copy each top-level member under "/"
    root = src_file["/"]
    for name in root.keys():
        # Using src_file.copy ensures link-expansion options work properly
        src_file.copy(
            source=root[name],
            dest=dst_group,
            name=name,               # keep original name
            shallow=False,           # deep copy of entire subtrees
            expand_soft=expand_soft,
            expand_external=expand_external,
            expand_refs=expand_refs,
            without_attrs=False      # keep all attrs on groups/datasets
        )

def compare_lattice_groups(lattice1: h5py.Group, lattice2: h5py.Group, exclude_keys=None, path="") -> bool:
    """
    Compare two lattice groups to check if they are identical.
    
    Compares attributes and all datasets/subgroups recursively.
    
    Args:
        lattice1: First lattice group
        lattice2: Second lattice group
        exclude_keys: Set of keys to exclude from comparison (e.g., {'simulation_input_file'})
        path: Current path for debugging (internal use)
    
    Returns:
        bool: True if lattices are identical, False otherwise
    """
    if exclude_keys is None:
        # Exclude simulation_input_file* as it varies per simulation
        exclude_keys = set()
    
    # Compare attributes
    if set(lattice1.attrs.keys()) != set(lattice2.attrs.keys()):
        print(f"Attribute keys differ at {path}: {lattice1.attrs.keys()} vs {lattice2.attrs.keys()}")
        return False
    
    for key in lattice1.attrs.keys():
        val1 = lattice1.attrs[key]
        val2 = lattice2.attrs[key]
        if isinstance(val1, np.ndarray) and isinstance(val2, np.ndarray):
            if not np.array_equal(val1, val2):
                print(f"Attribute {key} differs at {path}")
                return False
        elif isinstance(val1, bytes) and isinstance(val2, bytes):
            if val1 != val2:
                print(f"Attribute {key} differs at {path}: {val1} vs {val2}")
                return False
        elif val1 != val2:
            print(f"Attribute {key} differs at {path}: {val1} vs {val2}")
            return False
    
    # Compare members (datasets and subgroups), excluding specified keys
    # Also exclude all simulation_input_file* datasets (varies per simulation)
    keys1 = set(k for k in lattice1.keys() if not k.startswith('simulation_input_file')) - exclude_keys
    keys2 = set(k for k in lattice2.keys() if not k.startswith('simulation_input_file')) - exclude_keys
    
    if keys1 != keys2:
        print(f"Keys differ at {path}: {keys1} vs {keys2}")
        return False
    
    for key in keys1:
        item1 = lattice1[key]
        item2 = lattice2[key]
        
        # Both must be same type (group or dataset)
        if isinstance(item1, h5py.Group) != isinstance(item2, h5py.Group):
            return False
        
        if isinstance(item1, h5py.Group):
            # Recursively compare subgroups
            if not compare_lattice_groups(item1, item2, exclude_keys, path=f"{path}/{key}"):
                return False
        else:
            # Compare datasets
            data1 = item1[()]
            data2 = item2[()]
            
            if isinstance(data1, np.ndarray) and isinstance(data2, np.ndarray):
                if not np.array_equal(data1, data2):
                    print(f"Dataset {key} differs at {path}: shapes {data1.shape} vs {data2.shape}")
                    return False
            elif isinstance(data1, bytes) and isinstance(data2, bytes):
                if data1 != data2:
                    print(f"Dataset {key} (bytes) differs at {path}")
                    return False
            elif data1 != data2:
                print(f"Dataset {key} differs at {path}: {data1} vs {data2}")
                return False
            
            # Compare dataset attributes
            if set(item1.attrs.keys()) != set(item2.attrs.keys()):
                print(f"Dataset {key} attribute keys differ at {path}")
                return False
            for attr_key in item1.attrs.keys():
                if item1.attrs[attr_key] != item2.attrs[attr_key]:
                    print(f"Dataset {key} attribute {attr_key} differs at {path}")
                    return False
    
    return True


def combine_files(input_dir: str, output_h5: str):
    """
    Combine all HDF5 files in input_dir into a single HDF5 file.
    
    Args:
        input_dir: Directory containing individual .h5 files to combine
        output_h5: Path to output combined HDF5 file
    """
    # Find all .h5 files in the directory
    h5_files = [f for f in os.listdir(input_dir) 
                if f.endswith('.h5') and f != os.path.basename(output_h5)]
    
    if not h5_files:
        print(f"No HDF5 files found in {input_dir}")
        return
    
    print(f"Found {len(h5_files)} HDF5 files to combine")
    
    # Assume summary is a list of dicts with 'id' and 'filename' keys
    combined_data = {}
    ids = []
    
    with h5py.File(output_h5, "w") as out_f:
        for h5_file in h5_files:
            # Extract ID from filename (remove .h5 extension)
            file_id = os.path.splitext(h5_file)[0]
            ids.append(file_id)
            
            file_path = os.path.join(input_dir, h5_file)
            print(f"Processing: {h5_file}")
            
            # Load data from each file
            g = out_f.require_group(file_id)

            with h5py.File(file_path, "r") as f:
                copy_file_into_group(f, g)


        # Write combined data to output HDF5
        # Add summary YAML as a top-level group
        top_groups = [name for name in out_f.keys() if isinstance(out_f[name], h5py.Group)]
        i = 0
        first_group_name = None
        data_standard_version = None
        first_lattice = None
        
        for grp_name in top_groups:
            # Check Data_Standard_Version consistency
            grp = out_f[grp_name]
            if 'Data_Standard_Version' in grp.attrs:
                version = grp.attrs['Data_Standard_Version']
                if data_standard_version is None:
                    data_standard_version = version
                else:
                    assert version == data_standard_version, \
                        f"Data_Standard_Version mismatch: {grp_name} has '{version}' but expected '{data_standard_version}'"
            
            # Check lattice consistency
            if 'lattice' in grp:
                current_lattice = grp['lattice']
                if first_lattice is None:
                    first_lattice = current_lattice
                else:
                    assert compare_lattice_groups(first_lattice, current_lattice), \
                        f"Lattice mismatch: {grp_name} has different lattice than {first_group_name}. Cannot combine files with different lattices."
            
            if i == 0:
                first_group_name = grp_name
                out_f.move(grp_name + '/lattice', '/lattice')
                
                # Remove simulation_input_file* datasets from lattice (they vary per simulation)
                lattice_grp = out_f['lattice']
                sim_input_keys = [k for k in lattice_grp.keys() if k.startswith('simulation_input_file')]
                for key in sim_input_keys:
                    del lattice_grp[key]
                
                # Store Data_Standard_Version at root level (now that we've verified all match)
                if data_standard_version is not None:
                    out_f.attrs['Data_Standard_Version'] = data_standard_version
                        
                # Store list of IDs as root attribute
                out_f.attrs['IDs'] = ids
            else:
                del out_f[grp_name + '/lattice']
            i += 1
        
        print(f"Successfully combined {len(ids)} files into {output_h5}")

        # Store the summary as a single attribute table on the summary_yaml group
        # Write summary information as attributes
        # shots_per_id_stored = False
        # for key in summary[0].keys():
        #     if key == 'ID':
        #         out_f.attrs['IDs'] = [entry.get('ID') for entry in summary]
        #     else:
        #         # Collect all values for this key
        #         values = [entry.get(key) for entry in summary]
                
        #         # Check if all values are lists
        #         if all(isinstance(v, list) for v in values):
        #             # Flatten all lists into a single list and track shots per ID (once)
        #             flattened = []
        #             if not shots_per_id_stored:
        #                 shots_per_id = []
        #             for val_list in values:
        #                 if not shots_per_id_stored:
        #                     shots_per_id.append(len(val_list))
        #                 flattened.extend(val_list)
        #             try:
        #                 out_f.attrs[key] = np.array(flattened)
        #             except:
        #                 out_f.attrs[key] = flattened
        #             # Store shots per ID once for all list-valued keys
        #             if not shots_per_id_stored:
        #                 out_f.attrs["shots_per_ID"] = shots_per_id
        #                 shots_per_id_stored = True
        #         elif all(isinstance(v, (int, float)) for v in values):
        #             # Numeric values - store as array
        #             try:
        #                 out_f.attrs[key] = np.array(values)
        #             except:
        #                 out_f.attrs[key] = values
        #         else:
        #             # Store as list (handles strings and mixed types)
        #             out_f.attrs[key] = values

    # print(f"Combined file written to {output_h5}")

def main():
    parser = argparse.ArgumentParser(description="Combine files listed in summary_yaml into a single HDF5 file.")
    parser.add_argument("input_dir", help="Directory containing summary_yaml and data files")
    parser.add_argument("output_h5", help="Output HDF5 file path")
    args = parser.parse_args()

    combine_files(args.input_dir, args.output_h5)

    

if __name__ == "__main__":
    main()

#TO DO: Make a script to undo this process and separate the combined file back into individual files.