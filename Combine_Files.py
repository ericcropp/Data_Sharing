"""
Combine_Files.py

This script combines multiple HDF5 files (each representing a single data point) into a single HDF5 file for easier sharing and analysis. 
It reads a summary YAML file (summary_table.yaml) that lists the IDs of the files to combine, loads each corresponding HDF5 file, and copies its contents into a group within the output file named by its ID. 
The script also moves the first lattice group to the root and removes duplicate lattice groups, and stores the summary table as attributes in a top-level group ("summary_yaml") in the combined file.

Usage:
    python Combine_Files.py <input_dir> <output_h5>
    - <input_dir>: Directory containing summary_table.yaml and the individual HDF5 files.
    - <output_h5>: Path to the combined output HDF5 file.

"""

import os
import yaml
import h5py
import argparse


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

def main():
    parser = argparse.ArgumentParser(description="Combine files listed in summary_yaml into a single HDF5 file.")
    parser.add_argument("input_dir", help="Directory containing summary_yaml and data files")
    parser.add_argument("output_h5", help="Output HDF5 file path")
    args = parser.parse_args()

    summary_path = os.path.join(args.input_dir, "summary_table.yaml")
    if not os.path.exists(summary_path):
        print(f"summary_table.yaml not found in {args.input_dir}")
        return

    with open(summary_path, "r") as f:
        summary = yaml.safe_load(f)

    # Assume summary is a list of dicts with 'id' and 'filename' keys
    combined_data = {}
    with h5py.File(args.output_h5, "w") as out_f:
        for entry in summary:
            file_id = entry.get("ID")
            if not file_id:
                print(f"Skipping entry with missing id: {entry}")
                continue
            file_path = os.path.join(args.input_dir, str(file_id) + '.h5')
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                continue
            # Load data from each file (assuming HDF5 for this example)
            g = out_f.require_group(file_id)

            with h5py.File(file_path, "r") as f:

                copy_file_into_group(f, g)
                # # Copy all datasets under a group named by file_id
                # combined_data[file_id] = {}
                # def copy_datasets(name, obj):
                #     if isinstance(obj, h5py.Dataset):
                #         combined_data[file_id][name] = obj[()]
                #     elif isinstance(obj, h5py.Group):
                #         # Copy group-level attributes
                #         if "attrs" not in combined_data[file_id]:
                #             combined_data[file_id]["attrs"] = {}
                #         combined_data[file_id]["attrs"][name] = dict(obj.attrs)
                #     elif isinstance(obj, h5py.Dataset):
                #         # Copy dataset-level attributes
                #         if "dset_attrs" not in combined_data[file_id]:
                #             combined_data[file_id]["dset_attrs"] = {}
                #         combined_data[file_id]["dset_attrs"][name] = dict(obj.attrs)
                # f.visititems(copy_datasets)

    # Write combined data to output HDF5
    
        # for file_id, datasets in combined_data.items():
        #     grp = out_f.create_group(file_id)
        #     for dset_name, data in datasets.items():
        #         grp.create_dataset(dset_name, data=data)
        # Add summary YAML as a top-level group
        top_groups = [name for name in out_f.keys() if isinstance(out_f[name], h5py.Group)]
        i = 0
        for grp_name in top_groups:
            if i == 0:
                out_f.move(grp_name + '/lattice', '/lattice')
            else:
                del out_f[grp_name + '/lattice']  # TO DO: Check that lattice is the same for all files
            i += 1

        summary_grp = out_f.create_group("summary_yaml")
        # Store the summary as a single attribute table on the summary_yaml group
        for key in summary[0].keys():
            summary_grp.attrs[key] = [entry.get(key) for entry in summary]

    print(f"Combined file written to {args.output_h5}")

if __name__ == "__main__":
    main()

#TO DO: Make a script to undo this process and separate the combined file back into individual files.