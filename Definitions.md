Definition of a "run": a run is defined as a single shot (or series of shots with the same input settings) experimentally, i.e. a shot on the accelerator.  Similarly, a single simulation run.

"Batch": A series of experimental runs or simulation runs "that go together."  This is not a well-defined concept, yet and the only requirement is that they have the same lattice file.

"Lattice": This is the definition of the simulation or experimental configuration, i.e. lattice.  It generally contains the default settings.  THE LATTICE IS DEFINED ON A PER-BATCH BASIS

"Inputs": These are changes to the lattice and are DEFINED ON A PER-RUN BASIS.  These override the lattice settings

"Outputs": Per-run measurements that are recorded.

# Data Standard Detailed Rules

## 1. Inputs

- **Scalar Inputs**
  - Each scalar input must have: `name` (str), `value` (float), `location` (str), `units` (str), and `description` (str) (optional).
  - Units must be a recognized string or "Custom Unit". Prefixes (e.g., "k", "M") are supported.
  - All scalar inputs are stored as a dictionary keyed by name.
  - Scalar inputs must not be blank unless explicitly allowed.

- **Input Distribution**
  - Must be either a 2D numpy array (image) or a `ParticleGroup`.
  - If an image, `input_distribution_attrs` must include `pixel_calibration`.
  - If blank, both the distribution and its attributes must be empty or None.

## 2. Lattice

- **Lattice Location**
  - Must be a non-empty string.
  - If set to `'included'`, `lattice_files` must be provided.

- **Lattice Files**
  - Can be a list of file paths (strings) or a dictionary mapping filenames to contents.
  - If a list, each file must exist and be readable as text.
  - If a dict, keys are filenames and values are file contents (strings).

## 3. Outputs

- Each output is a `SingleOutput` with:
  - `location`: string, number (i.e. s position), or list of these.
  - `datum`: value(s), type depends on `datum_type`, must have same length as location list.
  - `attrs`: dictionary of additional attributes (optional).
  - `datum_name`: string.
  - `datum_type`: one of `'scalar'`, `'image'`, or `'distribution'`.
  - `units`: required for `'scalar'` type.  Same rules as for inputs
- For `'scalar'` outputs, datum must be a float/int or list/array of such.
- For `'image'` outputs, datum must be a 2D numpy array, and pixel calibration must be specified.
- For `'distribution'` outputs, datum must be a `ParticleGroup`.
- If `location` is a list, `datum` must be a list/array of the same length.

## 4. Summary

- `summary_keys` is a list of strings, each a key to extract from inputs, outputs, or run information for a top-level summary
- `summary_location` specifies where to extract summary values (e.g., `'final'` or a specific location).
- All keys must be non-empty strings.

## 5. Run Information

- Must include: `source` (str), `date` (str), and `notes` (str).
- All fields must be non-empty strings unless blank is explicitly allowed.

## 6. Simulation Metadata (for SimulatedDataPoint2)

- Must include: `simulation_start` (str), `simulation_end` (str), `simulation_code` (str), `simulation_input_file` (str).
- All fields must be non-empty strings unless blank is explicitly allowed.

## 7. DataPoint2 and SimulatedDataPoint2

- All components (inputs, lattice, outputs, summary, run information, simulation metadata) must pass their respective validation checks.
- The `ID` is generated as an MD5 hash of the scalar inputs and lattice location.
- The `finalize()` method must be called before saving to ensure all checks and summary extraction are performed.

## 8. HDF5 Serialization

- Inputs, lattice, outputs, summary, and run information are saved as groups/datasets/attributes in the HDF5 file.
- Scalar inputs are saved as datasets with attributes for metadata.
- Input distribution is saved as a dataset or group, with attributes.
- Lattice files are saved as datasets under a group.
- Outputs are saved as datasets or groups, with attributes.
- Summary and run information are saved as file-level attributes.
- Simulation metadata is saved as a dataset or group if present.

## 9. Error Handling

- Type and value checks are enforced for all fields.
- Missing or invalid data raises `TypeError`, `ValueError`, or `AssertionError` as appropriate.
- Allowing blank fields must be explicitly specified via `allow_blank=True` in checkers.

---
## 10. Batch-level HDF5 files
These are initially stored as per-run HDF5 files, but can be combined into a per-batch HDF5 file using Combine_Files.py