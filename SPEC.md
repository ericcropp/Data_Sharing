# Data Standard Specification

**Version:** 0.1.2 (2026-10-16)  
**Document Status:** Complete Technical Specification

---

## Table of Contents

- [Overview](#overview)
- [Core Principles](#core-principles)
  - [1. Observable-Centric Design](#1-observable-centric-design)
  - [2. Flexible Dimensionality](#2-flexible-dimensionality)
  - [3. Location-Based Organization](#3-location-based-organization)
  - [4. ParticleGroup & Data Support](#4-particlegroup--data-support)
- [Definitions](#definitions)
- [HDF5 File Structure](#hdf5-file-structure)
- [Detailed Specification](#detailed-specification)
  - [Root Level Structure](#root-level-structure)
  - [Lattice Group](#lattice-group)
  - [Batch Groups](#batch-groups)
  - [Observables Group](#observables-group)
    - [Pattern 1: Location-Primary Storage](#pattern-1-location-primary-storage)
    - [Pattern 2: Multi-Location Storage](#pattern-2-multi-location-storage)
  - [Timestamp Observable](#timestamp-observable)
  - [Batch Broadcasting](#batch-broadcasting)
  - [ParticleGroup Storage](#particlegroup-storage)
  - [Data Type Requirements](#data-type-requirements)
  - [Dimension Validation](#dimension-validation)
  - [Unit Handling](#unit-handling)
- [Validation Rules](#validation-rules)
- [Validating Files](#validating-files)
- [Changes in 0.1.2](#changes-in-012)
- [Implementation Notes](#implementation-notes)

---

## Overview

This document defines a standardized format for storing accelerator physics data in HDF5 files. The standard supports both experimental and simulation data, with flexible dimensionality for batch processing and feature-rich observables.

**Key Features:**
- Hierarchical HDF5 storage with rich metadata
- Support for scalar, vector, and multi-dimensional observables
- Integration with openPMD-beamphysics ParticleGroup format
- Flexible location-based or multi-location data organization
- Units validated against OpenPMD units, but stored as written, including SI prefixes (e.g. `mW`, `pC`)
- Optional per-shot ISO 8601 timestamps
- Summary metadata for dataset discovery and filtering

**Defining Implementation:**
- `src/data_standard/Data_Standard_2.py`: Classes that generate intermediate files 
- `src/data_standard/Combine_Files.py`: Utility for finalizing a single HDF5 file that is compliant with the data standard defined below. Automatically appends the Data Standard version to the output filename (e.g., `filename_v0.1.2.h5`).
- `src/data_standard/validate.py`: Standalone validator for in-memory data points and HDF5 files (see [Validating Files](#validating-files)).
- Three examples are in `examples/`.  See `README.md` for how to run those examples.

---

## Core Principles

### 1. Observable-Centric Design
All physical quantities are stored as **observables** with associated metadata:
- `data_name`: Identifier for the observable. For experimental data this is the **full PV**
  (e.g. `BPMS:IN10:781:TMIT`, not `TMIT`); for simulation data the full simulation parameter
  name (e.g. `CQ10121:b1_gradient`) plays the role of the PV. `Timestamp` is reserved.
- `units`: Physical units, stored as written including any SI prefix (e.g. `uW`, `mW`)
- `location`: Where the measurement/simulation occurred (typically a lattice element name)
- `read_only`: Boolean; `True` for measured/read-back quantities, `False` for settable
  (control/input) parameters. Replaces the legacy `control` flag (`read_only = not control`).  
  The legacy `control` flag is still accepted by the validator, but is converted to `read_only` 
  with opposite value if written using the tooling shown herein.
- `batch_dims`: Tuple defining batch structure
- `num_feature_dims`: Integer count of feature dimensions

### 2. Flexible Dimensionality
Data can have multiple dimension types:
- **Batch dimensions** (`batch_dims`): Independent experimental/simulation runs. A batch
  dimension of size 1 in the data is **broadcast** to every shot, so data that is constant
  across a scan (e.g. an input image or ParticleGroup) is stored only once
  (see [Batch Broadcasting](#batch-broadcasting)).
- **Feature dimensions** (`num_feature_dims`): Intrinsic data structure (images, spectra, etc.)
- **Location dimensions**: Multiple observation points along beamline

### 3. Location-Based Organization
Two storage patterns:
- **`location_primary=True`**: Data grouped by location. A location with several observables
  (or with per-shot ParticleGroups) is an HDF5 group holding them; a location with exactly one
  observable stored as a single HDF5 entry is not given a group — the observable is stored
  directly under `observables/` and its location is recorded in `@location`. Every
  location-primary entry carries `@location`, whether nested or not.
- **Multi-location** (`location_primary=False` / `location_group`): Data grouped by type with a
  shared location array (for example a simulation dataset where outputs are generated at many
  points throughout the beamline). A data point may hold several such groups, named
  `multi_location_data` or `multi_location_data_<suffix>` (e.g. `multi_location_data_impact`,
  `multi_location_data_bmad`), each with its own location array.

### 4. ParticleGroup & Data Support
- **Scalar/array data**: Stored as HDF5 datasets
- **ParticleGroups**: Stored using openPMD-beamphysics native format

---

## Definitions

**Shot:** A single run of the accelerator or simulation code

**Batch:** An N-dimensional set of shots (i.e. scanning a single quadrupole from -5 T/m to 5 T/m in 1 T/m steps is a 1-D batch). This standard can accommodate any dimension, including 0 → a single data point.

**Lattice:** Configuration defining the accelerator beamline elements and their properties.  All observables' locations must be specified by this lattice.  

**Observable:** A quantity measured or computed at specific location(s) along the beamline.  Notably, these can be inputs or outputs (specify with the read_only boolean; see below)

**PV (Process Variable):** The control-system name of a single quantity (e.g. the EPICS PV `BPMS:IN10:781:TMIT`). Observables are named by their full PV; for simulations, the full simulation parameter name is used instead.

**Feature dimensions:** Intrinsic dimensions of the data beyond batch and location (e.g., pixels in an image, bins in a spectrum)

**Broadcasting:** A batch dimension of size 1 in an observable's data applies to every shot along that batch dimension (numpy broadcasting semantics).

---

## HDF5 File Structure

Almost always, there will be multiple shots per batch.  These shots must share a lattice and some logical connection (flexibility left to the end user).  The HDF5 file can also contain multiple batches per file (they must share a lattice).  The choice of how to group the shots (in the same batch, the same file, or completely separate file) is left up to the user and all choices can comply with this data standard.  The authoritative requirements of HDF5 files in this data standard are listed below.    

To comply with this standard, the data must be stored in hierarchical data format (HDF5).  Here is a diagram of such a file:

The HDF5 file structure for combined files follows this hierarchy:

```
<filename>.h5
├── @Data_Standard_Version                  # Root attribute: Version that all data corresponds to
├── @IDs                                    # Root attribute: Array of all data point IDs
│
├── lattice/                                # Shared lattice at root level
│   ├── @lattice_location                   # Group attribute: "included" or "external_reference"
│   ├── lattice_mapping                     # Dataset: 2xn table mapping PVs to lattice elements (optional)
│   └── lattice_files/                      # Subgroup containing lattice files
│       └── <filename>                      # Datasets storing file contents
│
└── <ID>/                                   # One group per data point
    ├── @Data_Standard_Version              # Group attribute: Version for this data point
    ├── @ID                                 # Group attribute: Unique identifier (hash)
    ├── @batch_dims                         # Group attribute: Tuple of batch dimension sizes
    ├── @run_information_source             # Group attribute: Data source (e.g., "FACET-II")
    ├── @run_information_date               # Group attribute: Date in YYYY-MM-DD format
    ├── @run_information_notes              # Group attribute: Optional notes
    ├── @simulation_code                    # Group attribute: (Simulation only) Code name
    ├── @simulation_version                 # Group attribute: (Simulation only) Code version
    ├── @simulation_start                   # Group attribute: (Simulation only) Start position
    ├── @simulation_end                     # Group attribute: (Simulation only) End position
    │
    └── observables/
        ├── <PV>                            # Dataset: only observable at its location (flattened)
        │   ├── @location                   # Dataset attribute: Location name
        │   └── ...                         # Same attributes as nested datasets below
        ├── <PV>/                           # Single ParticleGroup: only observable at its location
        │   └── @location                   # Group attribute: Location name
        ├── Timestamp                       # Optional dataset: ISO 8601 strings, shape batch_dims
        │
        ├── <location_name>/                # Group: several observables, or per-shot ParticleGroups
        │   ├── <PV>                        # Dataset: batch_dims + feature_dims (size-1 batch dims broadcast)
        │   │   ├── @location               # Dataset attribute: Location name (= group name)
        │   │   ├── @read_only              # Dataset attribute: Boolean: measured (True) / settable (False)
        │   │   ├── @num_feature_dims       # Dataset attribute: Integer: feature dimensions
        │   │   ├── @units                  # Dataset attribute: Unit string incl. prefix (e.g., "mm", "pC")
        │   │   ├── @bin_size               # Dataset attribute: (Required if num_feature_dims > 0)
        │   │   └── @offset                 # Dataset attribute: (Required if num_feature_dims > 0)
        │   └── <PV>_<i>[_<j>...]/          # ParticleGroup (one per batch element), or <PV>/ if single
        │       └── @location               # Group attribute: Location name (= group name)
        │
        └── multi_location_data[_<suffix>]/ # Multi-location storage (one or more groups)
            ├── DATA_LOCATIONS              # Dataset: location array (1-D, shared by all shots)
            │   ├── @units                  # Dataset attribute: Location units
            │   └── @num_feature_dims       # Dataset attribute: Always 0 for locations
            │
            └── <observable_name>           # Dataset: batch_dims + (locations,) + feature_dims
                ├── @read_only              # Dataset attribute: Boolean
                ├── @num_feature_dims       # Dataset attribute: Integer: feature dimensions
                ├── @units                  # Dataset attribute: Unit string incl. prefix
                ├── @bin_size               # Dataset attribute: (Required if num_feature_dims > 0)
                └── @offset                 # Dataset attribute: (Required if num_feature_dims > 0)
```

---

## Detailed Specification

### Root Level Structure

**Root Attributes:**

#### @Data_Standard_Version
- **Type:** String
- **Format:** X.Y.Z
- **Purpose:** Identifies which version of this standard was used to create the file
- **Rule:** All batches in a combined file must use the same version

#### @IDs
- **Type:** Array of strings
- **Requirement:** All IDs must be unique within the file
- **Purpose:** List of all batch identifiers in the file
- **Rule:** Length must match number of batches groups in file

### Lattice Group

**Location:** `/lattice/`

**Group Attributes:**

#### @lattice_location
- **Type:** String
- **Allowed Values:** `null`, `""`, `"included"`, or a specification to a path (URL, filepath, etc)
- **Requirement:** Must be one of the four allowed values
- **Rule:** When `"included"`, `lattice_files/` subgroup must exist and be non-empty
- **Rule:** When an external reference is given, no file datasets should be stored
- **Rule:** All batches in a combined file must share the same lattice_location value and lattice configuration

**Subgroups:**

#### lattice_files/
- **Purpose:** Contains all lattice configuration files
- **Requirement:** Present and non-empty when `@lattice_location = "included"`
- **File Content Type:** String or bytes
- **Content:** Files are stored verbatim. This standard does not define a lattice format, and
  validation only checks that the files are present; their contents are not interpreted.
- **Rule:** All batches in a combined file must have identical lattice files

**Optional Datasets:**

#### lattice_mapping
- **Type:** 2D array of byte strings, shape (2, n)
- **Purpose:** Maps control system names (often EPICS PVs) to lattice element names for experimental data
- **Requirement:** Optional; typically used with experimental data from accelerator facilities
- **Format:** 2xn array where:
  - Row 0: EPICS PV names (e.g., `[b'SOLN:IN10:121', b'QUAD:IN10:121', ...]`)
  - Row 1: Corresponding lattice element names (e.g., `[b'SOL10121', b'CQ10121', ...]`)
- **Usage:** Enables correlation between control system variables and beamline model elements
- **Example:** Column 0: `['SOLN:IN10:121', 'SOL10121']` maps PV to lattice element
- **Access Pattern:** `pv_name = lattice_mapping[0, i]`, `lattice_name = lattice_mapping[1, i]`

### Batch Groups

**Location:** `/<ID>/`
**Naming:** Each data point is a group named by its unique ID

**Group Attributes:**

#### @Data_Standard_Version
- **Type:** String
- **Format:** X.Y.Z
- **Requirement:** Must match root @Data_Standard_Version in combined files

#### @ID
- **Type:** String
- **Requirement:** Must be non-empty and unique within a file
- **Reference implementation:** 32-character hexadecimal MD5 hash of every observable's
  location, data_name, data shape, data (numeric data rounded to 10 decimals; strings such as
  timestamps hashed as text; ParticleGroups excluded) and units, plus the lattice location
- **Combined files:** the data-point group is named after the ID

#### @batch_dims
- **Type:** Tuple/Array of positive integers
- **Stored as:** Numpy array in HDF5 (empty array for single point)
- **Requirement:** All elements must be positive integers > 0
- **Rule:** Product of batch_dims must equal number of data points in batch
- **Empty tuple:** `()` indicates single data point (not batched)

#### @run_information_source
- **Type:** String
- **Requirement:** Non-empty string
- **Purpose:** Describes data source (e.g., "FACET-II", "AWA", "Impact-T simulation")

#### @run_information_date
- **Type:** String  
- **Format:** YYYY-MM-DD or empty string
- **Requirement:** Must match pattern `^\d{4}-\d{2}-\d{2}$` if non-empty

#### @run_information_notes
- **Type:** String
- **Requirement:** Can be empty
- **Purpose:** Additional notes or description

**Simulation-Only Attributes:**

#### @simulation_code
- **Type:** String
- **Requirement:** Required and non-empty for simulated data
- **Examples:** "Impact-T", "elegant", "Astra"

#### @simulation_version
- **Type:** String
- **Requirement:** Required and non-empty for simulated data

#### @simulation_start
- **Type:** Numeric (int or float)
- **Requirement:** Required for simulated data
- **Rule:** Must be less than @simulation_end

#### @simulation_end
- **Type:** Numeric (int or float)
- **Requirement:** Required for simulated data
- **Rule:** Must be greater than @simulation_start

### Observables Group

**Location:** `/<ID>/observables/`

Two storage patterns are supported:

#### Pattern 1: Location-Primary Storage

**Structure:**
- Location with **one** observable stored as a single HDF5 entry (a dataset, or a single
  ParticleGroup): `/<ID>/observables/<PV>` (flattened; location in `@location`)
- Location with **several** observables, or with per-shot ParticleGroups
  (`<PV>_<i>[_<j>...]`): `/<ID>/observables/<location_name>/...`

**Use Case:** Single location per observable (BPMs, screens, individual diagnostics)

**Example (FACET-II):** BPM `BPM10781` reads three PVs, so they share a group:
`observables/BPM10781/BPMS:IN10:781:TMIT`, `observables/BPM10781/BPMS:IN10:781:X`, ...
A laser power meter with a single PV is stored as `observables/LASR:LT10:930:PWR` with
`@location = "LASR:LT10:930:PWR"` (or its lattice element name, if mapped).

**Requirements:**
- Location must be list/array with exactly one element
- Observable name (`data_name`) must be a non-empty string without `/`; for experimental data
  it should be the full PV
- Names must not collide: a flattened observable may not share its name with a location
  group or another flattened observable; names within a location group must be unique
- The prefix `multi_location_data` is reserved for multi-location groups: no location name or
  `data_name` may start with it

**Dataset Shape:** `batch_dims + feature_dims`, where each batch dimension may be 1 (broadcast)

**Dataset Attributes:**

##### @location
- **Type:** String
- **Requirement:** Required on every location-primary dataset and ParticleGroup group, nested
  or flattened. When nested, must match the parent group name. (Optional only for `Timestamp`.)
- **Purpose:** Explicit location identification; for flattened observables it is the only
  record of the location

##### @read_only
- **Type:** Boolean (True/False)
- **Requirement:** Must be boolean type, not truthy/falsy value
- **Purpose:** `True` for measured/read-back quantities (outputs), `False` for settable
  control parameters (inputs)
- **Legacy:** Files written before 0.1.2 store `@control` (= `not read_only`) instead. Readers
  and validators must accept `@control` when `@read_only` is absent; if both are present they
  must be consistent. New files store only `@read_only`.

##### @num_feature_dims
- **Type:** Integer
- **Requirement:** Must be >= 0
- **Values:**
  - 0: Scalar value
  - 1: 1D array (waveform, spectrum)
  - 2: 2D array (image)
  - 3+: Higher dimensional data

##### @units
- **Type:** String
- **Requirement:** Should be a unit recognized by openPMD-beamphysics, optionally with an SI
  prefix; custom units are allowed
- **Format:** Stored exactly as written, **including the SI prefix** (e.g., `"uW"`, `"mW"`,
  `"pC"`, `"mm"`). Data values are in these units.
- **Dimensionless:** Use "1" for unitless/dimensionless quantities
- **Custom Units:** Non-recognized units (e.g., "unitless", "counts") are stored as-is
- **ParticleGroup Data:** Units attribute is not stored for ParticleGroup datasets; units are embedded in the ParticleGroup's own metadata

##### @unit_multiplier (legacy)
- No longer written (since 0.1.2). Files written before 0.1.2 stored the base unit in `@units`
  (e.g. `"C"`) and the prefix factor here (e.g. `1e-12`). Validators accept it (must be > 0)
  and report it as a legacy construct.

**Additional Attributes (when num_feature_dims > 0):**

When `num_feature_dims > 0`, the following attributes are **required** and must be provided in the `attrs` dictionary parameter when calling `add_observable()`:

##### @bin_size
- **Type:** Float, or String (the `data_name` of another observable, used as a key)
- **Requirement:** Required when num_feature_dims > 0; must be specified in attrs dict
- **Purpose:** Physical size of one bin/pixel in feature space
- **Units:** Match observable units
- **Note:** Can be positive or negative (negative for reversed coordinate axes)
- **Per-shot bin sizes:** When the bin size differs across stacked/batched images, `bin_size` may be a string naming another observable (a key) instead of a float. The referenced observable must exist and its data must have dimension `batch_dims` (one bin size per batch element), i.e. its data shape must equal `batch_dims` (size-1 dimensions allowed, see [Batch Broadcasting](#batch-broadcasting)).
- **Usage:** `attrs={'bin_size': 1e-6, 'offset': 0.0}` or `attrs={'bin_size': 'PixelResolution', 'offset': 0.0}`

##### @offset
- **Type:** Float
- **Requirement:** Required when num_feature_dims > 0; must be specified in attrs dict
- **Purpose:** Physical coordinate of first bin/pixel
- **Units:** Match observable units
- **Can be:** Any float
- **Coordinate calculation:** `coord[i] = offset + i * bin_size`
- **Usage:** `attrs={'bin_size': 1e-6, 'offset': 0.0}`

#### Pattern 2: Multi-Location Storage

**Structure:** `/<ID>/observables/multi_location_data/` and/or
`/<ID>/observables/multi_location_data_<suffix>/`
**Use Case:** Multiple observation points along beamline with shared location array

**Groups:**
- A data point may contain any number of multi-location groups. Each is named
  `multi_location_data` or `multi_location_data_<suffix>` with a non-empty suffix
  (e.g. `multi_location_data_impact`, `multi_location_data_bmad`).
- Group names are unique (as HDF5 requires). Each group is self-contained: it has its own
  `DATA_LOCATIONS` and the rules below apply **within** each group; different groups may use
  different locations, location units and `num_feature_dims`.
- **API:** `add_observable(..., location_group='multi_location_data_impact')`;
  `location_primary=False` alone uses `multi_location_data`.

**Special Dataset:**

##### DATA_LOCATIONS
- **Type:** Numeric array
- **Shape:** `(num_locations,)` — shared by all shots in the data point
- **Purpose:** Shared location coordinates for all observables in this group (a position,
  or another coordinate such as time for time-stepped simulations)
- **Attributes:**
  - `@units`: Location units (e.g. "m" for position along beamline, "s" for time)
  - `@num_feature_dims`: Always 0 (locations are scalar coordinates)
- **Requirement:** All observables in the group must share identical DATA_LOCATIONS
- **Requirement:** location_units must be specified

**Observable Datasets:**

**Shape:** `batch_dims + (num_locations,) + feature_dims` (size-1 batch dimensions broadcast)

**Dataset Attributes:**

Same as location-primary storage except:
- No `@location` attribute (shared via DATA_LOCATIONS)
- Location dimension inserted between batch and feature dimensions (omitted when
  DATA_LOCATIONS has a single entry)
- All observables in the group must have the same `num_feature_dims`

### Timestamp Observable

Per-shot timestamps are **optional**. When present they are stored as an observable with the
reserved name `Timestamp` that follows all other observable rules.

**Structure:** `/<ID>/observables/Timestamp` (or `/<ID>/observables/<location_name>/Timestamp`
if a location is given that is shared with other observables)

- **Shape:** `batch_dims` (one timestamp per shot; a scalar for `batch_dims = ()`)
- **Data type:** UTF-8 variable-length strings
- **Format:** ISO 8601-1:2019 extended-format calendar date and time of day, with seconds,
  an optional decimal fraction, and a **mandatory** UTC designator or offset:
  `YYYY-MM-DDThh:mm:ss[.f…](Z|±hh:mm)`
  - Examples: `2026-10-16T13:55:40-07:00`, `2026-10-16T13:55:40.050-07:00`, `2026-10-16T20:55:40.050Z`
  - Not allowed: dates without times, times without an offset, basic format (`20261016T135540Z`),
    a space instead of `T`
- **Attributes:** `@read_only = True`, `@num_feature_dims = 0`, `@units = ""`;
  `@location` is optional
- **Rules:** at most one `Timestamp` per data point; never stored in a multi-location group
- **API:** `DataPoint2.add_timestamp(data, batch_dims=None, location=None)` accepts ISO 8601
  strings or timezone-aware `datetime` objects

### Batch Broadcasting

Every observable declares the data point's full `batch_dims`, but its data may use size 1 in any
batch dimension. A size-1 dimension applies to every shot along that dimension (numpy
broadcasting). This avoids storing copies of data that does not change during a scan — most
importantly large inputs such as images and ParticleGroups.

- **Data shape:** `b + (num_locations,) + feature_dims`, where `len(b) == len(batch_dims)` and each
  `b[i]` is `batch_dims[i]` or `1`. The batch rank is always kept, so batch dimensions can be
  told apart from feature dimensions.
- **Applies to:** every observable type — scalars, waveforms/images, multi-location data,
  ParticleGroups and `Timestamp`. `DATA_LOCATIONS` is always 1-D (shared by all shots).
- **No marker attribute:** constancy is implied by the shape alone.
- **Reading:** `np.broadcast_to(data, batch_dims + data.shape[len(batch_dims):])` gives one value per shot.

**Examples** (`batch_dims = (5,)` unless stated):
```
scalar varying per shot             shape (5,)
scalar constant over the scan       shape (1,)
image constant over the scan        shape (1, H, W)
batch_dims = (3, 4), constant along the first scan axis only   shape (1, 4)
```

### ParticleGroup Storage

**Special Handling:** ParticleGroups stored as HDF5 groups, not datasets

**Naming Convention:**
- A ParticleGroup `<name>` must not itself end in `_<integer>` (that suffix is reserved for
  per-shot indices)
- A single ParticleGroup for every shot (data shape all ones, including `batch_dims = ()`):
  `<name>` (no index)
- Otherwise one group per element of the data shape: `<name>_i_j_k`, where i,j,k index the data's
  batch dimensions (index 0 along broadcast size-1 dimensions)

**Placement:** A single ParticleGroup follows the same rules as datasets: directly under
`observables/` if it is the only observable at its location, otherwise inside the
`<location_name>/` group. Per-shot ParticleGroups are several entries and are therefore always
stored inside their `<location_name>/` group (or in a multi-location group). Each group of a
location-primary observable has an `@location` group attribute.

**Examples:**
```
batch_dims = ()                    →  initial_particles
batch_dims = (5,), data shape (1,) →  initial_particles                  (same for all shots)
batch_dims = (5,)                  →  VCCF/initial_particles_0, ..., VCCF/initial_particles_4
batch_dims = (2,3)                 →  VCCF/initial_particles_0_0, ..., VCCF/initial_particles_1_2
batch_dims = (2,3), shape (1,3)    →  VCCF/initial_particles_0_0, VCCF/initial_particles_0_1, VCCF/initial_particles_0_2
```

**Structure:** Uses openPMD-beamphysics native format with:
- Particle coordinates (x, y, z)
- Momenta (px, py, pz)
- Time (t)
- Weights, status, charge, mass
- Per-quantity unit attributes

**Requirements:**
- Container must be numpy object array
- All elements must be ParticleGroup instances
- Only the `@location` attribute is added (location-primary observables); no units,
  read_only, etc.
- Each ParticleGroup is self-contained with internal metadata

### Data Type Requirements

#### Regular Numeric Data
- **Container:** Numpy ndarray
- **Allowed dtypes:** numeric — floating point (e.g. float32, float64), signed/unsigned integer
  (e.g. int32, int64, uint16), boolean or complex
- **Shape validation:** Must match `batch_dims + (num_locations,) + feature_dims` (size-1 batch dims allowed)

#### Timestamp Data
- **Container:** Numpy unicode array in memory; UTF-8 variable-length strings in HDF5
- **Content validation:** Every element is an ISO 8601 date-time with UTC offset (see [Timestamp Observable](#timestamp-observable))

#### ParticleGroup Data
- **Container:** Numpy object array
- **dtype:** Must be `object`
- **Content validation:** Every element must be ParticleGroup instance
- **Shape validation:** Must match batch_dims (size-1 dims allowed; no location or feature dims)

### Dimension Validation

#### Batch Dimensions
- **Empty tuple:** `batch_dims = ()` requires exactly one data point
- **Non-empty:** Product of batch_dims must equal number of data points
- **Data shape:** First N dimensions of data must match the N batch dimensions, each either equal
  or 1 (broadcast)

#### Location Dimensions
- **location_primary=True:** Exactly 1 location required
- **Multi-location (location_primary=False / location_group):** 1+ locations allowed, must provide location_units
- **Multi-location consistency:** Within each multi-location group, all observables must share identical locations

#### Feature Dimensions
- **num_feature_dims=0:** Scalar (single value per batch/location point)
- **num_feature_dims=1:** 1D array (requires bin_size and offset)
- **num_feature_dims=2:** 2D array (requires bin_size and offset)
- **num_feature_dims≥3:** Higher-dimensional (requires bin_size and offset)

### Unit Handling

#### Unit String Processing
The unit string is **stored unchanged**. To validate it, the reference implementation:
1. Accepts the string directly if it is a known openPMD-beamphysics unit
2. Otherwise strips the longest matching SI prefix (ASCII 'u' accepted for 'µ')
3. Looks up the prefix multiplier (e.g., 'p' → 1e-12) and checks the base unit (e.g., 'pC' → 'C')
4. If the unit is not recognized, treats it as a custom unit

The multiplier is used only for this check; it is not written to the file.

#### Supported Prefixes
**SI Prefixes:** y, z, a, f, p, n, µ, m, c, d, da, h, k, M, G, T, P, E, Z, Y
**ASCII Alias:** 'u' accepted for 'µ' (micro)

#### Storage

- `@units`: Unit string exactly as provided, including prefix (e.g., "pC", "uW", "mm",
  or custom units such as "unitless", "counts", "arb")
- No `@unit_multiplier` (legacy, see above)

---

## Validation Rules

The following rules are enforced when creating and combining HDF5 files:

**File-Level Rules:**
- No duplicate IDs allowed within a file
- All IDs must be non-empty strings; every ID in root @IDs has a group and every data-point
  group is listed in @IDs
- Root @Data_Standard_Version must match all batch @Data_Standard_Version attributes
- Combined files store the lattice once, at the root

**Lattice Rules:**
- All batches in a combined file must share identical lattice configurations
- When @lattice_location is "included", lattice_files/ subgroup must be non-empty

**Batch Rules:**
- batch_dims product must equal actual number of data points
- All elements in batch_dims must be positive integers (> 0)
- Empty batch_dims `()` indicates a single data point

**Observable Rules:**
- read_only attribute must be boolean type (legacy files: control, which must equal not read_only if both exist)
- num_feature_dims must be >= 0
- units must be a string (stored with SI prefix)
- When num_feature_dims > 0, bin_size and offset must be specified in attrs dict
- offset must be convertible to float
- bin_size must be a float, or a string naming another observable (key) whose data has dimension batch_dims (size-1 dims allowed)
- unit_multiplier (legacy files only) must be positive (> 0)

**Layout Rules:**
- A location with exactly one location-primary observable stored as a single entry is stored at
  `observables/<PV>`; a location with several observables, or with per-shot ParticleGroups, is a
  group `observables/<location_name>/`
- Names starting with `multi_location_data` are reserved for multi-location groups
- Every location-primary dataset and ParticleGroup group has `@location` (matching the parent
  group name when nested); only `Timestamp` may omit it
- No two entries may map to the same HDF5 path; `data_name` must not contain `/`

**Timestamp Rules:**
- Optional; at most one per data point; shape batch_dims; num_feature_dims = 0; read_only = True
- Each value is an ISO 8601 extended-format date-time with UTC offset

**Multi-Location Rules:**
- Multi-location groups are named `multi_location_data` or `multi_location_data_<suffix>`
- Within each group, all observables share identical DATA_LOCATIONS, location_units and
  num_feature_dims; DATA_LOCATIONS is 1-D
- location_units must be specified for multi-location data

**Broadcasting Rules:**
- Each leading batch dimension of an observable's data equals `batch_dims[i]` or 1

**ParticleGroup Rules:**
- Container must be numpy object array with dtype='object'
- All elements must be ParticleGroup instances
- Shape must match batch_dims (size-1 dims allowed; no location or feature dims)
- Per-shot groups `<name>_i[_j...]` form a full index grid of the data shape; a single
  ParticleGroup is named `<name>`; `<name>` must not end in `_<integer>`

**Data Type Rules:**
- Regular data: numeric (float, integer, boolean, complex); Timestamp: ISO 8601 strings
- Data shape must match: batch_dims + (num_locations,) + feature_dims (size-1 batch dims allowed)

---

## Validating Files

Every rule above can be checked independently of the code that wrote the data:

```python
from data_standard import validate_file, validate_datapoint

report = validate_file("Example_v0.1.2.h5")   # single data-point or combined file
report.ok           # True if no errors
report.errors       # list of "path: message" strings
report.warnings     # legacy/discouraged constructs that are still accepted
report.raise_if_errors()

validate_datapoint(D)   # in-memory DataPoint2 / SimulatedDataPoint2, without saving
```

Command line (exit code 1 if any file is invalid):

```bash
data-standard-validate file.h5 [more.h5 ...] [--strict] [--quiet]
python -m data_standard file.h5 [...]
```

`--strict` (or `validate_file(..., strict=True)`) treats warnings as errors.

Files written by earlier versions remain valid; the following are reported as **warnings**:
`@control` instead of `@read_only`, `@unit_multiplier`, location groups holding a single
single-entry observable, per-shot ParticleGroups directly under `observables/`, a single
ParticleGroup named `<name>_0` for `batch_dims = ()`, ParticleGroups without `@location`, and a
`Data_Standard_Version` other than the validator's.

---

## Changes in 0.1.2

- `@read_only` replaces `@control` (inverted); `control=` remains accepted by the Python API
- Observables are named by their full PV (convention)
- Locations with a single observable stored as one entry are flattened to `observables/<PV>`;
  per-shot ParticleGroups stay in their location group; `@location` is written on every
  location-primary entry, including ParticleGroup groups
- Batch broadcasting: size-1 batch dimensions apply to every shot, so data that is constant
  over a scan is stored once; a single ParticleGroup is named `<name>` (previously `<name>_0`)
- Multiple multi-location groups (`multi_location_data_<suffix>`), each with its own DATA_LOCATIONS
- `@units` stores the unit with its SI prefix; `@unit_multiplier` is no longer written
- Optional per-shot `Timestamp` observable (ISO 8601 with UTC offset)
- Standalone validator (`validate_file`, `validate_datapoint`, `data-standard-validate`)

---

## Implementation Notes

This specification defines the data standard by describing the HDF5 file structure and validation rules. The Python implementation in `src/data_standard/Data_Standard_2.py` and `src/data_standard/Combine_Files.py` serves as the reference implementation but is not definitive. The HDF5 file format itself is the authoritative standard.


- **README.md**: Quick start guide and API overview and how to use the examples


