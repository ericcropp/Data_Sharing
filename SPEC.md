# Data Standard Specification

**Version:** 2026-06-22  
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
  - [ParticleGroup Storage](#particlegroup-storage)
  - [Data Type Requirements](#data-type-requirements)
  - [Dimension Validation](#dimension-validation)
  - [Unit Handling](#unit-handling)
- [Validation Rules](#validation-rules)
- [Implementation Notes](#implementation-notes)

---

## Overview

This document defines a standardized format for storing accelerator physics data in HDF5 files. The standard supports both experimental and simulation data, with flexible dimensionality for batch processing and feature-rich observables.

**Key Features:**
- Hierarchical HDF5 storage with rich metadata
- Support for scalar, vector, and multi-dimensional observables
- Integration with openPMD-beamphysics ParticleGroup format
- Flexible location-based or multi-location data organization
- Comprehensive unit handling with SI prefix support
- Summary metadata for dataset discovery and filtering

**Defining Implementation:**
- `src/data_standard/Data_Standard_2.py`: Classes that generate intermediate files 
- `src/data_standard/Combine_Files.py`: Utility for finalizing a single HDF5 file that is compliant with the data standard defined below. Automatically appends the Data Standard version to the output filename (e.g., `filename_v0.1.0.h5`).
- Three examples are in `examples/`.  See `README.md` for how to run those examples.

---

## Core Principles

### 1. Observable-Centric Design
All physical quantities are stored as **observables** with associated metadata:
- `data_name`: Unique identifier for the observable
- `units`: Physical units with prefix support
- `location`: Where the measurement/simulation occurred
- `control`: Boolean indicating if this is an input (control) or output parameter
- `batch_dims`: Tuple defining batch structure
- `num_feature_dims`: Integer count of feature dimensions

### 2. Flexible Dimensionality
Data can have multiple dimension types:
- **Batch dimensions** (`batch_dims`): Independent experimental/simulation runs
- **Feature dimensions** (`num_feature_dims`): Intrinsic data structure (images, spectra, etc.)
- **Location dimensions**: Multiple observation points along beamline

### 3. Location-Based Organization
Two storage patterns:
- **`location_primary=True`**: Data grouped by location (each location is a separate HDF5 group)
- **`location_primary=False`**: Data grouped by type with shared location array

### 4. ParticleGroup & Data Support
- **Scalar/array data**: Stored as HDF5 datasets with shape `*batch_dims + feature_dims`
- **ParticleGroups**: Stored using the openPMD-beamphysics N-D ensemble format;
  each particle component has shape `*batch_dims + (n_particles,)` — the same leading
  dimensions as numeric observables, making batch iteration uniform across data types

---

## Definitions

**Shot:** A single run of the accelerator or simulation code

**Batch:** An N-dimensional set of shots (i.e. scanning a single quadrupole from -5 T/m to 5 T/m in 1 T/m steps is a 1-D batch). This standard can accommodate any dimension, including 0 → a single data point.

**Lattice:** Configuration defining the accelerator beamline elements and their properties.  All observables' locations must be specified by this lattice.  

**Observable:** A quantity measured or computed at specific location(s) along the beamline.  Notably, these can be inputs or outputs (specify with control boolean; see below)

**Feature dimensions:** Intrinsic dimensions of the data beyond batch and location (e.g., pixels in an image, bins in a spectrum)

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
    ├── @batch_labels                       # Group attribute: (Optional) Labels for batch dimensions
    ├── @run_information_source             # Group attribute: Data source (e.g., "FACET-II")
    ├── @run_information_date               # Group attribute: Date in YYYY-MM-DD format
    ├── @run_information_notes              # Group attribute: Optional notes
    ├── @simulation_code                    # Group attribute: (Simulation only) Code name
    ├── @simulation_version                 # Group attribute: (Simulation only) Code version
    ├── @simulation_start                   # Group attribute: (Simulation only) Start position
    ├── @simulation_end                     # Group attribute: (Simulation only) End position
    │
    └── observables/
        ├── <location_name>/                # Location-grouped storage
        │   ├── <observable_name>           # Dataset: batch_dims + feature_dims (numeric)
        │   │   ├── @location               # Dataset attribute: Location name
        │   │   ├── @control                # Dataset attribute: Boolean: control variable?
        │   │   ├── @num_feature_dims       # Dataset attribute: Integer: feature dimensions
        │   │   ├── @units                  # Dataset attribute: Unit string (e.g., "m", "pC")
        │   │   ├── @unit_multiplier        # Dataset attribute: Prefix multiplier (e.g., 1e-12)
        │   │   ├── @bin_size               # Dataset attribute: (Required if num_feature_dims > 0)
        │   │   └── @offset                 # Dataset attribute: (Required if num_feature_dims > 0)
        │   └── ParticleGroup/              # Group: ParticleGroup observable (fixed name)
        │       ├── @control                # Group attribute: Boolean: control variable?
        │       ├── @location               # Group attribute: Location name
        │       ├── @num_feature_dims       # Group attribute: n_particles per distribution
        │       └── <species>/              # e.g. "electron" – written by ensemble format
        │           ├── @numDistributions   # Scalar: prod(batch_dims)
        │           ├── @ensembleShape      # Array: batch_dims (authoritative N-D shape)
        │           └── position/x  (*batch_dims, n_particles)
        │
        └── multi_location_data/            # Multi-location storage
            ├── @data_locations_key              # Group attribute: MUST specify the dataset name containing locations
            ├── DATA_LOCATIONS              # Dataset: location array
            │   ├── @units                  # Dataset attribute: Location units
            │   ├── @num_feature_dims       # Dataset attribute: Always 0 for locations
            │   └── @batch_dims             # Dataset attribute: Always "N/A" (batches do not apply)
            │
            ├── <observable_name>           # Dataset: batch_dims + (locations,) + feature_dims (numeric)
            │   ├── @control                # Dataset attribute: Boolean
            │   ├── @num_feature_dims       # Dataset attribute: Integer: feature dimensions
            │   ├── @units                  # Dataset attribute: Unit string
            │   ├── @unit_multiplier        # Dataset attribute: Prefix multiplier
            │   ├── @bin_size               # Dataset attribute: (Required if num_feature_dims > 0)
            │   └── @offset                 # Dataset attribute: (Required if num_feature_dims > 0)
            └── ParticleGroup/              # Group: ParticleGroup observable (fixed name)
                ├── @control                # Group attribute: Boolean: control variable?
                ├── @num_feature_dims       # Group attribute: n_particles per distribution
                └── <species>/              # e.g. "electron"
                    ├── @numDistributions   # Scalar: prod(batch_dims)
                    ├── @ensembleShape      # Array: batch_dims (authoritative N-D shape)
                    └── position/x  (*batch_dims, n_particles)
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
- **Requirement:** Present when `@lattice_location = "included"`
- **Content:** One dataset per file, with filename as dataset name
- **File Content Type:** String or bytes
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
- **Format:** `<DataType>_<8-char-hash>`
- **Hash:** 8-character MD5 hash derived from:
  - data_type
  - batch_dims
  - timestamp
  - run_information source
  - control observables (names and locations)
- **Requirement:** Must be non-empty and unique

#### @batch_dims
- **Type:** Tuple/Array of positive integers, or the string `"N/A"`
- **Stored as:** Numpy array in HDF5 (empty array for single point), or string `"N/A"`
- **Requirement:** All elements must be positive integers > 0 (when numeric)
- **Rule:** Product of batch_dims must equal number of data points in batch
- **Empty tuple:** `()` indicates single data point (not batched)
- **Special value `"N/A"`:** Indicates that batch dimensions do not apply to this dataset. This value is ONLY permitted on the DATA_LOCATIONS dataset within `multi_location_data/`. All other datasets must have a numeric `batch_dims`.

#### @batch_labels
- **Type:** Array of strings
- **Requirement:** Optional
- **Purpose:** Human-readable labels describing the meaning of each batch dimension
- **Rule:** If present, length must equal `len(batch_dims)` (i.e., one label per batch dimension)
- **Special case:** When `batch_dims = ()` (empty), batch_labels may be a single string describing the lone data point
- **Examples:**
  - `batch_dims = [5]` → `batch_labels = ["quadrupole_strength"]` (1 label)
  - `batch_dims = [5, 2]` → `batch_labels = ["quadrupole_strength", "solenoid_current"]` (2 labels)
  - `batch_dims = []` → `batch_labels = ["baseline_measurement"]` (1 label, special case)

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

**Structure:** `/<ID>/observables/<location_name>/<observable_name>`
**Use Case:** Single location per observable (BPMs, screens, individual diagnostics)

**Requirements:**
- Location must be list/array with exactly one element
- Observable name must be non-empty string

**Dataset Shape:** `batch_dims + feature_dims`

**Dataset Attributes:**

##### @location
- **Type:** String
- **Requirement:** Must match parent group name
- **Purpose:** Redundant but explicit location identification

##### @control
- **Type:** Boolean (True/False)
- **Requirement:** Must be boolean type, not truthy/falsy value
- **Purpose:** Indicates if this is a control variable (input) or output

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
- **Requirement:** Should be valid unit string recognized by openPMD-beamphysics, but custom units are allowed
- **Format:** Base unit without prefix (e.g., "m" not "um")
- **Dimensionless:** Use "1" for unitless/dimensionless quantities
- **Custom Units:** Non-recognized units (e.g., "unitless", "counts") are stored as-is with unit_multiplier=1.0
- **ParticleGroup Data:** Units attribute is not stored for ParticleGroup datasets; units are embedded in the ParticleGroup's own metadata

##### @unit_multiplier  
- **Type:** Float
- **Requirement:** Must be positive (> 0)
- **Purpose:** Converts displayed units to base units
- **Example:** For "um", unit_multiplier = 1e-6, units = "m"
- **Calculation:** `base_value = displayed_value * unit_multiplier`

**Additional Attributes (when num_feature_dims > 0):**

When `num_feature_dims > 0`, the following attributes are **required** and must be provided in the `attrs` dictionary parameter when calling `add_observable()`:

##### @bin_size
- **Type:** Float
- **Requirement:** Required when num_feature_dims > 0; must be specified in attrs dict
- **Purpose:** Physical size of one bin/pixel in feature space
- **Units:** Match observable units
- **Note:** Can be positive or negative (negative for reversed coordinate axes)
- **Usage:** `attrs={'bin_size': 1e-6, 'offset': 0.0}`

##### @offset
- **Type:** Float
- **Requirement:** Required when num_feature_dims > 0; must be specified in attrs dict
- **Purpose:** Physical coordinate of first bin/pixel
- **Units:** Match observable units
- **Can be:** Any float
- **Coordinate calculation:** `coord[i] = offset + i * bin_size`
- **Usage:** `attrs={'bin_size': 1e-6, 'offset': 0.0}`

#### Pattern 2: Multi-Location Storage

**Structure:** `/<ID>/observables/multi_location_data/`
**Use Case:** Multiple observation points along beamline with shared location array

**Group Attributes:**

##### @data_locations_key
- **Type:** String
- **Requirement:** MUST be specified when `multi_location_data` group exists
- **Purpose:** Specifies the name of the dataset within this group that contains the location coordinates
- **Default value:** `"DATA_LOCATIONS"`
- **Rule:** The named dataset must exist within the `multi_location_data` group

**Special Dataset:**

##### DATA_LOCATIONS
- **Type:** Numeric array
- **Shape:** `(num_locations,)`
- **Purpose:** Shared location coordinates for all observables in this group
- **Attributes:**
  - `@units`: Location units (typically "m" for position along beamline)
  - `@num_feature_dims`: Always 0 (locations are scalar coordinates)
  - `@batch_dims`: Always `"N/A"` — batch dimensions do not apply to the location array itself
- **Requirement:** All observables in multi_location_data must share identical DATA_LOCATIONS
- **Requirement:** location_units must be specified
- **Note:** The `@batch_dims = "N/A"` is the standard notation indicating that batch dimensions do not apply to this dataset. This is ONLY permitted for DATA_LOCATIONS; all other datasets must carry a numeric `batch_dims`. The location array is a shared coordinate axis, not a per-batch quantity.

**Observable Datasets:**

**Shape:** `batch_dims + (num_locations,) + feature_dims`

**Dataset Attributes:**

Same as location-primary storage except:
- No `@location` attribute (shared via DATA_LOCATIONS)
- Location dimension inserted between batch and feature dimensions

### ParticleGroup Storage

**Ensemble Format:** ParticleGroups are stored using the openPMD-beamphysics ensemble format
(`write_particle_ensemble` / `read_particle_ensemble` from the
[custom fork](https://github.com/ericcropp/openPMD-beamphysics)).

**Storage Layout:**
Each component (`x`, `px`, `y`, `py`, `z`, `pz`, `t`, `weight`, `status`) is stored as an
**N-D dataset** of shape `(*batch_dims, n_particles)` inside a species subgroup — directly
mirroring the shape of a regular numeric observable.  The species group carries an
`ensembleShape` attribute (equal to `batch_dims`) and a scalar `numDistributions` attribute.

**Examples:**
```
batch_dims = ()      → ensemble with ensembleShape=[] ,  dataset shape (n_particles,)
batch_dims = (5,)    → ensemble with ensembleShape=[5],  dataset shape (5, n_particles)
batch_dims = (3, 4)  → ensemble with ensembleShape=[3,4], dataset shape (3, 4, n_particles)
```

**HDF5 structure (ensemble):**
```
<parent_group>/                           (e.g. observables/<location>)
  ParticleGroup/                          ← fixed group name; one per location
    @control          = False             ← observable metadata (mirrors numeric observables)
    @location         = "injector"        ← location name (location-primary storage only)
    @num_feature_dims = n_particles       ← number of particles per distribution
    <species>/                            (e.g. "electron") – written by ensemble format
      @speciesType    = "electron"
      @numParticles   = n_particles
      @numDistributions = prod(batch_dims)  ← scalar convenience count
      @ensembleShape  = batch_dims          ← authoritative N-D batch shape
      @totalCharge    = ...
      @chargeUnitSI   = 1.0
      position/
        x   (*batch_dims, n_particles)    ← same leading dims as a regular observable
        y   (*batch_dims, n_particles)
      momentum/
        x   (*batch_dims, n_particles)
        y   (*batch_dims, n_particles)
        z   (*batch_dims, n_particles)
      time    (*batch_dims, n_particles)
      weight  (*batch_dims, n_particles)
      particleStatus (*batch_dims, n_particles)
```

**Batch dimensions:**
- All batch shapes are supported: 0-D `()`, 1-D `(N,)`, and arbitrary N-D
  (e.g., `(3, 4)` for a 2-D parameter scan over a pair of knobs).
- `ensembleShape` records the exact batch shape so it can be recovered on read-back.

**Unified Dimension Model:**

ParticleGroup components (`x`, `px`, `y`, …) follow the **same dimension convention**
as all other observables in this standard:

| Dimension type | Regular numeric data | ParticleGroup components |
|---|---|---|
| Batch dims | Leading axes of dataset | Leading axes `*batch_dims` |
| Feature dims | Trailing axes (num_feature_dims) | Fixed trailing axis: `n_particles` |
| Location dim | Between batch and feature | N/A (no per-location replication) |

This means a `batch_dims = (3, 4)` parameter scan produces identically-shaped leading
dimensions whether the observable is an emittance scalar, a beam-size profile, or a full
ParticleGroup:

```
emittance dataset   shape: (3, 4)              → batch_dims only
profile dataset     shape: (3, 4, 512)         → batch_dims + 1 feature dim
ParticleGroup x     shape: (3, 4, n_particles) → batch_dims + particle dim
```

The particle dimension plays the role of the feature dimension for ParticleGroups.
Consequently, `num_feature_dims` is always 0 for ParticleGroup observables — the
particle array is handled internally by the ensemble format.

### Data Type Requirements

#### Regular Numeric Data
- **Container:** Numpy ndarray
- **Allowed dtypes:** float32, float64, int32, int64
- **Shape validation:** Must match `batch_dims + (num_locations,) + feature_dims`

#### ParticleGroup Data
- **Container (Python API):** Pass a numpy object array (`dtype=object`) to
  `add_observable()`. This is a Python-level API convention; the HDF5 file does
  not store any "object array" marker.
- **Content validation:** All elements must be `ParticleGroup` instances with
  identical species and `n_particle`.
- **Shape/batch validation:** Array shape must equal `batch_dims` exactly.
  `SingleObservable.data_dim_checker()` (called from `add_observable()`) raises
  `ValueError` if the shape does not match, ensuring the ParticleGroup batch
  dimensions are always consistent with all other observables on the same `DataPoint2`.
- **HDF5 group attrs:** The `ParticleGroup` group carries `@control`, `@location`
  (location-primary only), and `@num_feature_dims` (= n_particles) — consistent
  with numeric observable metadata.
- **HDF5 path:** Always written under the fixed group name `ParticleGroup` inside the
  location group, e.g. `observables/<location>/ParticleGroup/<species>/`.
- **Ensemble metadata:** `@ensembleShape` on the species subgroup is the authoritative
  N-D batch shape for round-trip recovery; `@numDistributions` is a scalar convenience
  equal to `prod(batch_dims)`.

### Dimension Validation

#### Batch Dimensions
- **Empty tuple:** `batch_dims = ()` requires exactly one data point
- **Non-empty:** Product of batch_dims must equal number of data points
- **Data shape:** First N dimensions of data must match N batch dimensions

#### Location Dimensions
- **location_primary=True:** Exactly 1 location required
- **location_primary=False:** 1+ locations allowed, must provide location_units
- **Multi-location consistency:** All observables in multi_location_data must share identical locations

#### Feature Dimensions
- **num_feature_dims=0:** Scalar (single value per batch/location point)
- **num_feature_dims=1:** 1D array (requires bin_size and offset)
- **num_feature_dims=2:** 2D array (requires bin_size and offset)
- **num_feature_dims≥3:** Higher-dimensional (requires bin_size and offset)

### Unit Handling

#### Unit String Processing
1. Check for ASCII 'u' as micro prefix alias (convert to 'µ')
2. Extract prefix from unit string
3. Look up prefix multiplier (e.g., 'p' → 1e-12)
4. Extract base unit (e.g., 'pC' → 'C')
5. Validate base unit with openPMD-beamphysics
6. If unit is not recognized, treat as custom unit

#### Supported Prefixes
**SI Prefixes:** y, z, a, f, p, n, µ, m, c, d, da, h, k, M, G, T, P, E, Z, Y
**ASCII Alias:** 'u' accepted for 'µ' (micro)

#### Storage

**Recognized Units:**
- `@units`: Base unit string (e.g., "C" for coulombs)
- `@unit_multiplier`: Numeric multiplier (e.g., 1e-12 for pC → C)

**Custom/Unrecognized Units:**
- `@units`: Original unit string as provided (e.g., "unitless", "counts", "arb")
- `@unit_multiplier`: 1.0 (no conversion)

---

## Validation Rules

The following rules are enforced when creating and combining HDF5 files:

**File-Level Rules:**
- No duplicate IDs allowed within a file
- All IDs must be non-empty and follow the format `<DataType>_<8-char-hash>`
- Root @Data_Standard_Version must match all batch @Data_Standard_Version attributes

**Lattice Rules:**
- All batches in a combined file must share identical lattice configurations
- When @lattice_location is "included", lattice_files/ subgroup must be non-empty

**Batch Rules:**
- batch_dims product must equal actual number of data points
- All elements in batch_dims must be positive integers (> 0)
- Empty batch_dims `()` indicates a single data point
- If @batch_labels is present, its length must equal `len(batch_dims)` (or 1 when batch_dims is empty)

**Observable Rules:**
- control attribute must be boolean type
- num_feature_dims must be >= 0
- When num_feature_dims > 0, bin_size and offset must be specified in attrs dict
- bin_size and offset must be convertible to float
- unit_multiplier must be positive (> 0)

**Multi-Location Rules:**
- `@data_locations_key` attribute MUST be specified on the `multi_location_data` group
- The dataset named by `@data_locations_key` must exist within the group
- All observables in multi_location_data must share identical DATA_LOCATIONS
- location_units must be specified for multi-location data
- DATA_LOCATIONS dataset must have `@batch_dims = "N/A"`

**ParticleGroup Rules:**
- Container must be numpy object array with dtype='object'
- All elements must be ParticleGroup instances
- All distributions must share the same species and n_particle
- Shape must match batch_dims exactly (no location or feature dims)
- Supports arbitrary N-D batch_dims; ensembleShape attribute records the full shape

**Data Type Rules:**
- Regular numeric data: float32, float64, int32, int64
- Data shape must match: batch_dims + (num_locations,) + feature_dims

---

## Implementation Notes

This specification defines the data standard by describing the HDF5 file structure and validation rules. The Python implementation in `src/data_standard/Data_Standard_2.py` and `src/data_standard/Combine_Files.py` serves as the reference implementation but is not definitive. The HDF5 file format itself is the authoritative standard.


- **README.md**: Quick start guide and API overview and how to use the examples


