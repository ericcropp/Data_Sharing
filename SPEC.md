(Documentation under construction)

TODO: philosophy: multiple shots per batch (share lattice and some logical connection), multiple batches per file (share lattice), choice whether to make new batch or new file, etc up to user.  

Definitions:
    1) Shot: A single run of the accelerator or simulation code
    2) Batch: An N-dimensional set of shots (i.e. scanning a single quadrupole from -5 T/m to 5 T/m in 1 T/m steps is a 1-D batch).  This standard can accomodate any dimension, including 0 --> a single data point.
    3) Lattice
    4) Observable
    5) Feature dimensions


A single file can contain multiple batches.  To comply with this standard, the data must be stored in hierarchical data format (HDF5).  Here is a diagram of such a file:

The HDF5 file structure for combined files follows this hierarchy:

```
<filename>.h5
├── @Data_Standard_Version                  # Root attribute: Version that all data corresponds to
├── @IDs                                    # Root attribute: Array of all data point IDs
│
├── lattice/                                # Shared lattice at root level
│   ├── @lattice_location                   # Group attribute: "included" or "external_reference"
│   ├── simulation_input_file               # Dataset: simulation input (if present)
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
        ├── <location_name>/                # Location-grouped storage
        │   └── <observable_name>           # Dataset: batch_dims + feature_dims
        │       ├── @location               # Dataset attribute: Location name
        │       ├── @control                # Dataset attribute: Boolean: control variable?
        │       ├── @num_feature_dims       # Dataset attribute: Integer: feature dimensions
        │       ├── @units                  # Dataset attribute: Unit string (e.g., "m", "pC")
        │       └── @unit_multiplier        # Dataset attribute: Prefix multiplier (e.g., 1e-12)
        │
        └── multi_location_data/            # Multi-location storage
            ├── DATA_LOCATIONS              # Dataset: location array
            │   ├── @units                  # Dataset attribute: Location units
            │   └── @num_feature_dims       # Dataset attribute: Always 0 for locations
            │
            └── <observable_name>           # Dataset: batch_dims + (locations,) + feature_dims
                ├── @control                # Dataset attribute: Boolean
                ├── @num_feature_dims       # Dataset attribute: Integer: feature dimensions
                ├── @units                  # Dataset attribute: Unit string
                └── @unit_multiplier        # Dataset attribute: Prefix multiplier
```


## Detailed Specification

### Root Level Structure

**Root Attributes:**

#### @Data_Standard_Version
- **Type:** String
- **Format:** YYYY-MM-DD date format
- **Requirement:** Must match pattern `^\d{4}-\d{2}-\d{2}$`
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
TODO Add rule about how this must be true for ALL batches included in file

**Subgroups:**

#### lattice_files/
- **Purpose:** Contains all lattice configuration files
- **Requirement:** Present when `@lattice_location = "included"`
- **Content:** One dataset per file, with filename as dataset name
- **File Content Type:** String or bytes
TODO Add rule about how this must be true for ALL batches included in file

### Batch Groups

**Location:** `/<ID>/`
**Naming:** Each data point is a group named by its unique ID

**Group Attributes:**

#### @Data_Standard_Version
- **Type:** String
- **Format:** YYYY-MM-DD
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
- **Requirement:** Must be valid unit string recognized by openPMD-beamphysics
- **Format:** Base unit without prefix (e.g., "m" not "um")
- **Special Case:** "particlegroup" for ParticleGroup data # TODO Check this -- custom units, unitless, etc

##### @unit_multiplier  
- **Type:** Float
- **Requirement:** Must be positive (> 0)
- **Purpose:** Converts displayed units to base units
- **Example:** For "um", unit_multiplier = 1e-6, units = "m"
- **Calculation:** `base_value = displayed_value * unit_multiplier`

**Additional Attributes (when num_feature_dims > 0):**

##### @bin_size
- **Type:** Float
- **Requirement:** Required when num_feature_dims > 0
- **Requirement:** Must be positive (> 0) # CHeck this --> it should not need to be positive
- **Purpose:** Physical size of one bin/pixel in feature space
- **Units:** Match observable units
- **Examples:**
  - Image: pixel size in meters
  - Spectrum: energy bin width in eV
  - Time series: time step in seconds

##### @offset
- **Type:** Float
- **Requirement:** Required when num_feature_dims > 0
- **Purpose:** Physical coordinate of first bin/pixel
- **Units:** Match observable units
- **Can be:** Negative, zero, or positive
- **Coordinate calculation:** `coord[i] = offset + i * bin_size`

#### Pattern 2: Multi-Location Storage

**Structure:** `/<ID>/observables/multi_location_data/`
**Use Case:** Multiple observation points along beamline with shared location array

**Special Dataset:**

##### DATA_LOCATIONS
- **Type:** Numeric array
- **Shape:** `(num_locations,)`
- **Purpose:** Shared location coordinates for all observables in this group
- **Attributes:**
  - `@units`: Location units (typically "m" for position along beamline)
  - `@num_feature_dims`: Always 0 (locations are scalar coordinates)
- **Requirement:** All observables in multi_location_data must share identical DATA_LOCATIONS
- **Requirement:** location_units must be specified

**Observable Datasets:**

**Shape:** `batch_dims + (num_locations,) + feature_dims`

**Dataset Attributes:**

Same as location-primary storage except:
- No `@location` attribute (shared via DATA_LOCATIONS)
- Location dimension inserted between batch and feature dimensions

### ParticleGroup Storage

**Special Handling:** ParticleGroups stored as HDF5 groups, not datasets

**Naming Convention:**
- 0-D arrays (single ParticleGroup): `<name>_0`
- N-D arrays: `<name>_i_j_k` where i,j,k are batch indices

**Examples:**
```
batch_dims = ()  →  initial_particles_0
batch_dims = (5,)  →  initial_particles_0, initial_particles_1, ..., initial_particles_4
batch_dims = (2,3)  →  initial_particles_0_0, initial_particles_0_1, ..., initial_particles_1_2
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
- No dataset attributes (units, control, etc.) stored
- Each ParticleGroup is self-contained with internal metadata

### Data Type Requirements

#### Regular Numeric Data
- **Container:** Numpy ndarray
- **Allowed dtypes:** float32, float64, int32, int64
- **Shape validation:** Must match `batch_dims + (num_locations,) + feature_dims`

#### ParticleGroup Data
- **Container:** Numpy object array
- **dtype:** Must be `object`
- **Content validation:** Every element must be ParticleGroup instance
- **Shape validation:** Must match batch_dims exactly (no location or feature dims)

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

#### Supported Prefixes
**SI Prefixes:** y, z, a, f, p, n, µ, m, c, d, da, h, k, M, G, T, P, E, Z, Y
**ASCII Alias:** 'u' accepted for 'µ' (micro)

#### Storage
- `@units`: Base unit string (e.g., "C" for coulombs)
- `@unit_multiplier`: Numeric multiplier (e.g., 1e-12 for pC → C)



#### Validation Rules
- No duplicate IDs allowed
- All data points must use same Data_Standard_Version
- All lattices must be identical (after exclusions)



## Implementation Notes

This specification defines the data standard by describing the HDF5 file structure and validation rules. The Python implementation in `src/data_standard/Data_Standard_2.py` serves as the reference implementation but is not definitive. The HDF5 file format itself is the authoritative standard.


- **README.md**: Quick start guide and API overview


