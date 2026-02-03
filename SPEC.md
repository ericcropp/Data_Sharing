(Documentation under construction)
Definitions:
    1) Shot: A single run of the accelerator or simulation code
    2) Batch: An N-dimensional set of shots (i.e. scanning a single quadrupole from -5 T/m to 5 T/m in 1 T/m steps is a 1-D batch).  This standard can accomodate any dimension, including 0 --> a single data point.
    3) Lattice
    4) Observable
    5) Feature dimensions


A single file can contain multiple batches.  To comply with this standard, the data must be stored in hierarchical data format (HDF5).  Here is a diagram of such a file:

The HDF5 file structure follows this hierarchy:

```
Combined_Data.h5
├── (root attributes)
│   ├── Data_Standard_Version               # Version that all data corresponds to
│   └── IDs                                 # List of unique identifiers
│
├── lattice/
│   ├── (group attributes)
│   │   └── lattice_location                # Name, URL, or description
│   ├── simulation_input_file               # Optional: If simulation input file exists, it goes here
│   └── lattice_files/   
│       └── <filename>                      # Datasets storing file contents of required simulation files
│
└── <ID>/
    └── observables/
        │   └── (group attributes)
        │       ├── Data_Standard_Version   # Version that this file corresponds to
        │       ├── ID                      # Unique Identifier (Hash)
        │       ├── batch_dims              # Data batch dimensions
        │       ├── run_information_date    # The creation date of the data
        │       ├── run_information_notes   # Custom field for a description of the data
        │       ├── run_information_source  # Where did this data come from?
        │       ├── simulation_code         # Simulation only: simulation code that made the data
        │       ├── simulation_end          # Simulation end location
        │       ├── simulation_start        # Simulation start location
        │       └── simulation_version      # Simulation code version
        │
        ├── <location_name>/                # Location-grouped storage
        │   └── <observable_name>           # Dataset (shape: batch_dims + feature_dims)
        │       └── (attributes)
        │           ├── units               # Unit string (e.g., "m", "pC")
        │           ├── unit_multiplier     # Prefix multiplier (e.g., 1e-12 for "p")
        │           ├── control             # Boolean: is this a control variable?
        │           ├── location            # Location name (redundant with group)
        │           └── num_feature_dims    # Integer: number of feature dimensions
        │
        └── multi_location_data/            # Alternative storage beamline stats
            └── <observable_name>           # Dataset (shape: batch_dims + 1 (location) + feature_dims)
                └── (attributes)
                    ├── units               # e.g. "m"
                    ├── control             # Boolean
                    ├── unit_multiplier     # Prefix multiplier (e.g., 1e-12 for "p")
                    └── num_feature_dims    # 0 for scalar
```


A description of the rules for each section follows:
(In development)