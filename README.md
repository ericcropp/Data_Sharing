# Data Standard for Cross-Institution Accelerator Data Sharing

[![Tests](https://github.com/ericcropp/Data_Sharing/actions/workflows/tests.yml/badge.svg)](https://github.com/ericcropp/Data_Sharing/actions/workflows/tests.yml)

A minimal Python package and data standard for storing, validating, and sharing
**simulation and experimental** datasets across institutions in a common and reproducible way.

## Motivation
Every accelerator facility produces large amounts of heterogeneous data (e.g. images, scalars, waveforms), including simulation outputs and experimental measurements.  Historically, each institution (or group within an institution) has its own ad-hoc structure for data, which impedes cross-institutional collaboration.  At best, it requires writing translators between formats and at worst, it leads to siloed research and solutions that cannot be extended to other institutions.  

Particularly with recent advances in machine learning (ML), a point of emphasis in the field has been to standardize techniques across labs.  This includes the in-development Particle Accelerator Lattice Standard (PALS: https://github.com/pals-project/pals), among other smaller cross-institutional ML efforts.  

This project defines a **minimal, evolving standard** for storing such data, along with Python tooling to read, write, validate, and combine datasets in a consistent way. This was developed for the **DOE HEP HAAI** cross-institutional collaboration, with possible extension to larger efforts in the field.  

## Features

- **Flexible data dimensions**: Support for 0D scalars, 1D waveforms, 2D images, and higher-dimensional data
- **ParticleGroup support**: Native handling of particle distribution data via `pmd_beamphysics`
- **Batch processing**: Built-in support for multi-dimensional scans.
- **Metadata tracking**: Lattice, simulation, and run information
- **HDF5 storage**: Efficient, hierarchical data storage
- **Validation utilities**: Automatic checking of data dimensions and units
- **Combining tools**: Merge multiple batches into unified files that **match the rules of the data standard**


## Scope

This project has been left intentionally minimal.

**What it provides**
- A small set of standardized data structures for accelerator lattices (to be replaced by PALS) and observables
- Validation utilities
- HDF5-based storage conventions
- Reference examples for three cases:
    - FACET-II Simulation Data
    - FACET-II Experimental Data
    - AWA Experimental Data

**What it does not try to be**
- A universal standard for all experiments
- A frozen or finalized standard
- A full analysis or visualization framework

**The standard is expected to evolve as collaboration needs change.**

## Installation

### Prerequisites

This repository uses **Git LFS (Large File Storage)** for example data files. Install Git LFS before cloning:

```bash
# On Ubuntu/Debian
sudo apt-get install git-lfs

# On macOS
brew install git-lfs

# On Windows
# Download from https://git-lfs.github.com/

# Initialize Git LFS
git lfs install
```

### Basic Installation

```bash
git clone https://github.com/ericcropp/Data_Sharing.git
cd Data_Sharing
conda env create -f environment.yml
conda activate data_standard
pip install -e .
```

For development, use the development env:
```bash
git clone https://github.com/ericcropp/Data_Sharing.git
cd Data_Sharing
conda env create -f environment.dev.yml
conda activate data_standard_dev
pip install -e .
```

## Development

For development and testing, install the package in editable mode:

```bash
pip install -e .
```

## Quickstart

```python
from data_standard import DataPoint2, SimulatedDataPoint2
import numpy as np

# Create a data point
D = DataPoint2()

# Add a scalar observable (e.g., beam charge)
D.add_observable(
    batch_dims=(),  # Empty tuple for single data point
    num_feature_dims=0,  # scalar
    location=['ICT1'],
    data=np.array(250.0),  # 0-D array for scalar
    data_name='charge',
    units='pC',
    control=False
)

# Add a 2D image (e.g., screen image)
image_array = np.zeros((100, 200))
D.add_observable(
    batch_dims=(),
    num_feature_dims=2,  # 2D image
    location=['Screen1'],
    data=image_array,
    data_name='image',
    units='counts',
    attrs={'bin_size': 1e-6, 'offset': 0.0}  # Required for feature dims > 0
)

# Add lattice and metadata
D.add_lattice(lattice_location='https://github.com/slaclab/facet2-lattice')
D.add_run_information(source='FACET-II', date='2025-01-26', notes='Example run')

# Finalize and save
D.saveHDF5('./output/')
```

## Data Structure

This is a summary.  The authoritative standard lives in SPEC.md.  

The HDF5 file structure follows this hierarchy:

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
        │       ├── @unit_multiplier        # Dataset attribute: Prefix multiplier (e.g., 1e-12)
        │       ├── @bin_size               # Dataset attribute: (Required if num_feature_dims > 0)
        │       └── @offset                 # Dataset attribute: (Required if num_feature_dims > 0)
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
                ├── @unit_multiplier        # Dataset attribute: Prefix multiplier
                ├── @bin_size               # Dataset attribute: (Required if num_feature_dims > 0)
                └── @offset                 # Dataset attribute: (Required if num_feature_dims > 0)
```

## Examples

Examples are provided in the `examples/` directory:

### FACET-II Simulation Data
Processes Impact-T simulation archives with particle distributions:
```bash
python examples/FACET-II_Simulation_Example.py 
```

### FACET-II Experimental Data
Converts EPICS-based experimental data:
```bash
python examples/FACET-II_Experimental_Example.py 
```

### AWA Experimental Data
Processes AWA facility data with waveforms and images:
```bash
python examples/AWA_Experimental_Example.py 
```

## API Stability

The public API exposed via `data_standard` is intended to remain stable within
minor versions.

Internal module structure may change as the standard evolves. Users should rely
only on documented imports from the top-level package.

## Testing

Run all tests:
```bash
pytest tests/
```

Run only fast tests (API imports):
```bash
pytest tests/ -m "not slow"
```

Run only integration tests:
```bash
pytest tests/ -m "slow"
```

## Contributing

This standard is under active development. Contributions and suggestions are welcome.

To contribute:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

