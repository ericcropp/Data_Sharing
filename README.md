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
- **Combining tools**: Merge multiple data points into unified datasets


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
    batch_dims=0,
    feature_dims=0,  # scalar
    location=['ICT1'],
    data=np.array([250.0]),  # pC
    data_names=['charge'],
    units='pC',
    control=False
)

# Add a 2D image (e.g., screen image)
image_array = np.zeros((100,200))
D.add_observable(
    batch_dims=0,
    feature_dims=2,  # 2D image
    location=['Screen1'],
    data=image_array,
    data_names=['image'],
    units='counts',
    attrs={'pxcal': 1e-6}  # pixel calibration
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

