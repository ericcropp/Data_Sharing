# Data Standard for Cross-Institution Accelerator Data

[![Tests](https://github.com/ericcropp/Data_Sharing/actions/workflows/tests.yml/badge.svg)](https://github.com/ericcropp/Data_Sharing/actions/workflows/tests.yml)

A minimal Python package and data standard for storing, validating, and sharing
**simulation and experimental** datasets across institutions in a common and reproducible way.

## Motivation
Every accelerator facility produces large amounts of heterogeneous data (e.g. images, scalars, waveforms), including simulation outputs and experiemntal measurements.  Historically, each institution (or group within an institution) has its own ad-hoc structure for data, which impedes cross-institutional collaboration.  At best, it requires writing translators between formats and at worst, it leads to siloed research and solutions that cannot be extended to other institutions.  

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

```bash
git clone https://github.com/ericcropp/Data_Sharing.git
cd Data_Sharing
conda env create -f environment.yml
conda activate data_standard
```

For development, use the development env:
```bash
git clone https://github.com/ericcropp/Data_Sharing.git
cd Data_Sharing
conda env create -f environment.dev.yml
conda activate data_standard
```


## Quickstart

```python
from data_standard import DataPoint2, SimulatedDataPoint2, validate_file

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
D.add_lattice(lattice_location='FACET-II')
D.add_run_information(source='FACET-II', date='2025-01-26', notes='Example run')

# Finalize and save
D.finalize()
D.saveHDF5('./output/')
```

## Data Structure

The HDF5 file structure follows this hierarchy:

```
<ID>.h5
├── (attributes)
│   ├── ID                          # Unique identifier
│   ├── run_information_source      # Data source
│   ├── run_information_date        # Timestamp
│   ├── Data_Standard_Version       # Format version
│   └── summary_*                   # Summary statistics
├── lattice/
│   ├── lattice_location           # Lattice name/URL
│   └── lattice_files/             # Lattice configuration files
└── observables/
    ├── <location1>/               # Location-grouped data
    │   ├── <observable_name>      # Dataset
    │   └── (attributes)           # units, control, location
    └── Type_Grouped_Data/         # Data grouped by type
        └── <observable_name>      # Dataset with location attribute
```

## Examples

Examples are provided in the `examples/` directory:

### FACET-II Simulation Data
Processes Impact-T simulation archives with particle distributions:
```bash
python examples/FACET-II_Simulation_Example.py \
    --input_dir ./examples/data/input/FACET-II_Simulation_Data/ \
    --output_dir ./examples/data/output/FACET-II_Simulation_Example/
```

### FACET-II Experimental Data
Converts EPICS-based experimental data:
```bash
python examples/FACET-II_Experimental_Example.py \
    --input_dir ./examples/data/input/FACET-II_Experimental_Data/ \
    --output_dir ./examples/data/output/FACET-II_Experimental_Example/
```

### AWA Experimental Data
Processes AWA facility data with waveforms and images:
```bash
python examples/AWA_Experimental_Example.py \
    --input_file ./examples/data/input/AWA_Experimental_Data/DYG12_1759956735.h5 \
    --output_dir ./examples/data/output/AWA_Experimental_Example/
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

