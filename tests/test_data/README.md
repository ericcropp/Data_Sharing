# Test Data

This directory contains a small subset of data files for CI/CD testing.

## Purpose

- **Avoid LFS quota issues**: These files are stored directly in git (not LFS)
- **Fast CI tests**: Only 2 simulation files (~10 MB total) instead of the full dataset
- **Sufficient coverage**: Enough data to validate core functionality

## Contents

- `FACET-II_Simulation_Data/`: 2 Impact-T simulation archive files (0.h5, 1.h5)
- `FACET-II_Simulation_Data/Lattice_Files/`: Required lattice configuration files

## Note

For running full examples locally, use the complete dataset in `examples/data/input/` which is tracked via Git LFS.
