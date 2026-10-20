"""
Data Standard Package
=====================

A standardized format for accelerator physics data, providing consistent
interfaces for experimental and simulation data with support for observables,
lattice information, and metadata.

Main Classes
------------
DataPoint2 : Base class for experimental data
SimulatedDataPoint2 : Extended class for simulation data with lattice support

Utilities
---------
combine_files : Combine multiple data files into a single HDF5 file
validate_file : Validate an HDF5 file (single data point or combined) against the standard
validate_datapoint : Validate an in-memory DataPoint2 without saving it
"""

from .Data_Standard_2 import DataPoint2, SimulatedDataPoint2, VERSION
from .Combine_Files import combine_files
from .validate import validate_file, validate_datapoint, ValidationReport, DataStandardValidationError

__version__ = VERSION

__all__ = [
    "DataPoint2",
    "SimulatedDataPoint2",
    "combine_files",
    "validate_file",
    "validate_datapoint",
    "ValidationReport",
    "DataStandardValidationError",
]