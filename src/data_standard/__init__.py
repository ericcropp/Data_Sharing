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
"""

__version__ = "0.0.0"

from .Data_Standard_2 import DataPoint2, SimulatedDataPoint2
from .Combine_Files import combine_files

__all__ = [
    "DataPoint2",
    "SimulatedDataPoint2",
    "combine_files",
]