"""Data Standard 2: Standardized Format for Accelerator Physics Data

This module provides a comprehensive framework for representing, validating, and saving
standardized data points from accelerator physics simulations and experiments.

Key Features:
-------------
- Hierarchical observable data structure with flexible dimensionality
- Support for scalar measurements, 1D waveforms, 2D images, and ParticleGroup distributions
- Lattice configuration management (to be replaced by PALS lattice standard)
- Automatic ID generation based on data content for reproducibility
- HDF5 serialization with metadata preservation
- Summary extraction for quick querying
- Simulation and experimental run metadata tracking

Data Organization:
------------------
Data is organized into observables with the following structure:
- batch_dims: Number of batch dimensions (e.g., parameter sweeps)
- feature_dims: Number of feature dimensions (0=scalar, 1=waveform, 2=image)
- shots_per_batch: Number of shots per batch
- location: Physical or logical location(s) in the beamline (distance from cathode or component name)
- data_names: Names of the data fields (e.g., 'xrms' on a screen)
- units: Physical units with automatic prefix handling
- control: Boolean flag indicating control vs measured parameters

Main Classes:
-------------
SingleObservable:
    Represents a single observable measurement or simulation output.
    Handles validation of data dimensions, units, locations, and data types.
    Supports scalar values, multi-dimensional arrays, and ParticleGroup objects.

Observables (list):
    Container for multiple SingleObservable instances.
    Provides methods to add observables and validate consistency.

Lattice:
    Stores beamline configuration information.
    Can reference external lattice files or embed lattice definitions.
    Supports PV (Process Variable) tables for experimental data.

Summary:
    Manages summary information for fast data point queries.
    Extracts specified observables at particular locations.

RunInformation:
    Tracks metadata about data origin (source, date, notes).

DataPoint2:
    Main container class representing a complete standardized data point.
    Combines observables, lattice, summary, and run information.
    Generates unique IDs based on data content (MD5 hash).
    Provides HDF5 serialization with proper handling of ParticleGroup objects.

SimulationMetadata:
    Stores simulation-specific metadata (start/end times, code version, input files).

SimulatedDataPoint2 (extends DataPoint2):
    Specialized version of DataPoint2 that includes simulation metadata.

Usage Example:
--------------
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

Exceptions:
-----------
TypeError: Raised for invalid data types
ValueError: Raised for invalid values or dimension mismatches
AssertionError: Raised for missing required information

Dependencies:
-------------
- numpy: Array operations
- pandas: Data frame handling (optional for inputs)
- pmd_beamphysics: ParticleGroup objects for particle distributions
- h5py: HDF5 file I/O
- hashlib: MD5 hash generation for unique IDs
- json: JSON serialization for hashing
- os: File system operations
- copy: Deep copying for ParticleGroup handling

Version: 2026-01-26
"""
import numpy as np
import pandas as pd
from pmd_beamphysics import ParticleGroup, units
import hashlib
import json
import h5py
import os
import copy

VERSION = '2026-01-26'

def unit_checker(unit):
    """
    Checks if the provided unit is valid.
    Args:
        unit (str): Unit string to check.

    Returns:
        openpmd_beamphysics.units: Valid unit if recognized, else "Custom_Unit".
    """
    valid_unit = "Custom Unit"
    prefix = 1
    if not isinstance(unit, str):
        raise ValueError("unit must be a string, received type: {}".format(type(unit)))
    
    # Handle ASCII 'u' as alias for micro 'µ' (common in many contexts where Unicode is problematic)
    if unit.startswith('u') and len(unit) > 1:
        # Check if 'u' followed by a known unit (e.g., 'um', 'us', 'uA')
        base_unit = unit[1:]
        if base_unit in units.known_unit:
            valid_unit = units.known_unit[base_unit]
            prefix = 1e-6  # micro = 1e-6
            return float(prefix), valid_unit
    
    if unit in units.known_unit.keys():
        valid_unit = units.known_unit[unit]
        prefix = 1
    else:
        # Check for prefix in units.PREFIX_FACTOR or units.SHORT_PREFIX_FACTOR
        for pro in list(units.PREFIX_FACTOR.keys()) + list(units.SHORT_PREFIX_FACTOR.keys()):
            if unit.startswith(pro):
                base_unit = unit[len(pro):]
                if base_unit in units.known_unit:
                    valid_unit = units.known_unit[base_unit]
                    prefix = units.PREFIX_FACTOR.get(pro, units.SHORT_PREFIX_FACTOR.get(pro, 1))


    return float(prefix), valid_unit


# ==============================================
# SingleObservable: Core observable data class
# ==============================================
class SingleObservable:
    """
    Represents a single observable measurement or simulation output.

    This class handles data with flexible dimensionality:
    - Batch dimensions: For parameter sweeps or batch processing
    - Feature dimensions: 0 (scalar), 1 (waveform), 2 (image), etc.
    - Location/name dimensions: Multiple locations or data fields

    The class validates data structure and handles unit conversion.

    Attributes
    ----------
    batch_dims : tuple
        Tuple of batch dimension sizes. Last dimension is number of shots.
    num_feature_dims : int
        Number of feature dimensions (0=scalar, 1=waveform, 2=image).
    location : ndarray
        Physical or logical location(s) in the beamline (1D array).
    data : ndarray
        Observable data array with shape: concatenate(batch_dims, length_dim, feature_dims).
    data_name : str
        Name of the data field (single string).
    location_primary : bool
        If True, data is grouped by location; if False, by data type.
    attrs : dict
        Additional attributes. For non-scalar data (num_feature_dims>0), must include 'bin_size' and 'offset'.
    control : bool
        Flag indicating control/input parameter (True) vs measured/output (False).
    units : str or unit object
        Physical units with automatic prefix handling.
    unit_multiplier : float
        Multiplier for unit prefix conversion.
    num_length_dim : int
        0 if single location, 1 if multiple locations (calculated property).
    length_dim : tuple
        Shape of location dimension (calculated property).
    feature_dims : tuple
        Shape of feature dimensions (calculated property).

    Methods
    -------
    __init__(batch_dims, num_feature_dims, location, data, attrs, data_name, units, location_primary, control)
        Initialize a SingleObservable instance with data and metadata.
    data_dim_checker()
        Validates that data array dimensions match the specified structure.
    to_dict()
        Returns a dictionary representation of the observable (for control parameters).
    """
    def __init__(self, batch_dims=None, num_feature_dims=0, location=None, data=None, attrs=None, data_name=None, units=None, location_units=None, location_primary=True, control=False):
        """
        Initialize a SingleObservable instance.
        Args:
            batch_dims (tuple): Tuple of batch dimension sizes. Last is number of shots.
            num_feature_dims (int): Number of feature dimensions.
            location: Location(s) associated with the observable.
            data: Observable data array.
            attrs (dict): Additional attributes.
            data_name (str): Name of the data field.
            units: Physical units.
            location_units (str): Physical units for location data. Default None.
            location_primary (bool): Group by location if True.
            control (bool): True for control/input, False for measured/output.
        Raises:
            TypeError, ValueError: For invalid types or mismatched data.
        """
        # Validate and convert batch_dims to tuple
        if batch_dims is None:
            batch_dims = ()
        elif isinstance(batch_dims, int):
            raise TypeError("batch_dims must be a tuple, not an int. For previous batch_dims=0, use batch_dims=(). For batch_dims=1, use batch_dims=(n_shots,).")
        elif not isinstance(batch_dims, (list, tuple)):
            raise TypeError(f"batch_dims must be a list or tuple, got {type(batch_dims)}")
        else:
            batch_dims = tuple(batch_dims)
            if not all(isinstance(d, (int, np.integer)) and d > 0 for d in batch_dims):
                raise ValueError("All batch_dims values must be positive integers")
        
        if location is not None and not (
            isinstance(location, (str, int, float, list, np.ndarray))
        ):
            raise TypeError("location must be a string, number, list, or np.ndarray")
        if isinstance(location, (str, int, float)):
            location = np.array([location])

        # Validate location format
        if isinstance(location, np.ndarray):
            if location.ndim != 1:
                raise ValueError(f"location as ndarray must be 1-D, got {location.ndim}-D array")
        elif isinstance(location, list):
            if not all(isinstance(item, (str, int, float)) for item in location):
                raise ValueError("location as list must contain only str, int, or float values (no nested lists)")
            location = np.array(location)

        if not isinstance(location_primary, bool):
            raise ValueError("location_primary must be a boolean value")
        
        # Validate control is a single bool
        if not isinstance(control, bool):
            raise TypeError(f"control must be a single boolean value, got {type(control)}")
        
        # Validate data_name format - must be a single string
        if data_name is not None:
            if isinstance(data_name, (list, np.ndarray)):
                if len(data_name) == 0:
                    raise ValueError("data_name cannot be an empty list")
                elif len(data_name) == 1:
                    # Coerce single-element list to string
                    data_name = str(data_name[0])
                else:
                    raise ValueError(f"data_name must be a single string, got list of length {len(data_name)}")
            elif not isinstance(data_name, str):
                raise TypeError(f"data_name must be a string, got {type(data_name)}")
        
        # Validate and coerce data to np.array
        if data is not None:
            if not isinstance(data, np.ndarray):
                try:
                    data = np.array(data)
                except Exception as e:
                    raise TypeError(f"data must be convertible to np.ndarray, got type {type(data)}. Error: {e}")
        
        # Check if data contains ParticleGroup objects
        if data is not None and data.size > 0:
            flat_data = data.flatten()
            has_particlegroup = any(isinstance(item, ParticleGroup) for item in flat_data)
            
            if has_particlegroup:
                # If any element is a ParticleGroup, all must be ParticleGroup
                if not all(isinstance(item, ParticleGroup) for item in flat_data):
                    raise TypeError(f"If any element of data is a ParticleGroup, all elements must be ParticleGroup")
                
                # If all are ParticleGroup, set num_feature_dims to 0
                num_feature_dims = 0

        self.batch_dims = batch_dims
        self.num_feature_dims = num_feature_dims
        self.location = location
        self.location_units = location_units
        self.data_name = data_name
        self.location_primary = location_primary
        self.data = data
        self.attrs = attrs if attrs is not None else {}
        self.control = control
        
        # Validate bin_size and offset for non-scalar data
        if self.num_feature_dims > 0:
            if 'bin_size' not in self.attrs:
                raise ValueError(f"For non-scalar data (num_feature_dims={self.num_feature_dims}), 'bin_size' must be specified in attrs")
            if 'offset' not in self.attrs:
                raise ValueError(f"For non-scalar data (num_feature_dims={self.num_feature_dims}), 'offset' must be specified in attrs")
            
            # Coerce bin_size and offset to floats
            try:
                self.attrs['bin_size'] = float(self.attrs['bin_size'])
            except (ValueError, TypeError) as e:
                raise TypeError(f"bin_size must be convertible to float, got {type(self.attrs['bin_size'])}: {e}")
            
            try:
                self.attrs['offset'] = float(self.attrs['offset'])
            except (ValueError, TypeError) as e:
                raise TypeError(f"offset must be convertible to float, got {type(self.attrs['offset'])}: {e}")
            
            # Assert that bin_size and offset are floats
            assert isinstance(self.attrs['bin_size'], float), f"bin_size must be float, got {type(self.attrs['bin_size'])}"
            assert isinstance(self.attrs['offset'], float), f"offset must be float, got {type(self.attrs['offset'])}"

        if self.location_primary:
            if len(location) != 1:
                raise ValueError(f"When location_primary is True, location must have exactly 1 element, got {len(location)}")
        
        prefix, valid_units = unit_checker(units)
        self.unit_multiplier = prefix
        self.units = units if valid_units == "Custom Unit" else valid_units
        
        if self.data is not None:
            self.data_dim_checker()

    @property
    def num_length_dim(self):
        """Number of location dimensions: 0 if single location, 1 if multiple."""
        if self.location is None:
            return 0
        return 0 if len(self.location) == 1 else 1
    
    @property
    def length_dim(self):
        """Shape of location dimension."""
        if self.num_length_dim == 0:
            return ()
        return (len(self.location),)
    
    @property
    def feature_dims(self):
        """Calculated feature dimensions from data shape."""
        if self.data is None:
            return tuple()
        expected_prefix_dims = len(self.batch_dims) + self.num_length_dim
        return self.data.shape[expected_prefix_dims:expected_prefix_dims + self.num_feature_dims]
    
    @property
    def units_str(self):
        """String representation of units for serialization."""
        if isinstance(self.units, str):
            return self.units
        return getattr(self.units, "unitSymbol", str(self.units))
    
    @property
    def has_particlegroup(self):
        """Check if data contains ParticleGroup objects."""
        if self.data is None or self.data.size == 0:
            return False
        flat_data = self.data.flatten()
        return any(isinstance(item, ParticleGroup) for item in flat_data)

    def data_dim_checker(self):
        """
        Validates that data array dimensions match: concatenate(batch_dims, length_dim, feature_dims).
        
        Raises:
            ValueError: If data dimensions don't match the expected structure.
        """
        expected_shape = self.batch_dims + self.length_dim + tuple([1] * self.num_feature_dims)
        expected_ndim = len(self.batch_dims) + self.num_length_dim + self.num_feature_dims
        
        if len(self.data.shape) != expected_ndim:
            raise ValueError(
                f"data must have {expected_ndim} dimensions "
                f"(batch_dims={len(self.batch_dims)}, length_dim={self.num_length_dim}, num_feature_dims={self.num_feature_dims}), "
                f"but got {len(self.data.shape)} dimensions with shape {self.data.shape}"
            )
        
        # Validate batch dimensions match
        for i, expected_size in enumerate(self.batch_dims):
            if self.data.shape[i] != expected_size:
                raise ValueError(
                    f"data batch dimension {i} should be {expected_size} but got {self.data.shape[i]}"
                )
        
        # Validate location dimension matches
        if self.num_length_dim == 1:
            loc_dim_idx = len(self.batch_dims)
            if self.data.shape[loc_dim_idx] != len(self.location):
                raise ValueError(
                    f"data location dimension should be {len(self.location)} but got {self.data.shape[loc_dim_idx]}"
                )
            
    def to_dict(self):
        """
        Returns a dictionary representation of the input.
        Returns:
            dict: Dictionary with input attributes.
        """
        # Ensure units is always a string for JSON serialization
        if isinstance(self.units, str):
            units_str = self.units
        else:
            units_str = getattr(self.units, "unitSymbol", str(self.units))
        if self.control:
            return {
                "name": self.data_name,
                "value": self.data,
                "location": self.location,
                "units": units_str,
        }
        else:
            return {}


# ==============================================
# Lattice: Beamline configuration
# ==============================================
class Lattice:
    """
    Represents lattice configuration for the data standard.  This will be replaced by PALS lattice standard in the future.

    Attributes
    ----------
    lattice_location : str
        Location or identifier of the lattice (e.g., 'FACET-II', 'included').
    lattice_files : dict or list
        Dictionary of {filename: contents} or list of file paths.
    PV_table : dict
        Dictionary of process variable names and their values.

    Methods
    -------
    __init__(lattice_location, lattice_files, PV_table)
        Initialize Lattice instance.
    process_lattice_files(lattice_files)
        Loads lattice files from a list or accepts a dict directly.
    add_lattice(lattice_location, lattice_files, PV_table)
        Adds lattice location and files.
    lattice_checker(allow_blank)
        Validates lattice configuration.
    """
    def __init__(self, lattice_location=None, lattice_files=None,PV_table=None):
        """
        Initialize Lattice instance.
        Args:
            lattice_location (str): Location of the lattice.
            lattice_files (list or dict): Lattice files or their contents.
        """
        self.add_lattice(lattice_location, lattice_files, PV_table)

    

    def process_lattice_files(self, lattice_files):
        """
        Loads lattice files from a list or accepts a dict directly.
        Args:
            lattice_files (list or dict): Lattice files or their contents.
        Returns:
            self: The Lattice instance.
        Raises:
            TypeError, FileNotFoundError: For invalid types or missing files.
        """
         # Accept dict directly
        if isinstance(lattice_files, dict):
            self.lattice_files = lattice_files
            return self
        if lattice_files is not None:
            if not isinstance(lattice_files, list):
                raise TypeError("lattice_files must be a list or dict")
            lattice_files_temp = {}
            for file in lattice_files:
                if not isinstance(file, str):
                    raise TypeError("Each item in lattice_files must be a string")
                if not os.path.isfile(file):
                    raise FileNotFoundError(f"Lattice file '{file}' does not exist.")
                with open(file, "r") as f:
                    lattice_files_temp[file] = f.read()
            self.lattice_files = lattice_files_temp
        return self

    def add_lattice(self, lattice_location, lattice_files=None, PV_table=None):
        """
        Adds lattice location and files.
        Args:
            lattice_location (str): Location of the lattice.
            lattice_files (list or dict): Lattice files or their contents.
            PV_table (dict): Optional dictionary of PVs and their values.
        """
        self.lattice_location = lattice_location
        self.lattice_files = lattice_files if lattice_files is not None else []
        self.PV_table = PV_table if PV_table is not None else {}
        if self.lattice_files and isinstance(self.lattice_files, list) and all(isinstance(f, str) for f in self.lattice_files):
            self.process_lattice_files(self.lattice_files)
    
    @property
    def has_included_files(self):
        """Check if lattice files are included."""
        return self.lattice_location == 'included'
    
    @property
    def is_empty(self):
        """Check if lattice is empty."""
        return (self.lattice_location is None or self.lattice_location == "") and len(self.lattice_files) == 0

    def lattice_checker(self,allow_blank = False):
        """
        Validates lattice configuration.
        Args:
            allow_blank (bool): If True, allows blank lattice configuration.
        Raises:
            TypeError, ValueError: For invalid types or missing required values.
        """
        if allow_blank and (self.lattice_location is None or self.lattice_location == "") and len(self.lattice_files) == 0:
            return
        if not isinstance(self.lattice_location, str):
            raise TypeError("lattice_location must be a string, received type: {}".format(type(self.lattice_location)))
        if self.lattice_location == "":
            raise ValueError("lattice_location must not be empty")
        if self.lattice_location == 'included' and (self.lattice_files is None or len(self.lattice_files) == 0):
            raise ValueError("lattice_files must be provided if lattice_location is 'included'")
        if not isinstance(self.lattice_files, (list, dict)):
            raise TypeError("lattice_files must be a list or dict")
        if isinstance(self.lattice_files, list):
            for file in self.lattice_files:
                if not isinstance(file, str):
                    raise TypeError("Each item in lattice_files must be a string")
        if isinstance(self.lattice_files, dict):
            for fname, contents in self.lattice_files.items():
                if not isinstance(fname, str):
                    raise TypeError("Each key in lattice_files dict must be a string (filename)")
                if not isinstance(contents, str):
                    raise TypeError("Each value in lattice_files dict must be a string (file contents)")

class Observables(list):
    """
    Container for output data for the data standard.

    This class extends Python's built-in list to store SingleObservable objects
    with additional validation methods.

    Attributes
    ----------
    Inherits from list, contains SingleObservable objects.
    Each element is a SingleObservable instance representing one measurement or output.

    Methods
    -------
    __init__(observable_list)
        Initialize Observables instance from a list.
    add_observable(batch_dims, num_feature_dims, location, data, attrs, data_name, units, location_primary, control)
        Adds an observable to the list.
    observable_checker(allow_blank)
        Validates all observables in the list.
    """
    def __init__(self, observable_list=None):
        """
        Initialize Observables instance.
        Args:
            observable_list (list): List of observable dictionaries.
        """
        super().__init__()
        observable_list = observable_list if observable_list is not None else []

        for observable in observable_list:
            self.add_observable(observable["location"], observable["datum"], observable["control"], observable["num_shots"], observable["units"], observable.get("attrs"), observable.get("datum_name", ""),observable.get("location_primary", True))

    def add_observable(self, batch_dims=None, num_feature_dims=0, location=None, data=None, attrs=None, data_name=None, units=None, location_units=None, location_primary=True, control=False):
        """
        Adds an observable to the Observables list.
        Args:
            batch_dims (list): List of batch dimension sizes.
            num_feature_dims (int): Number of feature dimensions.
            location: Location(s) associated with the observable.
            data: Observable data array.
            attrs (dict): Additional attributes.
            data_name (str): Name of the data field.
            units: Physical units.
            location_units (str): Physical units for location data. Default None.
            location_primary (bool): Group by location if True.
            control (bool): True for control/input, False for measured/output.
        """
        output = SingleObservable(
            batch_dims=batch_dims,
            num_feature_dims=num_feature_dims,
            location=location,
            data=data,
            attrs=attrs,
            data_name=data_name,
            units=units,
            location_units=location_units,
            location_primary=location_primary,
            control=control
        )
        self.append(output)
        self.observable_checker(allow_blank=True)

    def observable_checker(self,allow_blank = False):
        """
        Validates observables in the Observables list.
        Args:
            allow_blank (bool): If True, allows blank outputs.
        Raises:
            TypeError, ValueError, AssertionError: For invalid types or mismatched data.
        """
        if allow_blank and len(self) == 0:
            return
        
        # Check that all observables have the same batch_dims
        if len(self) > 0:
            first_batch_dims = self[0].batch_dims
            for i, observable in enumerate(self):
                if observable.batch_dims != first_batch_dims:
                    raise ValueError(
                        f"All observables must have the same batch_dims. "
                        f"Observable at index 0 has batch_dims={first_batch_dims}, "
                        f"but observable at index {i} has batch_dims={observable.batch_dims}"
                    )
        
        for observable in self:
            observable.data_dim_checker()

# ==============================================
# Summary: Fast data point querying
# ==============================================
class Summary:
    """
    Represents summary information for a data point.

    Attributes
    ----------
    summary : dict
        Dictionary containing ID and run information (populated by get_summary()).

    Methods
    -------
    __init__()
        Initialize Summary instance.
    """
    def __init__(self):
        """
        Initialize Summary instance.
        """
        self.summary = {}

# ==============================================
# RunInformation: Metadata tracking
# ==============================================
class RunInformation:
    """
    Stores metadata about the run for the data standard.

    Attributes
    ----------
    source : str
        Source of the data (e.g., 'FACET-II', 'AWA', 'Impact-T simulation').
    date : str
        Date of the run or data collection.
    notes : str
        Additional notes or comments about the run.

    Methods
    -------
    __init__(run_information)
        Initialize RunInformation instance.
    add_run_information(source, date, notes)
        Adds run information.
    run_info_checker(allow_blank)
        Validates run information.
    """
    def __init__(self, run_information):
        """
        Initialize RunInformation instance.
        Args:
            run_information (dict): Dictionary with run metadata.
        """
        if run_information == {}:
            run_information = {'source':"", 'date':"", 'notes':""}
        self.add_run_information(run_information.get('source',""), run_information.get('date',""), run_information.get('notes',""))
        self.run_info_checker(allow_blank=True)
      
    def add_run_information(self, source, date, notes):
        """
        Adds run information.
        Args:
            source (str): Source of the run.
            date (str): Date of the run.
            notes (str): Additional notes.
        """
        self.source = source
        self.date = date
        self.notes = notes
        self.run_info_checker(allow_blank=True)
    def run_info_checker(self,allow_blank = False):
        """
        Validates run information.
        Args:
            allow_blank (bool): If True, allows blank run information.
        Raises:
            TypeError, ValueError: For invalid types or missing required values.
        """
        if allow_blank and self.source=="" and self.date=="" and self.notes=="":
            return
        if not isinstance(self.source, str):
            raise TypeError("source must be a string")
        if self.source=="":
            raise ValueError("source must not be empty")
        if not isinstance(self.date, str):
            raise TypeError("date must be a string")
        if self.date=="":
            raise ValueError("date must not be empty")
        if not isinstance(self.notes, str):
            raise TypeError("notes must be a string")
        if self.notes=="":
            raise ValueError("notes must not be empty")


# ==============================================
# DataPoint2: Main standardized data container
# ==============================================
class DataPoint2:
    """
    Main class representing a standardized data point for the data standard.

    Attributes
    ----------
    lattice : Lattice
        Lattice configuration information.
    observables : Observables
        List of SingleObservable objects containing measurements and outputs.
    summary : Summary
        Summary information for fast data point querying.
    ID : str
        Unique MD5 hash identifier generated from data content.
    run_information : RunInformation
        Metadata about the data source, date, and notes.
    scalar_output_list : list
        List of scalar output names (legacy, may be deprecated).

    Methods
    -------
    __init__(lattice_location, lattice_files, observable_list, summary_keys, summary_location, ID, run_information)
        Initialize DataPoint2 instance.
    make_ID()
        Generates a unique MD5 hash ID based on data content.
    add_lattice(lattice_location, lattice_files, PV_table)
        Adds lattice information to the data point.
    add_run_information(source, date, notes)
        Adds run metadata to the data point.
    add_observable(batch_dims, feature_dims, shots_per_batch, location, data, attrs, data_names, units, location_primary, control)
        Adds an observable measurement or output to the data point.
    add_summary(summary_keys, summary_location)
        Specifies keys to include in summary for fast querying.
    get_summary()
        Extracts summary data based on specified keys and location.
    checker()
        Validates all components of the data point.
    finalize()
        Finalizes the data point (generates ID, extracts summary, validates).
    saveHDF5(fileloc)
        Saves the data point to HDF5 format with proper structure.
    """
    def __init__(self,  lattice_location=None, lattice_files=None,
                 observable_list=None, ID="", run_information=None
                 ):
        """
        Initialize DataPoint2 instance.
        Args:
            scalar_inputs: Scalar inputs.
            input_distribution: Input distribution.
            lattice_location: Location of the lattice.
            lattice_files: Lattice files or their contents.
            output_list: List of output dictionaries.
            summary_keys: Keys to include in summary.
            summary_location: Location for summary extraction.
            ID (str): Unique identifier.
            run_information: Run metadata.
            outputs: Outputs list.
            summary: Summary information.
            input_distribution_attrs: Attributes for input distribution.
        """
        # self.inputs = Inputs(scalar_inputs=scalar_inputs, input_distribution=input_distribution,
        #                      input_distribution_attrs=input_distribution_attrs)
        self.lattice = Lattice(lattice_location=lattice_location, lattice_files=lattice_files)
        self.observables = Observables(observable_list=observable_list)
        self.summary = Summary()
        self.ID = ""
        self.run_information = RunInformation(run_information if run_information is not None else {})
        self.scalar_output_list = []
    def make_ID(self):
        """
        Generates a unique ID for the data point.
        Returns:
            self: The DataPoint2 instance with updated ID.
        """
        import hashlib
        
        # Create a hash based on the actual data bytes, not JSON serialization
        hasher = hashlib.md5()
        
        # Sort observables by location and name for consistency
        sorted_obs = sorted(
            [obs for obs in self.observables],
            key=lambda x: (str(x.location), str(x.data_name))
        )
        
        for obs in sorted_obs:
            # Hash the location
            hasher.update(str(obs.location).encode('utf-8'))
            
            # Hash the data_name
            hasher.update(str(obs.data_name).encode('utf-8'))
            
            # Hash the actual data
            if obs.data is not None and obs.data.size > 0:
                # Skip ParticleGroup objects
                if obs.data.dtype != object:
                    # Convert to float64 and create bytes representation
                    data_float = np.asarray(obs.data, dtype=np.float64)
                    # Round to fixed precision
                    data_rounded = np.round(data_float, decimals=10)
                    # Use tobytes() for deterministic byte representation
                    hasher.update(data_rounded.tobytes())
            
            # Hash units
            if isinstance(obs.units, str):
                hasher.update(obs.units.encode('utf-8'))
            else:
                hasher.update(str(obs.units).encode('utf-8'))
        
        # Add lattice location to hash
        hasher.update(str(self.lattice.lattice_location).encode('utf-8'))
        
        self.ID = hasher.hexdigest()
        return self
    
    @property
    def filename(self):
        """Generated filename based on ID."""
        return f"{self.ID}.h5"

    def add_lattice(self, lattice_location=None, lattice_files=None, PV_table=None):
        """
        Adds lattice information to the data point.
        Args:
            lattice_location: Location of the lattice.
            lattice_files: Lattice files or their contents.
            PV_table (dict): Optional dictionary of PVs and their values.
        """
        self.lattice.add_lattice(lattice_location, lattice_files, PV_table)
        return self
    
    def add_run_information(self, source=None, date=None, notes=None):
        """
        Adds run information to the data point.
        Args:
            source (str): Source of the run.
            date (str): Date of the run.
            notes (str): Additional notes.
        Returns:
            self: The DataPoint2 instance.
        """
        self.run_information.add_run_information(source, date, notes)
        return self

    def add_observable(self, batch_dims=None, num_feature_dims=0, location=None, data=None, attrs=None, data_name=None, units=None, location_units=None, location_primary=True, control=False):
        """
        Adds an observable to the data point.
        Args:
            batch_dims (list): List of batch dimension sizes.
            num_feature_dims (int): Number of feature dimensions.
            location: Location(s) associated with the observable.
            data: Observable data array.
            attrs (dict): Additional attributes.
            data_name (str): Name of the data field.
            units: Physical units.
            location_units (str): Physical units for location data. Default None.
            location_primary (bool): Group by location if True.
            control (bool): True for control/input, False for measured/output.
        Returns:
            self: The DataPoint2 instance.
        """
        self.observables.add_observable(batch_dims, num_feature_dims, location, data, attrs, data_name, units, location_units, location_primary=location_primary, control=control)
        
        return self


    
    def get_summary(self):
        """
        Extracts summary data for the data point.
        
        Includes only ID and simulation metadata (if present).
        
        Returns:
            self: The DataPoint2 instance with populated summary.
        """
        summary = {}
        summary["ID"] = self.ID
        
        if hasattr(self, "simulation_metadata") and isinstance(self.simulation_metadata, SimulationMetadata):
            summary["simulation_start"] = self.simulation_metadata.simulation_start
            summary["simulation_end"] = self.simulation_metadata.simulation_end
            summary["simulation_code"] = self.simulation_metadata.simulation_code
            summary["simulation_version"] = self.simulation_metadata.simulation_version
        
        self.summary.summary = summary
        return self

    def checker(self):
        """
        Validates all components of the data point.
        
        Checks:
        - Lattice configuration validity
        - Observables structure and dimensions
        - Run information completeness
        - Summary structure
        - Simulation metadata (if present)
        
        Returns:
            self: The DataPoint2 instance.
        
        Raises:
            TypeError, ValueError: If any validation fails.
        """
        self.lattice.lattice_checker()
        self.observables.observable_checker()
        self.run_information.run_info_checker()
        if hasattr(self, "simulation_metadata") and isinstance(self.simulation_metadata, SimulationMetadata):
            self.simulation_metadata.sim_data_checker()
        return self

    def finalize(self):
        """
        Finalizes and validates the data point before saving.
        
        This method should be called before saving to ensure:
        1. Unique ID is generated based on data content
        2. Summary is extracted for fast querying
        3. All validations pass
        
        Returns:
            self: The DataPoint2 instance.
        
        Raises:
            TypeError, ValueError: If validation fails.
        """
        self.make_ID()
        self.get_summary()
        self.checker()
        return self

    def saveHDF5(self, fileloc=None):
        """
        Saves the data point to HDF5 format with proper handling of all data types.
        
        HDF5 Structure:
        ---------------
        /lattice/
            lattice_location (dataset)
            lattice_files/ (group, if lattice_location='included')
                <filename> (dataset): File contents
            PV_table (attributes): Process variable values
            simulation_input_file (dataset, if SimulatedDataPoint2)
        
        /observables/
            <location_name>/ (group, if location_primary=True)
                <data_name> (dataset): Observable data
                    Attributes: location, control, units, num_feature_dims, custom attrs
                <data_name>_<i>_<j>_... (group): ParticleGroup data matching batch_dims indices
            multi_location_data/ (group, if location_primary=False)
                DATA_LOCATIONS (dataset): Location data for all datasets in this group
                    Attributes: units (location units), num_feature_dims
                <data_name> (dataset): Observable data array
                    Attributes: control, units, num_feature_dims, custom attrs
                <data_name>_<i>_<j>_... (group): ParticleGroup data matching batch_dims indices
        
        Root Attributes:
        ----------------
        - ID: Unique identifier (MD5 hash)
        - run_information_source: Data source
        - run_information_date: Date of run
        - run_information_notes: Additional notes
        - Data_Standard_Version: Version of this standard
        - <summary_key>: Summary values (stored as arrays for lists, scalars otherwise)
        - summary_location: Location used for summary extraction
        
        Args:
            fileloc (str): File location or directory to save the HDF5 file.
                          If directory, filename will be {ID}.h5
                          If None, saves as {ID}.h5 in current directory
        """
        self.finalize()
        
        # Determine output filename
        if fileloc is None:
            filename = self.filename
        elif os.path.isdir(fileloc):
            filename = os.path.join(fileloc, self.filename)
        else:
            filename = fileloc
        
        with h5py.File(filename, "w") as f:
            # ==========================================
            # Save lattice configuration
            # ==========================================
            lattice_grp = f.create_group("lattice")
            lattice_grp.create_dataset("lattice_location", data=self.lattice.lattice_location)
            
            # Save lattice files if provided as dictionary
            if isinstance(self.lattice.lattice_files, dict):
                lattice_files_grp = lattice_grp.create_group("lattice_files")
                for fname, contents in self.lattice.lattice_files.items():
                    lattice_files_grp.create_dataset(fname, data=np.bytes_(contents))
            
            # Save PV_table as attributes if present
            if hasattr(self.lattice, "PV_table") and isinstance(self.lattice.PV_table, dict):
                for k, v in self.lattice.PV_table.items():
                    lattice_grp.attrs[k] = v

            # ==========================================
            # Save observables
            # ==========================================
            observables_grp = f.create_group("observables")
            
            # Collect observables by location_primary for validation
            multi_loc_observables = [obs for obs in self.observables if not obs.location_primary]
            
            # Validate that all multi_location_data observables have same num_feature_dims
            if multi_loc_observables:
                first_num_feature_dims = multi_loc_observables[0].num_feature_dims
                for obs in multi_loc_observables:
                    if obs.num_feature_dims != first_num_feature_dims:
                        raise ValueError(
                            f"All observables in multi_location_data must have same num_feature_dims. "
                            f"Found {obs.num_feature_dims} for '{obs.data_name}' but expected {first_num_feature_dims}"
                        )
            
            for i, observable in enumerate(self.observables):

                if observable.location_primary == False:
                    # Create or get the "multi_location_data" group
                    if "multi_location_data" not in observables_grp:
                        multi_loc_grp = observables_grp.create_group("multi_location_data")
                        
                        # Create DATA_LOCATIONS dataset on first observable
                        location_value = observable.location if isinstance(observable.location, np.ndarray) else np.array(observable.location)
                        # Handle string arrays for HDF5
                        if location_value.dtype.kind in ['U', 'O']:  # Unicode or Object dtype
                            loc_dataset = multi_loc_grp.create_dataset("DATA_LOCATIONS", data=location_value.astype('S'))
                        else:
                            loc_dataset = multi_loc_grp.create_dataset("DATA_LOCATIONS", data=location_value)
                        loc_dataset.attrs["units"] = observable.location_units
                        loc_dataset.attrs["num_feature_dims"] = observable.num_feature_dims
                    else:
                        multi_loc_grp = observables_grp["multi_location_data"]
                        
                        # Verify location consistency
                        existing_location = multi_loc_grp["DATA_LOCATIONS"][:]
                        current_location = observable.location if isinstance(observable.location, np.ndarray) else np.array(observable.location)
                        # Handle string comparison
                        if existing_location.dtype.kind == 'S':
                            existing_location = existing_location.astype('U')
                        if current_location.dtype.kind in ['U', 'O']:
                            current_location = current_location.astype('S').astype('U')
                        if not np.array_equal(existing_location, current_location):
                            raise ValueError(
                                f"All observables in multi_location_data must have same location. "
                                f"Found {current_location} for '{observable.data_name}' but expected {existing_location}"
                            )

                    # Handle ParticleGroup objects specially
                    if observable.has_particlegroup:
                        # Write each ParticleGroup to its own HDF5 group with index-based naming
                        for idx in np.ndindex(observable.data.shape):
                            pg = observable.data[idx]
                            # Create path using indices: name_i_j_k for batch_dims
                            # For 0-D arrays (batch_dims=()), idx is (), so use '0' as the index
                            if len(idx) == 0:
                                idx_str = '0'
                            else:
                                idx_str = '_'.join(map(str, idx))
                            path = observable.data_name + '_' + idx_str
                            pg_grp = multi_loc_grp.create_group(path)
                            pg.write(pg_grp)
                        # Note: No dataset needed - ParticleGroups are directly accessible by index
                        # Skip attribute setting for ParticleGroups as they don't have a dataset
                        continue
                    else:
                        # Regular numeric data
                        out_grp = multi_loc_grp.create_dataset(observable.data_name, data=np.array(observable.data))

                    # Set dataset attributes (no location here - it's in DATA_LOCATIONS)
                    out_grp.attrs["control"] = observable.control
                    out_grp.attrs["num_feature_dims"] = observable.num_feature_dims
                    out_grp.attrs["units"] = observable.units_str
                    out_grp.attrs["unit_multiplier"] = observable.unit_multiplier

                    for k, v in observable.attrs.items():
                        out_grp.attrs[k] = v

                # --- Handle location_primary=True: Group by location ---
                else:
                    # Validate location structure
                    assert isinstance(observable.location, (list, np.ndarray)), "observable.location must be a list or np array when location_primary is True, got {}".format(type(observable.location))
                    assert len(observable.location) == 1, "observable.location must have length 1 when location_primary is True, got length {}".format(len(observable.location))
                    
                    # Create group for this location
                    location_str = str(observable.location[0])
                    if location_str not in observables_grp:
                        out_grp = observables_grp.create_group(location_str)
                    else:
                        out_grp = observables_grp[location_str]

                    # Handle ParticleGroup data
                    if observable.has_particlegroup:
                        # Write each ParticleGroup to its own HDF5 group with index-based naming
                        for idx in np.ndindex(observable.data.shape):
                            pg = observable.data[idx]
                            # Create path using indices: name_i_j_k for batch_dims
                            # For 0-D arrays (batch_dims=()), idx is (), so use '0' as the index
                            if len(idx) == 0:
                                idx_str = '0'
                            else:
                                idx_str = '_'.join(map(str, idx))
                            path = observable.data_name + '_' + idx_str
                            pg_grp = out_grp.create_group(path)
                            pg.write(pg_grp)
                        # Note: No dataset needed - ParticleGroups are directly accessible by index
                        # Skip attribute setting for ParticleGroups as they don't have a dataset
                        continue
                    else:
                        dataset = out_grp.create_dataset(observable.data_name, data=np.array(observable.data))
                    
                    # Set dataset attributes
                    dataset.attrs["location"] = location_str
                    dataset.attrs["control"] = observable.control
                    dataset.attrs["num_feature_dims"] = observable.num_feature_dims
                    dataset.attrs["units"] = observable.units_str
                    dataset.attrs["unit_multiplier"] = observable.unit_multiplier
                    
                    # Assert that location in metadata matches group name
                    assert dataset.attrs["location"] == location_str, \
                        f"Location mismatch: group name is '{location_str}' but metadata has '{dataset.attrs['location']}'"
                    
                    for k, v in observable.attrs.items():
                        dataset.attrs[k] = v

            # Save simulation input file if this is a SimulatedDataPoint2
            if hasattr(self, "simulation_metadata") and isinstance(self.simulation_metadata, SimulationMetadata):
                lattice_grp.create_dataset("simulation_input_file", data=np.bytes_(self.simulation_metadata.simulation_input_file))

            # ==========================================
            # Save metadata as root attributes
            # ==========================================
            f.attrs["ID"] = self.ID
            # Store batch_dims as array if present
            if len(self.observables) > 0 and self.observables[0].batch_dims:
                f.attrs["batch_dims"] = np.array(self.observables[0].batch_dims)
            else:
                f.attrs["batch_dims"] = np.array([])
            f.attrs["run_information_source"] = self.run_information.source
            f.attrs["run_information_date"] = self.run_information.date
            f.attrs["run_information_notes"] = self.run_information.notes
            f.attrs["Data_Standard_Version"] = VERSION
            
            # Save summary data as attributes
            for key, value in self.summary.summary.items():
                # Convert to appropriate type for HDF5 attributes
                if isinstance(value, list):
                    if value:  # non-empty list
                        try:
                            # Try to store as numeric array
                            f.attrs[key] = np.array([float(item) for item in value])
                        except (ValueError, TypeError):
                            # Store as string array if not numeric
                            f.attrs[key] = [str(item) for item in value]
                    else:
                        f.attrs[key] = []
                elif isinstance(value, np.ndarray):
                    # Store array directly
                    f.attrs[key] = value
                elif isinstance(value, (int, float, np.integer, np.floating)):
                    f.attrs[key] = value
                else:
                    f.attrs[key] = str(value)

# ==============================================
# SimulationMetadata: Simulation-specific data
# ==============================================
class SimulationMetadata:
    """
    Stores simulation metadata for the data standard.

    Attributes
    ----------
    simulation_start : str
        Simulation start time or timestamp.
    simulation_end : str
        Simulation end time or timestamp.
    simulation_code : str
        Name of the simulation code (e.g., 'Impact-T', 'Astra', 'OPAL').
    simulation_input_file : str
        Contents or path of the simulation input file.
    simulation_version : str
        Version of the simulation code used.

    Methods
    -------
    __init__(simulation_start, simulation_end, simulation_code, simulation_input_file, simulation_version)
        Initialize SimulationMetadata instance.
    add_simulation_data(simulation_start, simulation_end, simulation_code, simulation_input_file, simulation_version)
        Adds simulation metadata.
    sim_data_checker(allow_blank)
        Validates simulation metadata.
    """
    def __init__(self, simulation_start="", simulation_end="", simulation_code="", simulation_input_file="", simulation_version=""):
        """
        Initialize SimulationMetadata instance.
        Args:
            simulation_start (str): Simulation start time.
            simulation_end (str): Simulation end time.
            simulation_code (str): Simulation code name.
            simulation_input_file (str): Input file for simulation.
            simulation_version (str): Version of the simulation code.
        """
        self.add_simulation_data(simulation_start, simulation_end, simulation_code, simulation_input_file, simulation_version)
        self.sim_data_checker(allow_blank=True)

    def add_simulation_data(self, simulation_start, simulation_end, simulation_code, simulation_input_file, simulation_version):
        """
        Adds simulation metadata.
        Args:
            simulation_start (str): Simulation start time.
            simulation_end (str): Simulation end time.
            simulation_code (str): Simulation code name.
            simulation_input_file (str): Input file for simulation.
        """
        self.simulation_start = str(simulation_start)
        self.simulation_end = str(simulation_end)
        self.simulation_code = str(simulation_code)
        self.simulation_input_file = str(simulation_input_file)
        self.simulation_version = str(simulation_version)
        self.sim_data_checker(allow_blank=True)

    def sim_data_checker(self, allow_blank=False):
        """
        Validates simulation metadata.
        Args:
            allow_blank (bool): If True, allows blank simulation metadata.
        Raises:
            TypeError, ValueError: For invalid types or missing required values.
        """
        if allow_blank and self.simulation_start == "" and self.simulation_end == "" and self.simulation_code == "" and self.simulation_input_file == "":
            return
        if not isinstance(self.simulation_start, str):
            raise TypeError("simulation_start must be a string")
        if self.simulation_start=="":
            raise ValueError("simulation_start must not be empty")
        if not isinstance(self.simulation_end, str):
            raise TypeError("simulation_end must be a string")
        if self.simulation_end=="":
            raise ValueError("simulation_end must not be empty")
        if not isinstance(self.simulation_code, str):
            raise TypeError("simulation_code must be a string")
        if self.simulation_code=="":
            raise ValueError("simulation_code must not be empty")
        if not isinstance(self.simulation_input_file, str):
            raise TypeError("simulation_input_file must be a string")
        if self.simulation_input_file=="":
            raise ValueError("simulation_input_file must not be empty")
        if not isinstance(self.simulation_version, str):
            raise TypeError("simulation_version must be a string")
        if self.simulation_version=="":
            raise ValueError("simulation_version must not be empty")

# ==============================================
# SimulatedDataPoint2: Extended data point for simulations
# ==============================================
class SimulatedDataPoint2(DataPoint2):
    """
    Extends DataPoint2 to include simulation-specific metadata.

    Inherits all attributes from DataPoint2 and adds:

    Attributes
    ----------
    simulation_metadata : SimulationMetadata
        Simulation-specific metadata (start/end times, code name, version, input file).

    Inherited Attributes
    --------------------
    lattice, observables, summary, ID, run_information, scalar_output_list
        See DataPoint2 for details.

    Methods
    -------
    __init__(lattice_location, lattice_files, observable_list, ID, run_information, simulation_start, simulation_end, simulation_code, simulation_input_file, simulation_version)
        Initialize SimulatedDataPoint2 instance with simulation metadata.
    add_simulation_data(simulation_start, simulation_end, simulation_code, simulation_input_file, simulation_version)
        Adds or updates simulation metadata.

    Inherited Methods
    -----------------
    make_ID, add_lattice, add_run_information, add_observable, get_summary, checker, finalize, saveHDF5
        See DataPoint2 for details.
    """
    def __init__(self, lattice_location=None, lattice_files=None,
                 observable_list=None, ID="", run_information=None,
                 simulation_start=None, simulation_end=None, simulation_code="", simulation_input_file="",simulation_version=""):
        """
        Initialize SimulatedDataPoint2 instance.
        Args:
            scalar_inputs: Scalar inputs.
            input_distribution: Input distribution.
            lattice_location: Location of the lattice.
            lattice_files: Lattice files or their contents.
            output_list: List of output dictionaries.
            ID (str): Unique identifier.
            run_information: Run metadata.
            outputs: Outputs list.
            summary: Summary information.
            input_distribution_attrs: Attributes for input distribution.
            simulation_start (str): Simulation start time.
            simulation_end (str): Simulation end time.
            simulation_code (str): Simulation code name.
            simulation_input_file (str): Input file for simulation.
        """
        super().__init__(lattice_location=lattice_location, lattice_files=lattice_files, observable_list=observable_list, ID=ID, run_information=run_information)
        
        self.simulation_metadata = SimulationMetadata(
            simulation_start=str(simulation_start) if simulation_start is not None else "",
            simulation_end=str(simulation_end) if simulation_end is not None else "",
            simulation_code=str(simulation_code) if simulation_code is not None else "",
            simulation_input_file=str(simulation_input_file) if simulation_input_file is not None else "",
            simulation_version=str(simulation_version) if simulation_version is not None else ""
        )

    def add_simulation_data(self, simulation_start=None, simulation_end=None, simulation_code="", simulation_input_file="", simulation_version=""):
        """
        Adds simulation metadata.
        Args:
            simulation_start (str): Simulation start time.
            simulation_end (str): Simulation end time.
            simulation_code (str): Simulation code name.
            simulation_input_file (str): Input file for simulation.
        Returns:
            self: The SimulatedDataPoint2 instance.
        """
        self.simulation_metadata.add_simulation_data(simulation_start, simulation_end, simulation_code, simulation_input_file, simulation_version=simulation_version)
        return self
