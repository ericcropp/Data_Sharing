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
    - Shots per batch: Multiple shots within each batch
    - Location/name dimensions: Multiple locations or data fields

    The class validates data structure and handles unit conversion.

    Attributes
    ----------
    batch_dims : int
        Number of batch dimensions for parameter sweeps or batch processing.
    feature_dims : int
        Number of feature dimensions (0=scalar, 1=waveform, 2=image).
    shots_per_batch : int
        Number of shots per batch (0 if not using batch shots).
    location : ndarray
        Physical or logical location(s) in the beamline (1D array).
    data : ndarray
        Observable data array with shape determined by dimensions.
    data_names : ndarray
        Names of the data fields (1D string array).
    location_primary : bool
        If True, data is grouped by location; if False, by data type.
    attrs : dict
        Additional attributes (e.g., pixel calibration for images).
    control : bool
        Flag indicating control/input parameter (True) vs measured/output (False).
    units : str or unit object
        Physical units with automatic prefix handling.
    unit_multiplier : float
        Multiplier for unit prefix conversion.

    Methods
    -------
    __init__(batch_dims, feature_dims, shots_per_batch, location, data, attrs, data_names, units, location_primary, control)
        Initialize a SingleObservable instance with data and metadata.
    data_dim_checker()
        Validates that data array dimensions match the specified structure.
    to_dict()
        Returns a dictionary representation of the observable (for control parameters).
    """
    def __init__(self, batch_dims=0, feature_dims=0, shots_per_batch=0, location=None, data=None, attrs=None, data_names=None, units=None, location_primary=True,control=False):
        """
        Initialize a SingleObservable instance.
        Args:
            location: Location(s) associated with the output.
            datum: Output value(s).
            attrs (dict): Additional attributes.
            datum_name (str): Name of the output datum.
            datum_type (str): Type of datum ('scalar', 'image', 'distribution').
        Raises:
            TypeError, ValueError: For invalid types or mismatched data.
        """
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
        
            
            
        
        # Validate data_names format
        if data_names is not None and not (
            isinstance(data_names, (str, list, np.ndarray))
        ):
            raise TypeError("data_names must be a string, list, or np.ndarray")
        
        if isinstance(data_names, str):
            data_names = np.array([data_names])

        if isinstance(data_names, np.ndarray):
            if data_names.ndim != 1:
                raise ValueError(f"data_names as ndarray must be 1-D, got {data_names.ndim}-D array")
            if not all(isinstance(item, str) for item in data_names):
                raise ValueError("data_names as ndarray must contain only string values")
        elif isinstance(data_names, list):
            if not all(isinstance(item, str) for item in data_names):
                raise ValueError("data_names as list must contain only string values (no nested lists)")
            data_names = np.array(data_names)
        
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
                    raise TypeError(f"If any element of data is a ParticleGroup, all elements must be ParticleGroup, got {[type(item) for item in flat_data]}")
                
                # If all are ParticleGroup, set feature_dims to 0
                feature_dims = 0

        self.batch_dims = batch_dims
        self.feature_dims = feature_dims
        self.shots_per_batch = shots_per_batch

        self.location = location
        self.data_names = data_names
        self.location_primary = location_primary

        self.data = data
        self.attrs = attrs if attrs is not None else {}
        self.control = control

        
        if self.location_primary:
            if len(location) != 1:
                raise ValueError(f"When location_primary is True, location must have exactly 1 element, got {len(location)}")
        else:
            if len(self.data_names) != 1:
                raise ValueError(f"When location_primary is False, data_names must have exactly 1 element, got {len(self.data_names)}")

        
        prefix, valid_units = unit_checker(units)
        # print(prefix, valid_units)
        self.unit_multiplier = prefix
            
        self.units = units if valid_units == "Custom Unit" else valid_units
        if self.data is not None:
            self.data_dim_checker()

    def data_dim_checker(self):
        """
        Validates that data array dimensions match the specified batch, feature, and metadata dimensions.
        
        The expected total number of dimensions is calculated as:
        batch_dims + feature_dims + shots_per_batch + location_dim + names_dim + 1 (shots dimension)
        
        Raises:
            ValueError: If data dimensions don't match the expected structure.
        """
        if len(self.location) == 1:
            len_dim = 0
        elif len(self.location) > 1:
            len_dim = 1
        if len(self.data_names) == 1:
            names_dim = 0
        elif len(self.data_names) > 1:
            names_dim = 1
        num_dimensions = self.batch_dims + self.feature_dims + self.shots_per_batch + len_dim + names_dim + 1 # Shots dimension
        
        if len(np.shape(self.data)) != num_dimensions:
            raise ValueError(f"data must have {num_dimensions} dimensions based on provided batch_dims {self.batch_dims}, feature_dims {self.feature_dims}, shots_per_batch {self.shots_per_batch}, location {len_dim}, and data_names {names_dim}, but got {len(np.shape(self.data))} dimensions")
            
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
                "name": self.data_names,
                "value": self.data,
                "location": self.location,
                "units": units_str,
                # "description": self.description
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
    add_observable(batch_dims, feature_dims, shots_per_batch, location, data, attrs, data_names, units, location_primary, control)
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

    def add_observable(self, batch_dims=0, feature_dims=0, shots_per_batch=0, location=None, data=None, attrs=None, data_names=None, units=None, location_primary=True,control=False):
        """
        Adds an output to the Outputs list.
        Args:
            location: Location(s) associated with the output.
            datum: Output value(s).
            attrs (dict): Additional attributes.
            datum_name (str): Name of the output datum.
            datum_type (str): Type of datum ('scalar', 'image', 'distribution').

        """
        output = SingleObservable(
            batch_dims=batch_dims,
            feature_dims=feature_dims,
            shots_per_batch=shots_per_batch,
            location=location,
            data=data,
            attrs=attrs,
            data_names=data_names,
            units=units,
            location_primary=location_primary,
            control=control
        )
        self.append(output)
        self.observable_checker(allow_blank=True)

    def observable_checker(self,allow_blank = False):
        """
        Validates observables in the Outputs list.
        Args:
            allow_blank (bool): If True, allows blank outputs.
        Raises:
            TypeError, ValueError, AssertionError: For invalid types or mismatched data.
        """
        if allow_blank and len(self) == 0:
            return
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
    summary_keys : list of str
        Keys (observable names) to include in the summary for fast querying.
    summary_location : str, float, or int
        Location at which to extract summary data ('final' or specific location).
    summary : dict
        Dictionary containing extracted summary values (populated by get_summary()).

    Methods
    -------
    __init__(summary_keys, summary_location)
        Initialize Summary instance.
    add_summary(summary_keys, summary_location)
        Adds summary keys and location.
    summary_checker(allow_blank)
        Validates summary information.
    """
    def __init__(self, summary_keys=None, summary_location='final'):
        """
        Initialize Summary instance.
        Args:
            summary_keys (list): Keys to include in summary.
            summary_location: Location for summary extraction.
        """
        
        self.add_summary(summary_keys, summary_location)
        self.summary_checker(allow_blank=True)

    def add_summary(self, summary_keys=None, summary_location='final'):
        """
        Adds summary keys and location.
        Args:
            summary_keys (list): Keys to include in summary.
            summary_location: Location for summary extraction.
        """
        self.summary_keys = summary_keys if summary_keys is not None else []
        self.summary_location = summary_location
    def summary_checker(self,allow_blank = False):
        """
        Validates summary information.
        Args:
            allow_blank (bool): If True, allows blank summary.
        Raises:
            TypeError, ValueError: For invalid types or missing required values.
        """
        if allow_blank and len(self.summary_keys) == 0:
            return
        for key in self.summary_keys:
            if not isinstance(key, str):
                raise TypeError("Each item in summary_keys must be a string.")
            if key == "":
                raise ValueError("Each item in summary_keys must not be empty.")
        if not isinstance(self.summary_location, (str, float, int)):
            raise TypeError("summary_location must be a string, float, or int.")

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
                 observable_list=None, summary_keys=None, summary_location='final', ID="", run_information=None
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
        self.summary = Summary(summary_keys=summary_keys, summary_location=summary_location)
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
            key=lambda x: (str(x.location), str(x.data_names))
        )
        
        for obs in sorted_obs:
            # Hash the location
            hasher.update(str(obs.location).encode('utf-8'))
            
            # Hash the data_names
            hasher.update(str(obs.data_names).encode('utf-8'))
            
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

    def add_observable(self, batch_dims=0, feature_dims=0, shots_per_batch=0, location=None, data=None, attrs=None, data_names=None, units=None, location_primary=True,control=False):
        """
        Adds an output to the data point.
        Args:
            location: Location(s) associated with the output.
            datum: Output value(s).
            attrs (dict): Additional attributes.
            datum_name (str): Name of the output datum.
            datum_type (str): Type of datum ('scalar', 'image', 'distribution').
        Returns:
            self: The DataPoint2 instance.
        """
        self.observables.add_observable(batch_dims, feature_dims, shots_per_batch, location, data, attrs, data_names, units, location_primary=location_primary, control=control)
        
        return self

    def add_summary(self, summary_keys=None, summary_location='final'):
        """
        Adds summary information to the data point.
        Args:
            summary_keys: Keys to include in summary.
            summary_location: Location for summary extraction.
        Returns:
            self: The DataPoint2 instance.
        """
        self.summary.add_summary(summary_keys, summary_location)
        return self
    
    def get_summary(self):
        """
        Extracts summary data for the data point based on specified summary keys.
        
        Handles two modes:
        1. location_primary=True: Extracts data where the key matches data_names
        2. location_primary=False: Extracts data at specific location from multi-location observables
        
        Returns:
            self: The DataPoint2 instance with populated summary.
        """
        summary = {}
                
        for key in self.summary.summary_keys:
            for observable in self.observables:
                # Case 1: location_primary=True (single location, key in data_names)
                if (
                    key in observable.data_names
                    and observable.location_primary == True
                ):
                    # Check if this observable's location matches the requested summary_location
                    loc = self.summary.summary_location
                    obs_location = observable.location[0]  # Single location for location_primary=True
                    
                    # If summary_location is 'final', we need to find the last location with this key
                    if loc == 'final':
                        # Continue searching for the last occurrence
                        data = np.squeeze(observable.data).tolist()
                        if isinstance(data, (int, float, np.integer, np.floating)):
                            data = [data]
                        summary[key] = data
                        # Don't break - keep looking for later occurrences
                        continue
                    elif obs_location == loc:
                        # Location matches - extract data
                        data = np.squeeze(observable.data).tolist()
                        if isinstance(data, (int, float, np.integer, np.floating)):
                            data = [data]
                        summary[key] = data
                        break
                # Case 2: location_primary=False (multiple locations, extract at specific location)
                elif key in observable.data_names and observable.location_primary == False:
                    loc = self.summary.summary_location
                    if loc == 'final':
                        loc = observable.location[-1]
                    if loc in observable.location:
                        idx = next(i for i, l in enumerate(observable.location) if l == loc)
                        data = np.squeeze(observable.data[:,idx]).tolist()
                        if isinstance(data, (int, float, np.integer, np.floating)):
                            data = [data]
                        summary[key] = data
                        break
        summary["ID"] = self.ID
        if hasattr(self, "simulation_metadata") and isinstance(self.simulation_metadata, SimulationMetadata):

            summary["simulation_start"] = self.simulation_metadata.simulation_start
            summary["simulation_end"] = self.simulation_metadata.simulation_end
            summary["simulation_code"] = self.simulation_metadata.simulation_code
            summary["simulation_version"] = self.simulation_metadata.simulation_version
            # "simulation_input_file": self.simulation_metadata.simulation_input_file
        
        # print(summary)
        self.summary.summary = summary
        self.summary.summary_keys = list(summary.keys())
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
        self.summary.summary_checker()
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
                    Attributes: location, control, units, custom attrs
                <data_name>_<idx> (group): ParticleGroup data (if applicable)
            Type_Grouped_Data/ (group, if location_primary=False)
                <data_name> (dataset): Observable data array
                    Attributes: location (array), control, units, custom attrs
                <data_name>_<idx> (group): ParticleGroup data (if applicable)
        
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
            filename = f"{self.ID}.h5"
        elif os.path.isdir(fileloc):
            filename = os.path.join(fileloc, f"{self.ID}.h5")
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
            j = 0
            for i, observable in enumerate(self.observables):

                if observable.location_primary == False:
                    # Create or get the "Type_Grouped_Data" group
                    if "Type_Grouped_Data" not in observables_grp:
                        type_grouped_grp = observables_grp.create_group("Type_Grouped_Data")
                    else:
                        type_grouped_grp = observables_grp["Type_Grouped_Data"]
                    assert len(observable.data_names) == 1, "observable.data_names must have length 1 when location_primary is False"

                    flat_data = observable.data.flatten()
                    has_particlegroup = any(isinstance(item, ParticleGroup) for item in flat_data)
                    
                    # Handle ParticleGroup objects specially
                    if has_particlegroup:
                        # Deep copy to avoid modifying original data
                        data = copy.deepcopy(observable.data)
                        # Write each ParticleGroup to its own HDF5 group
                        for idx in np.ndindex(data.shape):
                            pg = data[idx]
                            path = observable.data_names[0] + '_' + str(j)
                            pg_grp = type_grouped_grp.create_group(path)
                            data[idx] = path  # Replace ParticleGroup with path reference
                            pg.write(pg_grp)
                            j += 1
                        # Save path references as string dataset
                        out_grp = type_grouped_grp.create_dataset(observable.data_names[0], data=data.astype('S'))
                    else:
                        # Regular numeric data
                        out_grp = type_grouped_grp.create_dataset(observable.data_names[0], data=np.array(observable.data))

                    # Set dataset attributes (location, control, units)
                    location_value = observable.location.tolist() if isinstance(observable.location, np.ndarray) else observable.location
                    out_grp.attrs["location"] = location_value
                    out_grp.attrs["control"] = observable.control

                    if isinstance(observable.units, str):
                        out_grp.attrs["units"] = observable.units
                    else:
                        out_grp.attrs["units"] = getattr(observable.units, "unitSymbol", str(observable.units))

                    for k, v in observable.attrs.items():
                        out_grp.attrs[k] = v

                # --- Handle location_primary=True: Group by location ---
                else:
                    # Validate location structure
                    assert isinstance(observable.location, (list, np.ndarray)), "observable.location must be a list or np array when location_primary is True, got {}".format(type(observable.location))
                    assert len(observable.location) == 1, "observable.location must have length 1 when location_primary is True, got length {}".format(len(observable.location))
                    
                    if str(observable.location[0]) not in observables_grp:
                        out_grp = observables_grp.create_group(str(observable.location[0]))
                    else:
                        out_grp = observables_grp[str(observable.location[0])]

                    flat_data = observable.data.flatten()
                    has_particlegroup = any(isinstance(item, ParticleGroup) for item in flat_data)
                    
                    for k, obs_name in enumerate(observable.data_names):
                        if has_particlegroup:
                            data = copy.deepcopy(observable.data)
                            for idx in np.ndindex(data.shape):
                                pg = data[idx]
                                path = obs_name + '_' + str(j)
                                pg_grp = out_grp.create_group(path)
                                data[idx] = path
                                pg.write(pg_grp)
                                j += 1
                            dataset = out_grp.create_dataset(obs_name, data=data.astype('S'))
                        else:
                            dataset = out_grp.create_dataset(obs_name, data=np.array(observable.data))
                        dataset.attrs["location"] = str(observable.location[0])
                        dataset.attrs["control"] = observable.control
                        if isinstance(observable.units, str):
                            dataset.attrs["units"] = observable.units
                        else:
                            dataset.attrs["units"] = getattr(observable.units, "unitSymbol", str(observable.units))
                        for k, v in observable.attrs.items():
                            dataset.attrs[k] = v

            # Save simulation input file if this is a SimulatedDataPoint2
            if hasattr(self, "simulation_metadata") and isinstance(self.simulation_metadata, SimulationMetadata):
                lattice_grp.create_dataset("simulation_input_file", data=np.bytes_(self.simulation_metadata.simulation_input_file))

            # ==========================================
            # Save metadata as root attributes
            # ==========================================
            f.attrs["ID"] = self.ID
            f.attrs["run_information_source"] = self.run_information.source
            f.attrs["run_information_date"] = self.run_information.date
            f.attrs["run_information_notes"] = self.run_information.notes
            f.attrs["Data_Standard_Version"] = VERSION
            
            # Save summary keys as attributes
            for key in self.summary.summary_keys:
                value = getattr(self.summary, "summary", {}).get(key, "")
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
            f.attrs["summary_location"] = self.summary.summary_location

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
    __init__(lattice_location, lattice_files, observable_list, summary_keys, summary_location, ID, run_information, simulation_start, simulation_end, simulation_code, simulation_input_file, simulation_version)
        Initialize SimulatedDataPoint2 instance with simulation metadata.
    add_simulation_data(simulation_start, simulation_end, simulation_code, simulation_input_file, simulation_version)
        Adds or updates simulation metadata.

    Inherited Methods
    -----------------
    make_ID, add_lattice, add_run_information, add_observable, add_summary, get_summary, checker, finalize, saveHDF5
        See DataPoint2 for details.
    """
    def __init__(self, lattice_location=None, lattice_files=None,
                 observable_list=None, summary_keys=None, summary_location='final', ID="", run_information=None,
                 simulation_start=None, simulation_end=None, simulation_code="", simulation_input_file="",simulation_version=""):
        """
        Initialize SimulatedDataPoint2 instance.
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
            simulation_start (str): Simulation start time.
            simulation_end (str): Simulation end time.
            simulation_code (str): Simulation code name.
            simulation_input_file (str): Input file for simulation.
        """
        super().__init__(lattice_location=lattice_location, lattice_files=lattice_files, observable_list=observable_list, summary_keys=summary_keys, summary_location=summary_location, ID=ID, run_information=run_information)
        
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
