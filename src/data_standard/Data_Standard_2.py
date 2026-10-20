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
- data_names: Names of the data fields. For experimental data use the full PV
  (e.g., 'BPMS:IN10:781:TMIT'); for simulation data use the full parameter name.
- units: Physical units stored as given, including any SI prefix (e.g., 'mW', 'pC')
- read_only: Boolean flag; True for measured/read-back values, False for settable
  (control) parameters. The legacy ``control`` flag (= not read_only) is still accepted.
- Timestamp: Optional per-shot observable with the reserved name 'Timestamp' holding
  ISO 8601 date-time strings with a UTC offset.

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
        batch_dims=(),  # Empty tuple for single data point
        num_feature_dims=0,  # scalar
        location=['ICT1'],
        data=np.array(250.0),  # 0-D array for scalar
        data_name='charge',
        units='pC',
        read_only=True
    )
    
    # Add a 2D image (e.g., screen image)
    D.add_observable(
        batch_dims=(),
        num_feature_dims=2,  # 2D image
        location=['Screen1'],
        data=image_array,
        data_name='image',
        units='counts',
        attrs={'bin_size': 1e-6, 'offset': 0.0}  # REQUIRED for num_feature_dims > 0
    )
    
    # Add lattice and metadata
    D.add_lattice(lattice_location='https://github.com/slaclab/facet2-lattice')
    D.add_run_information(source='FACET-II', date='2025-01-26', notes='Example run')
    
    # Finalize and save
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

Version: 0.1.2
"""
import numpy as np
import pandas as pd
from pmd_beamphysics import ParticleGroup, units
import hashlib
import json
import h5py
import os
import copy
import re
import datetime

VERSION = '0.1.2'

# Reserved observable name for optional per-shot timestamps.
TIMESTAMP_NAME = 'Timestamp'
# Default group for observables that share a location array, and its location dataset.
# Further groups may be named 'multi_location_data_<suffix>' (e.g. 'multi_location_data_impact');
# names starting with MULTI_LOCATION_GROUP are reserved for these groups.
MULTI_LOCATION_GROUP = 'multi_location_data'
DATA_LOCATIONS_NAME = 'DATA_LOCATIONS'


def is_multi_location_group_name(name):
    """True if ``name`` is 'multi_location_data' or 'multi_location_data_<suffix>' (non-empty suffix)."""
    if not isinstance(name, str) or '/' in name:
        return False
    return name == MULTI_LOCATION_GROUP or (
        name.startswith(MULTI_LOCATION_GROUP + '_') and len(name) > len(MULTI_LOCATION_GROUP) + 1
    )


def is_broadcastable_batch_shape(shape, batch_dims):
    """
    True if ``shape`` (the leading batch part of a data shape) is compatible with ``batch_dims``
    under the broadcasting rule: same rank, and each dimension equals batch_dims[i] or 1.
    """
    shape = tuple(shape)
    batch_dims = tuple(batch_dims)
    return len(shape) == len(batch_dims) and all(s == b or s == 1 for s, b in zip(shape, batch_dims))

# ISO 8601-1:2019 extended-format calendar date-time with a mandatory UTC designator
# ('Z') or offset (±hh:mm), e.g. '2026-10-16T13:55:40.050-07:00'.
_ISO8601_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:[.,](\d+))?(Z|([+-])(\d{2}):(\d{2}))$"
)


def is_iso8601_timestamp(value):
    """
    Check whether ``value`` is an ISO 8601 extended-format date-time with UTC offset.

    Accepted form: ``YYYY-MM-DDThh:mm:ss[.fff...](Z|±hh:mm)``.

    Args:
        value (str or bytes): Candidate timestamp.

    Returns:
        bool: True if valid.
    """
    if isinstance(value, (bytes, np.bytes_)):
        try:
            value = value.decode('utf-8')
        except UnicodeDecodeError:
            return False
    if not isinstance(value, str):
        return False
    m = _ISO8601_RE.match(value)
    if not m:
        return False
    year, month, day, hour, minute, second = (int(g) for g in m.groups()[:6])
    if second > 60:  # 60 permitted for leap seconds
        return False
    try:
        datetime.datetime(year, month, day, hour, minute, min(second, 59))
    except ValueError:
        return False
    if m.group(8) != 'Z':
        if int(m.group(10)) > 23 or int(m.group(11)) > 59:
            return False
    return True


def to_iso8601_string(value):
    """
    Convert a timestamp value to an ISO 8601 string accepted by the standard.

    Args:
        value (str, bytes, or timezone-aware datetime.datetime / pandas.Timestamp).

    Returns:
        str: ISO 8601 date-time string.

    Raises:
        TypeError: For unsupported types.
        ValueError: For naive datetimes or malformed strings.
    """
    if isinstance(value, (bytes, np.bytes_)):
        value = value.decode('utf-8')
    if isinstance(value, (str, np.str_)):
        value = str(value)
        if not is_iso8601_timestamp(value):
            raise ValueError(
                f"Timestamp '{value}' is not an ISO 8601 date-time with UTC offset "
                f"(expected YYYY-MM-DDThh:mm:ss[.fff](Z|±hh:mm))"
            )
        return value
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Timestamp datetimes must be timezone-aware (include a UTC offset)")
        return value.isoformat()
    raise TypeError(f"Timestamp values must be ISO 8601 strings or timezone-aware datetimes, got {type(value)}")


def _coerce_timestamps(data):
    """Convert timestamp data (scalar or array-like) to a numpy unicode array."""
    arr = data if isinstance(data, np.ndarray) else np.asarray(data, dtype=object)
    if arr.dtype.kind == 'U':
        return arr
    out = np.empty(arr.shape, dtype=object)
    for idx in np.ndindex(arr.shape):
        out[idx] = to_iso8601_string(arr[idx])
    return out.astype(str)


def resolve_read_only(read_only=None, control=None):
    """
    Resolve the ``read_only`` flag from the new ``read_only`` and legacy ``control`` arguments.

    ``read_only`` is the inverse of ``control``. If neither is given, the observable is
    read-only (a measured/output quantity).

    Raises:
        TypeError: If a provided value is not a boolean.
        ValueError: If both are provided and inconsistent.
    """
    for name, value in (('read_only', read_only), ('control', control)):
        if value is not None and not isinstance(value, (bool, np.bool_)):
            raise TypeError(f"{name} must be a single boolean value, got {type(value)}")
    if read_only is None and control is None:
        return True
    if read_only is None:
        return not bool(control)
    if control is not None and bool(read_only) == bool(control):
        raise ValueError(
            f"read_only={bool(read_only)} and control={bool(control)} are inconsistent; "
            f"read_only must equal (not control). Prefer passing only read_only."
        )
    return bool(read_only)

# SI prefix factors — defined here for compatibility with beamphysics versions
# that may not export PREFIX_FACTOR / SHORT_PREFIX_FACTOR.
_PREFIX_FACTOR = getattr(units, 'PREFIX_FACTOR', {
    'yocto': 1e-24, 'zepto': 1e-21, 'atto': 1e-18, 'femto': 1e-15,
    'pico': 1e-12, 'nano': 1e-9, 'micro': 1e-6, 'milli': 1e-3,
    'centi': 1e-2, 'deci': 1e-1, 'deca': 1e1, 'hecto': 1e2,
    'kilo': 1e3, 'mega': 1e6, 'giga': 1e9, 'tera': 1e12,
    'peta': 1e15, 'exa': 1e18, 'zetta': 1e21, 'yotta': 1e24,
})
_SHORT_PREFIX_FACTOR = getattr(units, 'SHORT_PREFIX_FACTOR', {
    'y': 1e-24, 'z': 1e-21, 'a': 1e-18, 'f': 1e-15,
    'p': 1e-12, 'n': 1e-9, 'µ': 1e-6, 'u': 1e-6, 'm': 1e-3,
    'c': 1e-2, 'd': 1e-1, 'da': 1e1, 'h': 1e2,
    'k': 1e3, 'M': 1e6, 'G': 1e9, 'T': 1e12,
    'P': 1e15, 'E': 1e18, 'Z': 1e21, 'Y': 1e24,
})


def unit_checker(unit):
    """
    Checks if the provided unit is valid and splits off any SI prefix.

    The unit string itself is stored as given (e.g. 'mW', 'uW', 'pC'); the multiplier
    and base unit returned here are only used to validate the unit.

    Args:
        unit (str): Unit string to check. ASCII 'u' is accepted as an alias for 'µ'.

    Returns:
        tuple: (prefix_multiplier, base_unit) where base_unit is an
        openpmd_beamphysics unit if recognized, else "Custom Unit"
        (with multiplier 1.0).

    Raises:
        ValueError: If unit is not a string.
    """
    if not isinstance(unit, str):
        raise ValueError("unit must be a string, received type: {}".format(type(unit)))

    if unit in units.known_unit:
        return 1.0, units.known_unit[unit]

    # Longest prefix first so e.g. 'da' (deca) wins over 'd' (deci).
    factors = {**_PREFIX_FACTOR, **_SHORT_PREFIX_FACTOR}
    factors.setdefault('u', 1e-6)
    for pro in sorted(factors, key=len, reverse=True):
        if unit.startswith(pro):
            base_unit = unit[len(pro):]
            if base_unit and base_unit in units.known_unit:
                return float(factors[pro]), units.known_unit[base_unit]

    return 1.0, "Custom Unit"


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

    The constructor coerces inputs (e.g. lists to arrays) and then calls
    :meth:`checker`, which can also be called on its own at any time.

    Attributes
    ----------
    batch_dims : tuple
        Tuple of batch dimension sizes. Last dimension is number of shots.
    num_feature_dims : int
        Number of feature dimensions (0=scalar, 1=waveform, 2=image).
    location : ndarray or None
        Physical or logical location(s) in the beamline (1D array). Optional only
        for the 'Timestamp' observable.
    data : ndarray
        Observable data array with shape: concatenate(b, length_dim, feature_dims), where each
        b[i] is batch_dims[i] or 1. A size-1 batch dimension is broadcast to every shot, so data
        that is constant across (part of) a scan is stored only once.
    data_name : str
        Name of the data field (single string). For experimental data this should be the
        full PV (e.g. 'BPMS:IN10:781:TMIT'); 'Timestamp' is reserved for per-shot timestamps.
    location_primary : bool
        If True, data is grouped by location; if False, it is stored in a multi-location group.
    location_group : str or None
        Multi-location group name ('multi_location_data' or 'multi_location_data_<suffix>');
        None for location-primary observables.
    attrs : dict
        Additional attributes. For non-scalar data (num_feature_dims>0), must include 'bin_size' and 'offset'.
    read_only : bool
        True for measured/read-back quantities, False for settable (control/input) parameters.
    control : bool
        Legacy alias, always equal to ``not read_only``.
    units : str
        Units exactly as given, including any SI prefix (e.g. 'mW', 'pC').
    base_units : unit object or str
        Base unit found by :func:`unit_checker` ("Custom Unit" if not recognized).
    unit_multiplier : float
        SI prefix multiplier found by :func:`unit_checker` (used for validation only).
    num_length_dim : int
        0 if single location, 1 if multiple locations (calculated property).
    length_dim : tuple
        Shape of location dimension (calculated property).
    feature_dims : tuple
        Shape of feature dimensions (calculated property).

    Methods
    -------
    __init__(batch_dims, num_feature_dims, location, data, attrs, data_name, units, location_units, location_primary, control, read_only, location_group)
        Initialize a SingleObservable instance with data and metadata.
    checker()
        Validates the full observable (standalone).
    data_dim_checker()
        Validates that data array dimensions match the specified structure.
    to_dict()
        Returns a dictionary representation of the observable (for control parameters).
    """
    def __init__(self, batch_dims=None, num_feature_dims=0, location=None, data=None, attrs=None, data_name=None, units=None, location_units=None, location_primary=None, control=None, read_only=None, location_group=None):
        """
        Initialize a SingleObservable instance.
        Args:
            batch_dims (tuple): Tuple of batch dimension sizes. Last is number of shots.
            num_feature_dims (int): Number of feature dimensions.
            location: Location(s) associated with the observable.
            data: Observable data array. Each leading batch dimension may be batch_dims[i] or 1
                (broadcast to all shots), e.g. shape (1, H, W) for an image constant over batch_dims=(5,).
            attrs (dict): Additional attributes.
            data_name (str): Name of the data field.
            units (str): Physical units, stored as given (SI prefixes allowed).
            location_units (str): Physical units for location data. Required when location_primary is False.
            location_primary (bool): Group by location if True (default). False stores the observable
                in a multi-location group (default 'multi_location_data').
            control (bool): Legacy flag, True for control/input. Equivalent to read_only=not control.
            read_only (bool): True for measured/read-back values (default), False for settable inputs.
            location_group (str): Name of the multi-location group, 'multi_location_data' or
                'multi_location_data_<suffix>'. Implies location_primary=False.
        Raises:
            TypeError, ValueError: For invalid types or mismatched data.
        """
        if location_group is not None:
            if location_primary is True:
                raise ValueError("location_group requires location_primary=False (or leave location_primary unset)")
            location_primary = False
        elif location_primary is None:
            location_primary = True
        if location_primary is False and location_group is None:
            location_group = MULTI_LOCATION_GROUP

        if batch_dims is None:
            batch_dims = ()
        elif isinstance(batch_dims, int):
            raise TypeError("batch_dims must be a tuple, not an int. For previous batch_dims=0, use batch_dims=(). For batch_dims=1, use batch_dims=(n_shots,).")
        elif not isinstance(batch_dims, (list, tuple)):
            raise TypeError(f"batch_dims must be a list or tuple, got {type(batch_dims)}")
        else:
            batch_dims = tuple(batch_dims)

        if location is not None and not (
            isinstance(location, (str, int, float, list, np.ndarray))
        ):
            raise TypeError("location must be a string, number, list, or np.ndarray")
        if isinstance(location, (str, int, float)):
            location = np.array([location])
        elif isinstance(location, list):
            if not all(isinstance(item, (str, int, float)) for item in location):
                raise ValueError("location as list must contain only str, int, or float values (no nested lists)")
            location = np.array(location)

        if data_name is not None and isinstance(data_name, (list, np.ndarray)):
            if len(data_name) == 0:
                raise ValueError("data_name cannot be an empty list")
            elif len(data_name) == 1:
                data_name = str(data_name[0])
            else:
                raise ValueError(f"data_name must be a single string, got list of length {len(data_name)}")

        is_timestamp = data_name == TIMESTAMP_NAME
        if is_timestamp:
            if units is None:
                units = ""
            if data is not None:
                data = _coerce_timestamps(data)

        if data is not None and not isinstance(data, np.ndarray):
            try:
                data = np.array(data)
            except Exception as e:
                raise TypeError(f"data must be convertible to np.ndarray, got type {type(data)}. Error: {e}")

        # ParticleGroup data: all elements must be ParticleGroups and num_feature_dims is 0
        if data is not None and data.size > 0 and data.dtype == object:
            flat_data = data.flatten()
            if any(isinstance(item, ParticleGroup) for item in flat_data):
                if not all(isinstance(item, ParticleGroup) for item in flat_data):
                    raise TypeError("If any element of data is a ParticleGroup, all elements must be ParticleGroup")
                num_feature_dims = 0

        if isinstance(num_feature_dims, np.integer):
            num_feature_dims = int(num_feature_dims)

        attrs = dict(attrs) if attrs is not None else {}
        if isinstance(num_feature_dims, int) and num_feature_dims > 0:
            if 'bin_size' in attrs and not isinstance(attrs['bin_size'], str):
                try:
                    attrs['bin_size'] = float(attrs['bin_size'])
                except (ValueError, TypeError) as e:
                    raise TypeError(f"bin_size must be a float or a string key, got {type(attrs['bin_size'])}: {e}")
            if 'offset' in attrs:
                try:
                    attrs['offset'] = float(attrs['offset'])
                except (ValueError, TypeError) as e:
                    raise TypeError(f"offset must be convertible to float, got {type(attrs['offset'])}: {e}")

        self.batch_dims = batch_dims
        self.num_feature_dims = num_feature_dims
        self.location = location
        self.location_units = location_units
        self.data_name = data_name
        self.location_primary = location_primary
        self.location_group = location_group
        self.data = data
        self.attrs = attrs
        self.read_only = resolve_read_only(read_only, control)

        prefix, base_units = unit_checker(units)
        self.units = units
        self.unit_multiplier = prefix
        self.base_units = base_units

        self.checker()

    @property
    def control(self):
        """Legacy alias: True for control/input parameters, i.e. ``not read_only``."""
        return not self.read_only

    @control.setter
    def control(self, value):
        self.read_only = resolve_read_only(control=value)

    @property
    def is_timestamp(self):
        """True if this is the reserved per-shot 'Timestamp' observable."""
        return self.data_name == TIMESTAMP_NAME

    @property
    def batch_shape(self):
        """Leading batch part of the data shape (each entry is batch_dims[i] or 1)."""
        if self.data is None:
            return tuple(self.batch_dims)
        return tuple(self.data.shape[:len(self.batch_dims)])

    def entry_names(self):
        """
        HDF5 entry names this observable is written as.

        Returns [data_name] for regular data and for a single ParticleGroup (data shape all ones,
        i.e. one ParticleGroup for every shot, including batch_dims=()). Otherwise returns one
        '<data_name>_<i>[_<j>...]' group name per ParticleGroup, indexed over the data shape.
        """
        if self.has_particlegroup and self.data.size > 1:
            return [f"{self.data_name}_{'_'.join(map(str, idx))}" for idx in np.ndindex(self.data.shape)]
        return [self.data_name]

    def entries(self):
        """List of (entry_name, index) pairs; index selects the ParticleGroup in self.data."""
        names = self.entry_names()
        if self.has_particlegroup and self.data.size > 1:
            return list(zip(names, np.ndindex(self.data.shape)))
        return [(names[0], (0,) * self.data.ndim if self.has_particlegroup else None)]

    def checker(self):
        """
        Validates this observable. Safe to call standalone at any time; does not modify the object.

        Raises:
            TypeError, ValueError: If any rule is violated.
        """
        if not isinstance(self.batch_dims, tuple) or not all(
            isinstance(d, (int, np.integer)) and not isinstance(d, (bool, np.bool_)) and d > 0
            for d in self.batch_dims
        ):
            raise ValueError("All batch_dims values must be positive integers")

        if isinstance(self.num_feature_dims, (bool, np.bool_)) or not isinstance(self.num_feature_dims, (int, np.integer)) or self.num_feature_dims < 0:
            raise ValueError(f"num_feature_dims must be a non-negative integer, got {self.num_feature_dims!r}")

        if not isinstance(self.data_name, str):
            raise TypeError(f"data_name must be a string, got {type(self.data_name)}")
        if self.data_name == "":
            raise ValueError("data_name must not be empty")
        if '/' in self.data_name:
            raise ValueError(f"data_name must not contain '/', got '{self.data_name}'")

        if not isinstance(self.location_primary, bool):
            raise ValueError("location_primary must be a boolean value")
        if self.location_primary:
            if self.location_group is not None:
                raise ValueError("location_group may only be set for multi-location observables (location_primary=False)")
        elif not is_multi_location_group_name(self.location_group):
            raise ValueError(
                f"location_group must be '{MULTI_LOCATION_GROUP}' or start with '{MULTI_LOCATION_GROUP}_', "
                f"got {self.location_group!r}"
            )
        if self.data_name.startswith(MULTI_LOCATION_GROUP):
            raise ValueError(f"data_name must not start with the reserved prefix '{MULTI_LOCATION_GROUP}'")
        # Per-shot ParticleGroups are written as '<data_name>_<i>'; a data_name ending in '_<digits>'
        # would be ambiguous with those entry names.
        if self.has_particlegroup and re.search(r'_\d+$', self.data_name):
            raise ValueError(
                f"ParticleGroup data_name must not end in '_<integer>' (reserved for per-shot entries), got '{self.data_name}'"
            )
        if not isinstance(self.read_only, bool):
            raise TypeError(f"read_only must be a single boolean value, got {type(self.read_only)}")

        if self.location is None:
            if not self.is_timestamp:
                raise ValueError(f"location is required for observable '{self.data_name}'")
        else:
            if not isinstance(self.location, np.ndarray) or self.location.ndim != 1:
                raise ValueError("location must be a 1-D array of str, int, or float values")
            if len(self.location) == 0:
                raise ValueError("location must not be empty")
            if self.location_primary:
                if len(self.location) != 1:
                    raise ValueError(f"When location_primary is True, location must have exactly 1 element, got {len(self.location)}")
                if '/' in str(self.location[0]) or str(self.location[0]) == "":
                    raise ValueError(f"location must be a non-empty name without '/', got '{self.location[0]}'")
                if str(self.location[0]).startswith(MULTI_LOCATION_GROUP):
                    raise ValueError(f"location must not start with the reserved prefix '{MULTI_LOCATION_GROUP}'")
        if not self.location_primary:
            if not isinstance(self.location_units, str) or self.location_units == "":
                raise ValueError(f"location_units must be specified for multi-location observables (observable '{self.data_name}')")

        if not isinstance(self.units, str):
            raise ValueError("unit must be a string, received type: {}".format(type(self.units)))

        if self.num_feature_dims > 0:
            if 'bin_size' not in self.attrs:
                raise ValueError(f"For non-scalar data (num_feature_dims={self.num_feature_dims}), 'bin_size' must be specified in attrs")
            if 'offset' not in self.attrs:
                raise ValueError(f"For non-scalar data (num_feature_dims={self.num_feature_dims}), 'offset' must be specified in attrs")
            # bin_size may be a float, or a string naming another observable (key) whose
            # data has dimension batch_dims (per-shot bin sizes); validated in observable_checker.
            if isinstance(self.attrs['bin_size'], str):
                if len(self.batch_dims) == 0:
                    raise ValueError("bin_size may only be a string key when batch_dims is non-empty; use a float for single-shot data")
            elif not isinstance(self.attrs['bin_size'], float):
                raise TypeError(f"bin_size must be a float or a string key, got {type(self.attrs['bin_size'])}")
            if not isinstance(self.attrs['offset'], float):
                raise TypeError(f"offset must be a float, got {type(self.attrs['offset'])}")

        if self.is_timestamp:
            if self.num_feature_dims != 0:
                raise ValueError("Timestamp observable must have num_feature_dims=0")
            if not self.read_only:
                raise ValueError("Timestamp observable must be read_only")
            if not self.location_primary:
                raise ValueError("Timestamp observable must have location_primary=True")
            if self.data is not None:
                if self.data.dtype.kind != 'U':
                    raise TypeError(f"Timestamp data must be ISO 8601 strings, got dtype {self.data.dtype}")
                for value in self.data.ravel():
                    if not is_iso8601_timestamp(value):
                        raise ValueError(
                            f"Timestamp '{value}' is not an ISO 8601 date-time with UTC offset "
                            f"(expected YYYY-MM-DDThh:mm:ss[.fff](Z|±hh:mm))"
                        )
        elif self.data is not None and not self.has_particlegroup and self.data.dtype.kind not in 'biufc':
            raise TypeError(
                f"data for observable '{self.data_name}' must be numeric (or ParticleGroup objects), got dtype {self.data.dtype}"
            )

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
        """Units string for serialization, exactly as given (including any SI prefix)."""
        if isinstance(self.units, str):
            return self.units
        return getattr(self.units, "unitSymbol", str(self.units))
    
    @property
    def has_particlegroup(self):
        """Check if data contains ParticleGroup objects."""
        if self.data is None or self.data.size == 0 or self.data.dtype != object:
            return False
        flat_data = self.data.flatten()
        return any(isinstance(item, ParticleGroup) for item in flat_data)

    def data_dim_checker(self):
        """
        Validates that data array dimensions match: concatenate(b, length_dim, feature_dims),
        where each batch dimension b[i] is batch_dims[i] or 1 (broadcast to all shots).
        
        Raises:
            ValueError: If data dimensions don't match the expected structure.
        """
        expected_ndim = len(self.batch_dims) + self.num_length_dim + self.num_feature_dims
        
        if len(self.data.shape) != expected_ndim:
            raise ValueError(
                f"data must have {expected_ndim} dimensions "
                f"(batch_dims={len(self.batch_dims)}, length_dim={self.num_length_dim}, num_feature_dims={self.num_feature_dims}), "
                f"but got {len(self.data.shape)} dimensions with shape {self.data.shape}"
            )
        
        # Each batch dimension must match batch_dims or be 1 (broadcast)
        for i, expected_size in enumerate(self.batch_dims):
            if self.data.shape[i] not in (expected_size, 1):
                raise ValueError(
                    f"data batch dimension {i} should be {expected_size} (or 1 to broadcast) but got {self.data.shape[i]}"
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
        if self.control:
            return {
                "name": self.data_name,
                "value": self.data,
                "location": self.location,
                "units": self.units_str,
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
        Optional dictionary mapping EPICS Process Variable (PV) names to lattice element names.
        Used in experimental data to correlate control system variables with beamline elements.
        Example: {'SOLN:IN10:121': 'SOL10121', 'QUAD:IN10:121': 'CQ10121'}

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
            PV_table (dict): Optional dictionary mapping EPICS Process Variable (PV) names
                to their corresponding lattice element names. Used in experimental data to 
                correlate control system variables with beamline elements.
                Example: {'SOLN:IN10:121': 'SOL10121', 'QUAD:IN10:121': 'CQ10121'}
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
    add_observable(batch_dims, num_feature_dims, location, data, attrs, data_name, units, location_units, location_primary, control, read_only, location_group)
        Adds an observable to the list.
    layout()
        Returns where each observable is stored under observables/ in HDF5.
    observable_checker(allow_blank)
        Validates all observables in the list (standalone).
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

    def add_observable(self, batch_dims=None, num_feature_dims=0, location=None, data=None, attrs=None, data_name=None, units=None, location_units=None, location_primary=None, control=None, read_only=None, location_group=None):
        """
        Adds an observable to the Observables list.
        Args:
            batch_dims (list): List of batch dimension sizes.
            num_feature_dims (int): Number of feature dimensions.
            location: Location(s) associated with the observable.
            data: Observable data array (size-1 batch dimensions broadcast to all shots).
            attrs (dict): Additional attributes.
            data_name (str): Name of the data field.
            units (str): Physical units, stored as given (SI prefixes allowed).
            location_units (str): Physical units for location data. Default None.
            location_primary (bool): Group by location if True (default).
            control (bool): Legacy flag, True for control/input (= not read_only).
            read_only (bool): True for measured/read-back values (default), False for settable inputs.
            location_group (str): Multi-location group name ('multi_location_data' or
                'multi_location_data_<suffix>'); implies location_primary=False.
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
            control=control,
            read_only=read_only,
            location_group=location_group,
        )
        self.append(output)
        self.observable_checker(allow_blank=True)

    def layout(self):
        """
        Determine where each observable is stored under ``observables/`` in HDF5.

        - multi-location observables -> their ``location_group`` (e.g. ``multi_location_data``)
        - a location holding exactly one observable that is written as a single HDF5 entry
          (or a Timestamp without location) -> top level ('' parent), with the location kept
          in the ``@location`` attribute
        - otherwise (several observables, or per-shot ParticleGroups) -> a group named after
          the location

        Returns:
            list of (SingleObservable, parent) tuples, parent being '' (top level),
            a location name, or a multi-location group name.
        """
        counts = {}
        for obs in self:
            if obs.location_primary and obs.location is not None:
                key = str(obs.location[0])
                counts[key] = counts.get(key, 0) + 1
        result = []
        for obs in self:
            if not obs.location_primary:
                parent = obs.location_group
            elif obs.location is None or (counts[str(obs.location[0])] == 1 and len(obs.entry_names()) == 1):
                parent = ""
            else:
                parent = str(obs.location[0])
            result.append((obs, parent))
        return result

    def _layout_checker(self):
        """Raise ValueError if any two observables would map to the same HDF5 path."""
        top_level = {}
        groups = {}
        layout = self.layout()
        for obs, parent in layout:
            for name in obs.entry_names():
                if parent == "":
                    if name in top_level:
                        raise ValueError(
                            f"Observable '{obs.data_name}' would be stored as observables/{name}, "
                            f"which is already used by '{top_level[name]}'. Use unique (full PV) data_names."
                        )
                    top_level[name] = obs.data_name
                else:
                    names = groups.setdefault(parent, set())
                    if not obs.location_primary and name == DATA_LOCATIONS_NAME:
                        raise ValueError(f"'{DATA_LOCATIONS_NAME}' is reserved in {parent}")
                    if name in names:
                        raise ValueError(f"Duplicate observable '{name}' in observables/{parent}")
                    names.add(name)
        for parent in groups:
            if parent in top_level:
                raise ValueError(
                    f"observables/{parent} would be both a group and the observable '{top_level[parent]}'. "
                    f"Use unique (full PV) data_names."
                )

    def _multi_location_checker(self):
        """Within each multi-location group, observables must share locations, location_units and num_feature_dims."""
        by_group = {}
        for obs in self:
            if not obs.location_primary:
                by_group.setdefault(obs.location_group, []).append(obs)
        for group, members in by_group.items():
            first = members[0]
            for obs in members[1:]:
                a, b = np.asarray(first.location), np.asarray(obs.location)
                same = a.shape == b.shape and (
                    np.array_equal(a, b) if a.dtype.kind == b.dtype.kind else np.array_equal(a.astype(str), b.astype(str))
                )
                if not same:
                    raise ValueError(
                        f"All observables in {group} must have same location. "
                        f"Found {obs.location} for '{obs.data_name}' but expected {first.location}"
                    )
                if obs.num_feature_dims != first.num_feature_dims:
                    raise ValueError(
                        f"All observables in {group} must have same num_feature_dims. "
                        f"Found {obs.num_feature_dims} for '{obs.data_name}' but expected {first.num_feature_dims}"
                    )
                if obs.location_units != first.location_units:
                    raise ValueError(
                        f"All observables in {group} must have same location_units. "
                        f"Found '{obs.location_units}' for '{obs.data_name}' but expected '{first.location_units}'"
                    )

    def observable_checker(self,allow_blank = False):
        """
        Validates observables in the Observables list. Safe to call standalone.
        Args:
            allow_blank (bool): If True, allows blank outputs and performs only the
                incremental checks used while observables are being added.
        Raises:
            TypeError, ValueError, AssertionError: For invalid types or mismatched data.
        """
        if allow_blank and len(self) == 0:
            return

        if len(self) > 0:
            first_batch_dims = self[0].batch_dims
            for i, observable in enumerate(self):
                if observable.batch_dims != first_batch_dims:
                    raise ValueError(
                        f"All observables must have the same batch_dims. "
                        f"Observable at index 0 has batch_dims={first_batch_dims}, "
                        f"but observable at index {i} has batch_dims={observable.batch_dims}"
                    )

        n_timestamps = sum(1 for obs in self if obs.is_timestamp)
        if n_timestamps > 1:
            raise ValueError(f"At most one '{TIMESTAMP_NAME}' observable is allowed, found {n_timestamps}")

        if allow_blank:
            for observable in self:
                if observable.data is not None:
                    observable.data_dim_checker()
            return

        for observable in self:
            observable.checker()
            if observable.data is None:
                raise ValueError(f"Observable '{observable.data_name}' has no data")

        self._multi_location_checker()
        self._layout_checker()

        # String bin_size references must name another observable (key) holding one scalar per
        # shot, i.e. data shape batch_dims (size-1 dimensions broadcast).
        for observable in self:
            if observable.num_feature_dims > 0 and isinstance(observable.attrs.get('bin_size'), str):
                key = observable.attrs['bin_size']
                target = tuple(observable.batch_dims)
                matches = [obs for obs in self if obs.data_name == key]
                if not matches:
                    raise ValueError(
                        f"bin_size='{key}' for observable '{observable.data_name}' "
                        f"must match the data_name of another observable (key)"
                    )
                if not any(obs.data is not None and is_broadcastable_batch_shape(np.shape(obs.data), target) for obs in matches):
                    shapes = [None if obs.data is None else tuple(np.shape(obs.data)) for obs in matches]
                    raise ValueError(
                        f"bin_size key '{key}' must reference data with dimension "
                        f"batch_dims={target}, got shape(s) {shapes}"
                    )

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
    add_observable(batch_dims, num_feature_dims, location, data, attrs, data_name, units, location_units, location_primary, control, read_only, location_group)
        Adds an observable measurement or output to the data point.
    add_timestamp(data, batch_dims, location)
        Adds the optional per-shot 'Timestamp' observable (ISO 8601 strings).
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
            
            # Hash the actual data, including its shape (broadcast (1,) differs from (n,))
            if obs.data is not None:
                hasher.update(str(tuple(obs.data.shape)).encode('utf-8'))
            if obs.data is not None and obs.data.size > 0:
                # Skip ParticleGroup objects
                if obs.data.dtype.kind in 'US':
                    hasher.update("\x1f".join(str(v) for v in obs.data.ravel()).encode('utf-8'))
                elif obs.data.dtype != object:
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
            PV_table (dict): Optional dictionary mapping EPICS Process Variable (PV) names
                to their corresponding lattice element names. Used in experimental data to 
                correlate control system variables with beamline elements.
                Example: {'SOLN:IN10:121': 'SOL10121', 'QUAD:IN10:121': 'CQ10121'}
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

    def add_observable(self, batch_dims=None, num_feature_dims=0, location=None, data=None, attrs=None, data_name=None, units=None, location_units=None, location_primary=None, control=None, read_only=None, location_group=None):
        """
        Adds an observable to the data point.
        Args:
            batch_dims (tuple): Tuple of batch dimension sizes. Use () for single data point.
            num_feature_dims (int): Number of feature dimensions (0=scalar, 1=1D, 2=2D, etc).
            location: Location(s) associated with the observable.
            data: Observable data array. Each leading batch dimension is batch_dims[i] or 1; a size-1
                dimension is broadcast to all shots (e.g. an image constant over batch_dims=(5,)
                may be given with shape (1, H, W)).
            attrs (dict): Additional attributes. REQUIRED to include 'bin_size' and 'offset'
                         when num_feature_dims > 0. Example: {'bin_size': 1e-6, 'offset': 0.0}
            data_name (str): Name of the data field. Use the full PV for experimental data
                (e.g. 'BPMS:IN10:781:TMIT') and the full parameter name for simulations.
            units (str): Physical units, stored as given including SI prefix (e.g. 'mW').
            location_units (str): Physical units for location data. Required for multi-location observables.
            location_primary (bool): Group by location if True (default). False stores the observable
                in the multi-location group 'multi_location_data'.
            control (bool): Legacy flag, True for control/input. Equivalent to read_only=not control.
            read_only (bool): True for measured/read-back values (default), False for settable inputs.
            location_group (str): Name of the multi-location group, 'multi_location_data' or
                'multi_location_data_<suffix>' (e.g. 'multi_location_data_impact'). Implies
                location_primary=False. Each group has its own DATA_LOCATIONS.
        Returns:
            self: The DataPoint2 instance.
        """
        self.observables.add_observable(batch_dims, num_feature_dims, location, data, attrs, data_name, units, location_units, location_primary=location_primary, control=control, read_only=read_only, location_group=location_group)
        
        return self

    def add_timestamp(self, data, batch_dims=None, location=None):
        """
        Adds the optional per-shot 'Timestamp' observable.

        Args:
            data: ISO 8601 date-time string(s) with UTC offset (e.g. '2026-10-16T13:55:40.050-07:00')
                or timezone-aware datetime objects, with shape batch_dims (size-1 dimensions broadcast).
            batch_dims (tuple): Batch dimensions. Defaults to those of existing observables,
                otherwise to the shape of ``data``.
            location: Optional location; if omitted, the timestamp is stored at observables/Timestamp
                without an @location attribute.
        Returns:
            self: The DataPoint2 instance.
        """
        if batch_dims is None:
            batch_dims = self.observables[0].batch_dims if len(self.observables) > 0 else np.shape(data)
        self.observables.add_observable(
            batch_dims=batch_dims, num_feature_dims=0, location=location, data=data,
            data_name=TIMESTAMP_NAME, units="", location_primary=True, read_only=True,
        )
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
            lattice_mapping (dataset, optional): 2xn array where row 0 is PV names, row 1 is lattice elements
            simulation_input_file (dataset, if SimulatedDataPoint2 with scalar batch)
            simulation_input_file_0, _1, ... (datasets, if SimulatedDataPoint2 with non-scalar batch)
        
        /observables/
            <data_name> (dataset, if it is the only observable at its location)
                Attributes: location, read_only, units, num_feature_dims, bin_size/offset
            <data_name>/ (group, a single ParticleGroup that is the only observable at its location)
                Attributes: location
            <location_name>/ (group, if several observables share the location, or per-shot ParticleGroups)
                <data_name> (dataset): Observable data
                    Attributes: location, read_only, units, num_feature_dims, bin_size/offset
                <data_name> or <data_name>_<i>_<j>_... (group): ParticleGroup data
                    Attributes: location
            multi_location_data[_<suffix>]/ (group per location_group)
                DATA_LOCATIONS (dataset): Location data for all datasets in this group
                    Attributes: units (location units), num_feature_dims
                <data_name> (dataset): Observable data array
                    Attributes: read_only, units, num_feature_dims, bin_size/offset
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
            lattice_grp.attrs["lattice_location"] = self.lattice.lattice_location
            
            # Save lattice files if provided as dictionary
            if isinstance(self.lattice.lattice_files, dict):
                lattice_files_grp = lattice_grp.create_group("lattice_files")
                for fname, contents in self.lattice.lattice_files.items():
                    lattice_files_grp.create_dataset(fname, data=np.bytes_(contents))
            
            # Save PV_table as 2xn dataset if present
            if hasattr(self.lattice, "PV_table") and isinstance(self.lattice.PV_table, dict) and self.lattice.PV_table:
                # Create 2xn array: first row is PV names, second row is lattice element names
                pv_names = list(self.lattice.PV_table.keys())
                lattice_names = list(self.lattice.PV_table.values())
                
                # Create 2xn array and store as dataset
                mapping_array = np.array([pv_names, lattice_names], dtype='S')
                lattice_grp.create_dataset("lattice_mapping", data=mapping_array)

            # ==========================================
            # Save observables
            # ==========================================
            observables_grp = f.create_group("observables")
            for observable, parent in self.observables.layout():
                if parent == "":
                    target_grp = observables_grp
                elif not observable.location_primary:
                    if parent not in observables_grp:
                        target_grp = observables_grp.create_group(parent)
                        location_value = np.asarray(observable.location)
                        if location_value.dtype.kind in ['U', 'O']:
                            location_value = location_value.astype('S')
                        loc_dataset = target_grp.create_dataset(DATA_LOCATIONS_NAME, data=location_value)
                        loc_dataset.attrs["units"] = observable.location_units
                        loc_dataset.attrs["num_feature_dims"] = 0
                    else:
                        target_grp = observables_grp[parent]
                else:
                    target_grp = observables_grp.require_group(parent)
                _write_observable(target_grp, observable)

            # Save simulation input file if this is a SimulatedDataPoint2
            if hasattr(self, "simulation_metadata") and isinstance(self.simulation_metadata, SimulationMetadata):
                # Check if we have multiple simulations in this batch
                if len(self.observables) > 0 and self.observables[0].batch_dims:
                    # Multiple simulations: create simulation_input_file_0, _1, _2, etc.
                    batch_size = np.prod(self.observables[0].batch_dims)
                    for i in range(batch_size):
                        dataset_name = f"simulation_input_file_{i}"
                        lattice_grp.create_dataset(dataset_name, data=np.bytes_(self.simulation_metadata.simulation_input_file))
                else:
                    # Single simulation: use simple name
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

def _write_observable(grp, observable):
    """
    Write one observable into ``grp`` (observables/, a location group, or a multi-location group).

    Location-primary observables always get an ``@location`` attribute, on datasets and on
    each ParticleGroup group, regardless of whether they are nested in a location group.
    """
    location = None
    if observable.location_primary and observable.location is not None:
        location = str(observable.location[0])

    if observable.has_particlegroup:
        for name, idx in observable.entries():
            pg_grp = grp.create_group(name)
            observable.data[idx].write(pg_grp)
            if location is not None:
                pg_grp.attrs["location"] = location
        return

    data = np.asarray(observable.data)
    if data.dtype.kind == 'U':
        dataset = grp.create_dataset(observable.data_name, data=data.astype(object), dtype=h5py.string_dtype('utf-8'))
    else:
        dataset = grp.create_dataset(observable.data_name, data=data)

    if location is not None:
        dataset.attrs["location"] = location
    dataset.attrs["read_only"] = observable.read_only
    dataset.attrs["num_feature_dims"] = observable.num_feature_dims
    dataset.attrs["units"] = observable.units_str
    if observable.num_feature_dims > 0:
        dataset.attrs["bin_size"] = observable.attrs['bin_size']
        dataset.attrs["offset"] = observable.attrs['offset']


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
