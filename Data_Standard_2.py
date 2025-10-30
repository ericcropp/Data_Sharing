"""
Module: Data_Standard_2
This module provides classes and utilities for representing, validating, and saving standardized data points for accelerator physics simulations and experiments. 
It supports scalar inputs, input distributions (images or ParticleGroup), lattice information, outputs, summaries, run information, and simulation metadata. 
Data can be serialized to HDF5 format.

Classes:
--------
SingleInput:
    Represents a single scalar input parameter.
    Attributes:
        name (str): Name of the input.
        value: Value of the input.
        location (str): Location or context of the input.
        units (str): Units of the input value.
        description (str): Description of the input.
    Methods:
        to_dict(): Returns a dictionary representation of the input.
SingleOutput:
    Represents a single output datum.
    Attributes:
        location: Location(s) associated with the output.
        datum: Output value(s).
        attrs (dict): Additional attributes.
        datum_name (str): Name of the output datum.
        datum_type (str): Type of datum ('scalar', 'image', 'distribution').
    Raises:
        TypeError, ValueError: For invalid types or mismatched data.
Inputs:
    Manages scalar inputs and input distributions.
    Attributes:
        scalar_inputs (dict): Dictionary of SingleInput instances.
        input_distribution: Input distribution (image or ParticleGroup).
        input_distribution_attrs (dict): Attributes for input distribution.
    Methods:
        input_distribution_checker(allow_blank): Validates input distribution.
        add_input_distribution(input_distribution, input_distribution_attrs): Adds input distribution.
        check_scalar_inputs(allow_blank): Validates scalar inputs.
        add_scalar_inputs(scalar_inputs): Adds scalar inputs.
Lattice:
    Represents lattice configuration.  This will be replaced by PALS lattice standard in the future.
    Attributes:
        lattice_location (str): Location of the lattice.
        lattice_files (list or dict): Lattice files or their contents.
    Methods:
        process_lattice_files(lattice_files): Loads lattice files.
        add_lattice(lattice_location, lattice_files): Adds lattice info.
        lattice_checker(allow_blank): Validates lattice info.
Outputs (list):
    Container for output data.
    Methods:
        add_output(location, datum, attrs, datum_name, datum_type): Adds an output.
        output_checker(allow_blank): Validates outputs.
Summary:
    Represents summary information for a data point.
    Attributes:
        summary_keys (list): Keys to include in summary.
        summary_location: Location for summary extraction.
    Methods:
        add_summary(summary_keys, summary_location): Adds summary info.
        summary_checker(allow_blank): Validates summary info.
RunInformation:
    Stores metadata about the run.
    Attributes:
        source (str): Source of the run.
        date (str): Date of the run.
        notes (str): Additional notes.
    Methods:
        add_run_information(source, date, notes): Adds run info.
        run_info_checker(allow_blank): Validates run info.
DataPoint2:
    Main class representing a standardized data point.
    Attributes:
        inputs (Inputs): Input data.
        lattice (Lattice): Lattice info.
        outputs (Outputs): Output data.
        summary (Summary): Summary info.
        ID (str): Unique identifier.
        run_information (RunInformation): Run metadata.
        scalar_output_list (list): List of scalar output names.
    Methods:
        make_ID(): Generates unique ID.
        add_inputs(...): Adds inputs.
        add_lattice(...): Adds lattice info.
        add_run_information(...): Adds run info.
        add_output(...): Adds output.
        add_summary(...): Adds summary info.
        get_summary(): Extracts summary data.
        checker(): Validates all components.
        finalize(): Finalizes and validates the data point.
        saveHDF5(fileloc): Saves the data point to HDF5.
SimulationMetadata:
    Stores simulation metadata.
    Attributes:
        simulation_start (str): Simulation start time.
        simulation_end (str): Simulation end time.
        simulation_code (str): Simulation code name.
        simulation_input_file (str): Input file for simulation.
    Methods:
        add_simulation_data(...): Adds simulation metadata.
        sim_data_checker(allow_blank): Validates simulation metadata.
SimulatedDataPoint2 (DataPoint2):
    Extends DataPoint2 to include simulation metadata.
    Attributes:
        simulation_metadata (SimulationMetadata): Simulation metadata.
    Methods:
        add_simulation_data(...): Adds simulation metadata.
Exceptions:
-----------
TypeError, ValueError, AssertionError: Raised for invalid input types, values, or missing required information.
Dependencies:
-------------
numpy, pandas, pmd_beamphysics.ParticleGroup, hashlib, json, h5py, os
"""
import numpy as np
import pandas as pd
from pmd_beamphysics import ParticleGroup, units
import hashlib
import json
import h5py
import os

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


    return prefix, valid_unit


"""
Represents a single scalar input parameter for the data standard.
"""
class SingleInput:
    def __init__(self, name="", value=None, location="", units="", description=""):
        """
        Initialize a SingleInput instance.
        Args:
            name (str): Name of the input.
            value: Value of the input.
            location (str): Beamline location of the input (str or float).
            units (str): Units of the input value.
            description (str): Description of the input.
        """
        if name == "" or value is None or units == "" or location == "":
            raise ValueError("name, value, units, and location must not be blank")
        self.name = name
        self.location = location
        prefix, valid_units = unit_checker(units)
        try:
            self.value = float(value) * prefix
        except Exception:
            self.value = np.nan
        self.units = units if valid_units == "Custom Unit" else valid_units
        self.description = description

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
        return {
            "name": self.name,
            "value": self.value,
            "location": self.location,
            "units": units_str,
            "description": self.description
        }


"""
Represents a single output datum for the data standard.
"""
class SingleOutput:
    def __init__(self, location="", datum=None, attrs=None, datum_name="", datum_type=None, units=None):
        """
        Initialize a SingleOutput instance.
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
            isinstance(location, (str, int, float, list))
        ):
            raise TypeError("location must be a string, number, or list")
        self.location = location

        self.datum = datum
        self.attrs = attrs if attrs is not None else {}
        self.datum_name = datum_name
        allowed_types = {"scalar", "image", "distribution"}
        if datum_type is not None and datum_type not in allowed_types:
            raise ValueError(f"datum_type must be one of {allowed_types}")
        self.datum_type = datum_type  # 'scalar' or 'image' or 'distribution'
        if self.datum_type == 'scalar':
            assert units is not None, "units must be provided for scalar datum_type"
        if isinstance(self.datum,list):
            self.datum = np.array(self.datum)
        prefix, valid_units = unit_checker(units)
        if isinstance(prefix, (int, float)):
            if self.datum_type == 'scalar':
                self.datum = self.datum * prefix
            
        self.units = units if valid_units == "Custom Unit" else valid_units
        if isinstance(self.location, list):
            assert isinstance(self.datum, (list, np.ndarray)), "If location is a list, datum must be a list or np.ndarray."
            assert len(self.location) == len(self.datum), "location and datum lists must have the same length."

        if self.datum_type == 'image':
            if isinstance(self.location,list):
                for d in self.datum:
                    if not isinstance(d, np.ndarray):
                        d = np.array(d)
                    assert isinstance(d, np.ndarray) and d.ndim == 2, "Each item in datum must be a 2D np.ndarray for image data."
            else:
                if not isinstance(self.datum, np.ndarray):
                    self.datum = np.array(self.datum)
                assert isinstance(self.datum, np.ndarray) and self.datum.ndim == 2, "datum must be a 2D np.ndarray for image data."
        elif self.datum_type == 'distribution':
            if isinstance(self.location,list):
                for d in self.datum:
                    if not isinstance(d, ParticleGroup):
                        d = ParticleGroup(d)
                    assert isinstance(d, ParticleGroup), "Each item in datum must be a ParticleGroup for distribution data."
            else:
                if not isinstance(self.datum, ParticleGroup):
                    self.datum = ParticleGroup(self.datum)
                assert isinstance(self.datum, ParticleGroup), "datum must be a ParticleGroup for distribution data."

        elif self.datum_type == 'scalar':
            if isinstance(self.location,list):
                for d in self.datum:
                    assert isinstance(d, (int, float, np.integer, np.floating)), "Each item in datum must be a scalar (int or float) for scalar data."
            else:
                assert isinstance(self.datum, (int, float, np.integer, np.floating)), "datum must be a scalar (int or float) for scalar data."
        

def input_distribution_checker(input_distribution, input_distribution_attrs):
    """
    Checks the validity of the input distribution and its attributes.
    Args:
        input_distribution: The input distribution (numpy array or ParticleGroup).
        input_distribution_attrs (dict): Attributes for the input distribution.
    Returns:
        str: Type of input distribution ('image' or 'ParticleGroup').
    Raises:
        TypeError, ValueError, AssertionError: For invalid types or missing required attributes.
    """
    if not isinstance(input_distribution_attrs, dict):
        raise TypeError("input_distribution_attrs must be a dict")

    arr = np.array(input_distribution)
    if arr.ndim == 2:
        # It's an image
        if "pixel_calibration" not in input_distribution_attrs:
            raise AssertionError("input_distribution_attrs must contain 'pixel_calibration' for image input_distribution")
        return "image"
    else:
        raise ValueError("input_distribution must be a 2D numpy array to be considered an image")

"""
Manages scalar inputs and input distributions for the data standard.
"""
class Inputs:
    def input_distribution_checker(self,allow_blank = False):
        """
        Validates the input distribution and its attributes.
        Args:
            allow_blank (bool): If True, allows blank input distribution.
        Returns:
            str: Type of input distribution ('image' or 'ParticleGroup').
        Raises:
            TypeError, ValueError, AssertionError: For invalid types or missing required attributes.
        """
        if allow_blank:
            if (self.input_distribution is None or (isinstance(self.input_distribution, (list, dict)) and len(self.input_distribution) == 0)) and \
                (self.input_distribution_attrs is None or (isinstance(self.input_distribution_attrs, dict) and len(self.input_distribution_attrs) == 0)):
                return None
        if not isinstance(self.input_distribution_attrs, dict):
            raise TypeError("input_distribution_attrs must be a dict")

        if isinstance(self.input_distribution, np.ndarray):
            arr = self.input_distribution
            if arr.ndim == 2:
                # It's an image
                if "pixel_calibration" not in self.input_distribution_attrs:
                    raise AssertionError("input_distribution_attrs must contain 'pixel_calibration' for image input_distribution")
                return "image"
            else:
                raise ValueError("input_distribution must be a 2D numpy array to be considered an image")
        elif isinstance(self.input_distribution, ParticleGroup):
            # Accept ParticleGroup as valid
            return "ParticleGroup"
        else:
            raise ValueError("input_distribution must be a 2D numpy array or ParticleGroup, but received type: {}".format(type(self.input_distribution)))
        
    def add_input_distribution(self, input_distribution, input_distribution_attrs):
        """
        Adds input distribution and its attributes.
        Args:
            input_distribution: The input distribution (numpy array or ParticleGroup).
            input_distribution_attrs (dict): Attributes for the input distribution.
        """
        self.input_distribution = input_distribution if input_distribution is not None else {}
        self.input_distribution_attrs = input_distribution_attrs if input_distribution_attrs is not None else {}
        self.input_distribution_checker(allow_blank=True)

    def check_scalar_inputs(self,allow_blank = False):
        """
        Validates scalar inputs.
        Args:
            allow_blank (bool): If True, allows blank scalar inputs.
        Raises:
            TypeError: For invalid types.
        """
        if not isinstance(self.scalar_inputs, dict):
            raise TypeError("scalar_inputs must be a dict")
        if allow_blank and len(self.scalar_inputs) == 0:
            return
        for name, single_input in self.scalar_inputs.items():
            if not isinstance(single_input, SingleInput):
                raise TypeError(f"Each item in scalar_inputs must be a SingleInput instance. Error at {name}")

    def add_scalar_inputs(self, scalar_inputs):
        """
        Adds scalar inputs to the Inputs instance.
        Args:
            scalar_inputs: Scalar inputs (DataFrame, dict, list, or Series).
        Raises:
            TypeError, ValueError: For invalid types or missing required keys.
        """
        if scalar_inputs is not None:
            if isinstance(scalar_inputs, pd.DataFrame):
                required_cols = {"name", "value", "location", "units", "description"}
                if required_cols.issubset(scalar_inputs.columns):
                    for _, row in scalar_inputs.iterrows():
                        self.scalar_inputs[row["name"]] = SingleInput(
                            name=row["name"],
                            value=row["value"],
                            location=row["location"],
                            units=row["units"],
                            description=row["description"]
                    )
                else:
                    raise ValueError(f"scalar_inputs DataFrame must have columns: {required_cols}")
            elif isinstance(scalar_inputs, dict):
                for name, attrs in scalar_inputs.items():
                    if not isinstance(attrs, dict):
                        raise TypeError("Each scalar input must be a dictionary")
                    self.scalar_inputs[name] = SingleInput(
                        name=name,
                        value=attrs.get("value"),
                        location=attrs.get("location"),
                        units=attrs.get("units"),
                        description=attrs.get("description")
                    )
            elif isinstance(scalar_inputs,list):
                for item in scalar_inputs:
                    if not isinstance(item, dict) or "name" not in item:
                        raise ValueError("Each item in the list must be a dictionary with at least a 'name' key")
                    self.scalar_inputs[item["name"]] = SingleInput(
                        name=item["name"],
                        value=item.get("value"),
                        location=item.get("location"),
                        units=item.get("units"),
                        description=item.get("description")
                    )
            elif isinstance(scalar_inputs, pd.Series):
                # Convert Series to DataFrame of length 1 and process as DataFrame
                df = scalar_inputs.to_frame().T if isinstance(scalar_inputs, pd.Series) else pd.DataFrame([scalar_inputs])
                required_cols = {"name", "value", "location", "units", "description"}
                if required_cols.issubset(df.columns):
                    for _, row in df.iterrows():
                        self.scalar_inputs[row["name"]] = SingleInput(
                            name=row["name"],
                            value=row["value"],
                            location=row["location"],
                            units=row["units"],
                            description=row["description"]
                        )
                else:
                    raise ValueError(f"scalar_inputs Series must have keys: {required_cols}")
            else:
                raise TypeError("scalar_inputs must be a pandas DataFrame, dict of dicts, list of dicts, or pandas Series")
        self.check_scalar_inputs(allow_blank=True)
            
    def __init__(self, scalar_inputs=None, input_distribution=None, input_distribution_attrs=None):
        """
        Initialize Inputs instance.
        Args:
            scalar_inputs: Scalar inputs.
            input_distribution: Input distribution.
            input_distribution_attrs: Attributes for input distribution.
        """
        self.scalar_inputs = {}
        self.add_input_distribution(input_distribution, input_distribution_attrs)
        self.add_scalar_inputs(scalar_inputs)

"""
Represents lattice configuration for the data standard.  This will be replaced by PALS lattice standard in the future.
"""
class Lattice:
    def __init__(self, lattice_location=None, lattice_files=None):
        """
        Initialize Lattice instance.
        Args:
            lattice_location (str): Location of the lattice.
            lattice_files (list or dict): Lattice files or their contents.
        """
        self.add_lattice(lattice_location, lattice_files)

    

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
    
    def add_lattice(self, lattice_location, lattice_files=None):
        """
        Adds lattice location and files.
        Args:
            lattice_location (str): Location of the lattice.
            lattice_files (list or dict): Lattice files or their contents.
        """
        self.lattice_location = lattice_location
        self.lattice_files = lattice_files if lattice_files is not None else []
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
            raise TypeError("lattice_location must be a string")
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

"""
Container for output data for the data standard.
"""
class Outputs(list):
    def __init__(self, output_list=None):
        """
        Initialize Outputs instance.
        Args:
            output_list (list): List of output dictionaries.
        """
        super().__init__()
        output_list = output_list if output_list is not None else []

        # self = []
        for output in output_list:

            self.add_output(output["location"], output["datum"], output["units"], output.get("attrs"), output.get("datum_name", ""), output.get("datum_type", None))

    def add_output(self, location, datum, units='', attrs=None, datum_name="", datum_type=None):
        """
        Adds an output to the Outputs list.
        Args:
            location: Location(s) associated with the output.
            datum: Output value(s).
            attrs (dict): Additional attributes.
            datum_name (str): Name of the output datum.
            datum_type (str): Type of datum ('scalar', 'image', 'distribution').
        """
        assert location is not None, "Output 'location' must not be None"
        assert datum is not None, "Output 'datum' must not be None"
        output = SingleOutput(
            location=location,
            datum=datum,
            attrs=attrs,
            datum_name=datum_name,
            datum_type=datum_type,
            units=units
        )
        self.append(output)
        self.output_checker(allow_blank=True)

    def output_checker(self,allow_blank = False):
        """
        Validates outputs in the Outputs list.
        Args:
            allow_blank (bool): If True, allows blank outputs.
        Raises:
            TypeError, ValueError, AssertionError: For invalid types or mismatched data.
        """
        if allow_blank and len(self) == 0:
            return
        for output in self:
            if not isinstance(output, SingleOutput):
                raise TypeError("Each item in outputs must be a SingleOutput instance.")
            if output.datum_type not in {"scalar", "image", "distribution", None}:
                raise ValueError("datum_type must be 'scalar', 'image', 'distribution', or None.")
            if output.datum_type == 'image':
                if isinstance(output.location,list):
                    for d in output.datum:
                        if not isinstance(d, np.ndarray):
                            d = np.array(d)
                        assert isinstance(d, np.ndarray) and d.ndim == 2, "Each item in datum must be a 2D np.ndarray for image data."
                else:
                    if not isinstance(output.datum, np.ndarray):
                        output.datum = np.array(output.datum)
                    assert isinstance(output.datum, np.ndarray) and output.datum.ndim == 2, "datum must be a 2D np.ndarray for image data."
            elif output.datum_type == 'distribution':
                if isinstance(output.location,list):
                    for d in output.datum:
                        if not isinstance(d, ParticleGroup):
                            d = ParticleGroup(d)
                        assert isinstance(d, ParticleGroup), "Each item in datum must be a ParticleGroup for distribution data."
                else:
                    if not isinstance(output.datum, ParticleGroup):
                        output.datum = ParticleGroup(output.datum)
                    assert isinstance(output.datum, ParticleGroup), "datum must be a ParticleGroup for distribution data."
            elif output.datum_type == 'scalar':
                if isinstance(output.location,list):
                    for d in output.datum:
                        assert isinstance(d, (int, float, np.integer, np.floating)), "Each item in datum must be a scalar (int or float) for scalar data."
                else:
                    assert isinstance(output.datum, (int, float, np.integer, np.floating)), "datum must be a scalar (int or float) for scalar data."
    

"""
Represents summary information for a data point.
"""
class Summary:
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

"""
Stores metadata about the run for the data standard.
"""
class RunInformation:
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


"""
Main class representing a standardized data point for the data standard.
"""
class DataPoint2:
    def __init__(self, scalar_inputs=None, input_distribution=None, lattice_location=None, lattice_files=None,
                 output_list=None, summary_keys=None, summary_location='final', ID="", run_information=None,
                 outputs=None, summary=None, input_distribution_attrs=None):
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
        self.inputs = Inputs(scalar_inputs=scalar_inputs, input_distribution=input_distribution,
                             input_distribution_attrs=input_distribution_attrs)
        self.lattice = Lattice(lattice_location=lattice_location, lattice_files=lattice_files)
        self.outputs = Outputs(output_list=output_list)
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
        # returns hash of the object
        scalar_inputs_str = json.dumps({k: v.to_dict() for k, v in self.inputs.scalar_inputs.items()}, sort_keys=True)
        id_string = f"{scalar_inputs_str}{self.lattice.lattice_location}"
        self.ID = hashlib.md5(id_string.encode()).hexdigest()
        return self
    
    def add_inputs(self, scalar_inputs=None, input_distribution=None, input_distribution_attrs=None):
        """
        Adds inputs to the data point.
        Args:
            scalar_inputs: Scalar inputs.
            input_distribution: Input distribution.
            input_distribution_attrs: Attributes for input distribution.
        Returns:
            self: The DataPoint2 instance.
        """
        if scalar_inputs is not None:
            self.inputs.add_scalar_inputs(scalar_inputs)
        if input_distribution is not None or input_distribution_attrs is not None:
            self.inputs.add_input_distribution(input_distribution, input_distribution_attrs)
        return self

    def add_lattice(self, lattice_location=None, lattice_files=None):
        """
        Adds lattice information to the data point.
        Args:
            lattice_location: Location of the lattice.
            lattice_files: Lattice files or their contents.
        Returns:
            self: The DataPoint2 instance.
        """
        self.lattice.add_lattice(lattice_location, lattice_files)
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

    def add_output(self, location, datum, units='', attrs=None, datum_name="", datum_type=None):
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
        self.outputs.add_output(location, datum, units, attrs, datum_name, datum_type)
        if datum_type == 'scalar':
            self.scalar_output_list.append(datum_name)
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
        Extracts summary data for the data point.
        Returns:
            dict: Summary data.
        """
        summary = {}
        for key in self.summary.summary_keys:
            if key in self.inputs.scalar_inputs:
                summary[key] = self.inputs.scalar_inputs[key].value
            elif hasattr(self.run_information, key):
                summary[key] = getattr(self.run_information, key)
            elif key == 'ID':
                summary[key] = self.ID
            elif key.split(':')[-1] in self.scalar_output_list:
                output_dict = next((d for d in self.outputs if d.datum_name == key.split(':')[-1]), None)
                if output_dict is not None:
                    if isinstance(output_dict.location, list) and len(output_dict.location) > 1:
                        if self.summary.summary_location == 'final':
                            idx = -1
                        elif isinstance(self.summary.summary_location, str):
                            if self.summary.summary_location in output_dict.location:
                                idx = next(i for i, loc in enumerate(output_dict.location) if loc == self.summary.summary_location)
                        else:
                            idx = next(i for i, loc in enumerate(output_dict.location) if np.isclose(loc, self.summary.summary_location))
                        if idx is not None:
                            summary[key] = float(output_dict.datum[idx])
                    else:
                        summary[key] = float(output_dict.datum)
        summary["ID"] = self.ID
        if hasattr(self, "simulation_metadata") and isinstance(self.simulation_metadata, SimulationMetadata):

            summary["simulation_start"] = self.simulation_metadata.simulation_start
            summary["simulation_end"] = self.simulation_metadata.simulation_end
            summary["simulation_code"] = self.simulation_metadata.simulation_code
            # "simulation_input_file": self.simulation_metadata.simulation_input_file
        

        self.summary.summary = summary
        self.summary.summary_keys = list(summary.keys())
        return self

    def checker(self):
        """
        Validates all components of the data point.
        Returns:
            self: The DataPoint2 instance.
        """
        self.inputs.check_scalar_inputs()
        self.inputs.input_distribution_checker()
        self.lattice.lattice_checker()
        self.outputs.output_checker()
        self.run_information.run_info_checker()
        self.summary.summary_checker()
        if hasattr(self, "simulation_metadata") and isinstance(self.simulation_metadata, SimulationMetadata):
            self.simulation_metadata.sim_data_checker()
        return self

    def finalize(self):
        """
        Finalizes and validates the data point.
        Returns:
            self: The DataPoint2 instance.
        """
        self.make_ID()
        self.get_summary()
        self.checker()
        return self

    def saveHDF5(self, fileloc=None):
        """
        Saves the data point to HDF5 format.
        Args:
            fileloc (str): File location or directory to save the HDF5 file.
        """
        self.finalize()
        # Save to HDF5
        # If fileloc is a directory, append filename
        if fileloc is None:
            filename = f"{self.ID}.h5"
        elif os.path.isdir(fileloc):
            filename = os.path.join(fileloc, f"{self.ID}.h5")
        else:
            filename = fileloc
        # Note: self.inputs, self.lattice, self.outputs, self.summary, self.ID, self.run_information
        with h5py.File(filename, "w") as f:
            # Save inputs
            inputs_grp = f.create_group("inputs")
            # Scalar inputs
            for name, single_input in self.inputs.scalar_inputs.items():
                input_grp = inputs_grp.create_dataset(name,data=single_input.value)
                input_grp.attrs["name"] = single_input.name
                input_grp.attrs["location"] = single_input.location
                if isinstance(single_input.units, str):
                    input_grp.attrs["units"] = single_input.units
                else:
                    input_grp.attrs["units"] = getattr(single_input.units, "unitSymbol", str(single_input.units))
                input_grp.attrs["description"] = single_input.description
            # Input distribution
            if isinstance(self.inputs.input_distribution, np.ndarray):
                inputs_grp.create_dataset("input_distribution", data=self.inputs.input_distribution)
            elif isinstance(self.inputs.input_distribution, ParticleGroup):
                self.inputs.input_distribution.write(inputs_grp.create_group("input_distribution"))
            # Input distribution attrs
            if hasattr(self.inputs, "input_distribution_attrs") and self.inputs.input_distribution_attrs:
                for k, v in self.inputs.input_distribution_attrs.items():
                    inputs_grp["input_distribution"].attrs[k] = v

            # Save lattice
            lattice_grp = f.create_group("lattice")
            lattice_grp.create_dataset("lattice_location", data=self.lattice.lattice_location)
            if isinstance(self.lattice.lattice_files, dict):
                lattice_files_grp = lattice_grp.create_group("lattice_files")
                for fname, contents in self.lattice.lattice_files.items():
                    lattice_files_grp.create_dataset(fname, data=np.bytes_(contents))

            # Save outputs
            outputs_grp = f.create_group("observables")
            for i, output in enumerate(self.outputs):
                if output.datum_type == "scalar":
                    out_grp = outputs_grp.create_dataset(output.datum_name, data=output.datum)
                elif output.datum_type == "image":
                    out_grp = outputs_grp.create_dataset(output.datum_name, data=np.array(output.datum))
                elif output.datum_type == "distribution":
                    out_grp = outputs_grp.create_group(output.datum_name)
                    if isinstance(output.datum, ParticleGroup):
                        output.datum.write(out_grp)
                # out_grp = outputs_grp.create_group(output.datum_name)
                out_grp.attrs["location"] = output.location
                out_grp.attrs["datum_type"] = output.datum_type

                if isinstance(output.units, str):
                    out_grp.attrs["units"] = output.units
                else:
                    out_grp.attrs["units"] = getattr(output.units, "unitSymbol", str(output.units))
                    
                for k, v in output.attrs.items():
                    out_grp.attrs[k] = v
                # Save datum

            if hasattr(self, "simulation_metadata") and isinstance(self.simulation_metadata, SimulationMetadata):
                # sim_meta_grp = f.create_group("simulation_metadata")
                # Save simulation_input_file as a dataset within the group
                lattice_grp.create_dataset("simulation_input_file", data=np.bytes_(self.simulation_metadata.simulation_input_file))
                
                


            # Save ID and run_information
            f.attrs["ID"] = self.ID
            f.attrs["run_information_source"] = self.run_information.source
            f.attrs["run_information_date"] = self.run_information.date
            f.attrs["run_information_notes"] = self.run_information.notes
            for key in self.summary.summary_keys:
                f.attrs[key] = getattr(self.summary, "summary", {}).get(key, "")
            f.attrs["summary_location"] = self.summary.summary_location

"""
Stores simulation metadata for the data standard.
"""
class SimulationMetadata:
    def __init__(self, simulation_start="", simulation_end="", simulation_code="", simulation_input_file=""):
        """
        Initialize SimulationMetadata instance.
        Args:
            simulation_start (str): Simulation start time.
            simulation_end (str): Simulation end time.
            simulation_code (str): Simulation code name.
            simulation_input_file (str): Input file for simulation.
        """
        self.add_simulation_data(simulation_start, simulation_end, simulation_code, simulation_input_file)
        self.sim_data_checker(allow_blank=True)

    def add_simulation_data(self, simulation_start, simulation_end, simulation_code, simulation_input_file):
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

class SimulatedDataPoint2(DataPoint2):
    

    def __init__(self, scalar_inputs=None, input_distribution=None, lattice_location=None, lattice_files=None,
                 output_list=None, summary_keys=None, summary_location='final', ID="", run_information=None,
                 outputs=None, summary=None, input_distribution_attrs=None,
                 simulation_start=None, simulation_end=None, simulation_code="", simulation_input_file=""):
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
        super().__init__(scalar_inputs=scalar_inputs, input_distribution=input_distribution,
                         lattice_location=lattice_location, lattice_files=lattice_files,
                         output_list=output_list, summary_keys=summary_keys, summary_location=summary_location,
                         ID=ID, run_information=run_information, outputs=outputs, summary=summary,
                         input_distribution_attrs=input_distribution_attrs)
        self.simulation_metadata = SimulationMetadata(
            simulation_start=str(simulation_start) if simulation_start is not None else "",
            simulation_end=str(simulation_end) if simulation_end is not None else "",
            simulation_code=str(simulation_code) if simulation_code is not None else "",
            simulation_input_file=str(simulation_input_file) if simulation_input_file is not None else ""
        )

    def add_simulation_data(self, simulation_start=None, simulation_end=None, simulation_code="", simulation_input_file=""):
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
        self.simulation_metadata.add_simulation_data(simulation_start, simulation_end, simulation_code, simulation_input_file)
        return self
