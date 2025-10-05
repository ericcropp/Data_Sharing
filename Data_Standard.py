# from abc import ABC, abstractmethod
"""
Module: Data_Standard.py
This module defines the DataPoint and SimulatedDataPoint classes for representing and managing data points (i.e. runs 
of the accelerator or simulation),including input parameters, distributions, lattice information, outputs, and metadata. 
The classes provide validation, unique identification, summary extraction, and serialization to HDF5 format.

Notes:
1) The lattice specification should eventually point to or be a PALS standard.  That is not currently implemented
2) In general, for both experimental and simulation data, lattice information is treated as default unless overridden 
by other input information.
3) Generally, for a batch of DataPoint objects, a batch YAML file lists all the summary data and locations of the files
for easy indexing.  Examples are shown in other files in the repo.  
4) Future to-do: batches can be put in a single h5 file, with shared information stored once.  This is not yet implemented.

Classes:
--------
class DataPoint(BaseModel):
    Represents a single data point (i.e. experimental shot or simulation run) for simulation or experimental data.
    Attributes:
        scalar_inputs (dict[str, float]): Scalar input parameters for the simulation.
        input_distribution (Union[ParticleGroup, np.ndarray]): Input particle distribution or array.
        lattice_location (str): Location of the lattice ('included' or other). If included, lattice data must be provided.
        lattice (Any, optional): Lattice data, required if lattice_location == 'included'.
        output_list (list): List of output data dictionaries.
        summary_keys (list[str]): Keys to include in the summary.
        summary_location (Union[str, float]): Location for summary extraction ('final' or specific value). Default is 'final'. 
        ID (str): Unique identifier for the data point (MD5 hash).
        run_information (dict): Metadata about the simulation run.
        outputs (dict): Dictionary of outputs.
        summary (dict): Summary dictionary.
        scalar_output_list (list): List of scalar output names.
        input_distribution_attrs (dict): Attributes for input distribution.
    Methods:
        validate_inputs(values): Validates and normalizes input data before model creation.
        __init__(**data): Initializes the DataPoint and generates a unique ID.
        add_data(location, datum, attrs, datum_type, datum_name): Adds output data to the output_list.
        make_ID(): Generates a unique ID based on scalar_inputs and lattice_location.
        get_summary(): Extracts summary values based on summary_keys and summary_location.
        saveHDF5(fileloc): Serializes the DataPoint to an HDF5 file.
    Usage:
        Create a DataPoint instance with required inputs, add output data, and save to HDF5.
class SimulatedDataPoint(DataPoint):
    Extends DataPoint to include simulation-specific metadata.
    Additional Attributes:
        simulation_start (Union[str, float]): Simulation start time or identifier.
        simulation_end (Union[str, float]): Simulation end time or identifier.
        simulation_code (str): Name of the simulation code used.
        simulation_input_file (str): Path or identifier for the simulation input file.
    Usage:
        Use SimulatedDataPoint for data points that require simulation metadata.

"""        
from pydantic import BaseModel,model_validator
import hashlib
from typing import Union,Any
import pandas as pd
from pmd_beamphysics import ParticleGroup
import numpy as np
import h5py
import json

class DataPoint(BaseModel):
    model_config = {
        "arbitrary_types_allowed": True
    }
    scalar_inputs: dict[str, float]
    input_distribution: Union[ParticleGroup, np.ndarray]
    lattice_location: str
    lattice: Any = None  # Only used if lattice_location == 'included'
    output_list: list[Union[dict[str, str, float, dict], dict[float, str, float, dict]]] = []
    summary_keys: list[str]
    summary_location: Union[str, float] = 'final'
    ID: str = ""
    run_information: dict[str, Any]
    outputs: dict = {}
    summary: dict = {}  
    scalar_output_list: list = [] 
    input_distribution_attrs: dict = {}

    @model_validator(mode="before")
    @classmethod
    def validate_inputs(cls, values):
        scalar_inputs = values.get("scalar_inputs")
        if isinstance(scalar_inputs, pd.Series):
            values["scalar_inputs"] = scalar_inputs.to_dict()
        elif not isinstance(scalar_inputs, dict):
            raise ValueError("scalar_inputs must be a dict or pandas Series")

        input_distribution = values.get("input_distribution")
        if not isinstance(input_distribution, (ParticleGroup, np.ndarray)):
            raise ValueError("input_distribution must be a ParticleGroup or np.ndarray")
        # Add input_distribution_attrs to attrs for input_distribution if present
        input_distribution_attrs = values.get("input_distribution_attrs")
        # if input_distribution_attrs is not None:
        #     values.setdefault("attrs", {})
        #     values["attrs"]["input_distribution_attrs"] = input_distribution_attrs
        # outputs and output_list default handling
        values.setdefault("output_list", [])
        values.setdefault("outputs", {})
        lattice_location = values.get("lattice_location")
        lattice = values.get("lattice")
        if lattice_location == "included":
            if lattice is None:
                raise ValueError("If lattice_location is 'included', 'lattice' must be provided.")
        else:
            if lattice is not None:
                raise ValueError("If lattice_location is not 'included', 'lattice' must be None.")

        # ID will be set after model creation
        return values

    def __init__(self, **data):
        super().__init__(**data)
        self.make_ID()


    def add_data(self,location:Union[str,float] = None ,datum= None ,attrs= None ,datum_type= None ,datum_name= None ):
        #To-do location validator
        if datum_type not in ['scalar', 'ParticleGroup', 'image']:
            raise ValueError("datum_type must be 'scalar', 'ParticleGroup', or 'image'")
        datum_ID = 0
        if self.output_list is not None:

        # Check that datum_ID is unique
            existing_IDs = [d.get('ID') for d in self.output_list if 'ID' in d]
            while datum_ID in existing_IDs:
                datum_ID += 1
        self.output_list.append({'location': location, 'datum_type': datum_type,'ID': datum_ID,'datum': datum, 'attrs':attrs,'datum_name': datum_name})
        # self.outputs[datum_ID] = datum
        if isinstance(location, list):
            assert isinstance(datum, (list, np.ndarray)), "If location is a list, datum must be a list or np.ndarray."
            assert len(location) == len(datum), "location and datum lists must have the same length."
            # assert len(location) == len(datum), "location and datum lists must have the same length."
        if datum_type == 'scalar':
            self.scalar_output_list.append(datum_name)


        if datum_type == 'image':
            assert 'pixel_calibration' in attrs.keys()
        self.get_summary()
        return self

    def make_ID(self):
        # returns hash of the object
        scalar_inputs_str = json.dumps(self.scalar_inputs, sort_keys=True)
        id_string = f"{scalar_inputs_str}{self.lattice_location}"
        self.ID = hashlib.md5(id_string.encode()).hexdigest()
        return self
    
    def get_summary(self):
        summary = {}
        for key in self.summary_keys:
            if key in self.scalar_inputs:
                
                summary[key] = self.scalar_inputs[key]
            elif key in self.run_information:
                summary[key] = self.run_information[key]
            elif key == 'ID':
                summary[key] = self.ID
            elif key in self.scalar_output_list:
                output_dict = next((d for d in self.output_list if d.get('datum_name') == key), None)
                
                if len(output_dict['location']) > 1:
                    if self.summary_location == 'final':
                        idx = -1
                    elif type(self.summary_location) == str:
                        if self.summary_location in output_dict['location']:
                            idx = next(i for i, loc in enumerate(output_dict['location']) if loc == self.summary_location)
                    else:
                        idx = next(i for i, loc in enumerate(self.output_list[key]['location']) if np.isclose(loc, self.summary_location))
                    if idx is not None:
                        summary[key] = float(output_dict['datum'][idx])
                else:
                    summary[key] = float(output_dict['datum'])
            self.summary = summary
        return self

    def saveHDF5(self, fileloc=None):
        def save_dict_with_arrays_as_datasets(h5group, d):
            """
            Save a dict to an HDF5 group, storing arrays as datasets and the rest as JSON.
            """
            json_dict = {}
            for k, v in d.items():
                if isinstance(v, np.ndarray):
                    h5group.create_dataset(k, data=v)
                else:
                    json_dict[k] = v
            # Store the non-array part as JSON
            if json_dict:
                h5group.create_dataset('json', data=np.bytes_(json.dumps(json_dict)))
        if fileloc is None:
            filename = f"{self.ID}.h5"
        else:
            filename = fileloc + f"{self.ID}.h5"

        # Add self.ID to run_information before saving
        self.run_information['ID'] = self.ID

        with h5py.File(filename, 'w') as f:
            # Save inputs group (renamed from scalar_inputs)
            inputs_grp = f.create_group('inputs')
            for k, v in self.scalar_inputs.items():
                inputs_grp.create_dataset(k, data=v)
            # Save input_distribution as a key in inputs group
            input_distribution_attrs = getattr(self, "input_distribution_attrs", None)

            if isinstance(self.input_distribution, np.ndarray):
                inputs_grp.create_dataset('input_distribution', data=self.input_distribution)
                if input_distribution_attrs is not None:
                    assert "pixel_calibration" in input_distribution_attrs, "input_distribution_attrs must contain 'pixel_calibration'"
            
            
            elif isinstance(self.input_distribution, ParticleGroup):
                self.input_distribution.write(inputs_grp.create_group('input_distribution'))
            else:
                raise ValueError("Unsupported input_distribution type for HDF5 serialization.")
            
            for key, value in input_distribution_attrs.items():
                inputs_grp['input_distribution'].attrs[key] = value

            # Save lattice_location
            lattice_grp = f.create_group('Lattice')
            lattice_grp.create_dataset('Location', data=np.bytes_(self.lattice_location))
            if self.lattice_location == 'included':
                if self.lattice is not None:
                    # Assuming lattice is a ParticleGroup or similar object with a write method
                    lattice_subgrp = lattice_grp.create_group('Lattice_Files')
                    for fname, contents in self.lattice.items():
                        lattice_subgrp.create_dataset(fname, data=np.bytes_(contents))
                else:
                    raise ValueError("Lattice must be provided when lattice_location is 'included'.")
            # Save output_list
            if hasattr(self, "simulation_input_file") or hasattr(self, "simulation_code") or hasattr(self, "simulation_end") or hasattr(self, "simulation_start"):
                sim_info_grp = f.create_group("simulation_information")
                if hasattr(self, "simulation_input_file"):
                    sim_info_grp.create_dataset("simulation_input_file", data=np.bytes_(self.simulation_input_file))
                if hasattr(self, "simulation_code"):
                    sim_info_grp.attrs["simulation_code"] = self.simulation_code
                if hasattr(self, "simulation_start"):
                    sim_info_grp.attrs["simulation_start"] = self.simulation_start
                if hasattr(self, "simulation_end"):
                    sim_info_grp.attrs["simulation_end"] = self.simulation_end
            output_list_grp = f.create_group('outputs')
            for i, output in enumerate(self.output_list):
                datum = output.get('datum')
                datum_name = output.get('datum_name')
                if isinstance(datum, (np.ndarray, list)):
                    output_list_grp.create_dataset(str(datum_name), data=np.array(datum))
                elif isinstance(datum, (str, int, float, np.integer, np.floating)):
                    output_list_grp.create_dataset(str(datum_name), data=datum)
                elif isinstance(datum, ParticleGroup):
                        datum.write(output_list_grp.create_group(datum_name))
                else:
                    # For other types, store as JSON string
                    output_list_grp.create_dataset(str(datum_name), data=np.bytes_(json.dumps(datum)))
                # out_grp = output_list_grp.create_group(str(i))
                # Save output['datum'] according to the rules
                
                # if isinstance(datum, dict):
                    
                # else:
                #     # If datum is not a dict, save as a single dataset named 'datum'
                #     if isinstance(datum, (np.ndarray, list)):
                #         out_grp.create_dataset('datum', data=np.array(datum))
                #     elif isinstance(datum, (str, int, float, np.integer, np.floating)):
                #         out_grp.create_dataset('datum', data=datum)
                    
                #     else:
                #         out_grp.create_dataset('datum', data=np.bytes_(json.dumps(datum)))

                # Save the rest of the keys as attrs
                attrs = output.get('attrs') or {}
                # Save attrs as attributes of each output dataset/group
                datum_name = str(output.get('datum_name'))
                # Find the correct HDF5 object (dataset or group) just created
                h5obj = output_list_grp[datum_name]
                for k, v in attrs.items():
                    h5obj.attrs[k] = v
                h5obj.attrs['location'] = output['location']
                h5obj.attrs['datum_type'] = output['datum_type']
                h5obj.attrs['ID'] = output['ID']


            # Save summary_keys
            # f.create_dataset('summary_keys', data=np.array(self.summary_keys, dtype='S'))

            # Save ID
            # f.create_dataset('ID', data=np.bytes_(self.ID))

            # Save run_information as attrs (now includes ID)
            for k, v in self.run_information.items():
                f.attrs[k] = v

            for k, v in self.summary.items():
                f.attrs[k] = v

class SimulatedDataPoint(DataPoint, BaseModel):
    simulation_start: Union[str, float]
    simulation_end: Union[str, float]
    simulation_code: str
    simulation_input_file: str

