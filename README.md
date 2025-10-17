# Data_Sharing
Standardized format for storing simulation and measured data

## Overview
The **Data_Sharing** project provides a unified format for storing both simulation and measured data. This standard aims to facilitate data sharing and interoperability across different tools, workflows, and institutions. Includes:
- Consistent data organization
- Support for multiple data types (e.g., ParticleGroups, scalars, images, metadata) and simulations AND experimental data
- Extensible format for future requirements

Examples for formatting FACET-II experimental data are contained in **Experiment2DataStandard2.py**

Examples for formatting simulations of the same beamline, using a custom lattice (so lattice is included) are shown in **Lume2DataStandard2.py**

The results of the real data example are shown in **Test_Data2/**

The results of the simulated data example are shown in **Test_Sim_Data2/**

The standard data classes are found in **Data_Standard_2.py**

The data used for making experimental and simulation examples are in **Ex_Experimental_Data2/** and **Ex_Simulation_Data2/**, respectively
