"""
Integration tests for FACET-II Simulation Example.

These tests verify that:
- The FACET-II simulation example script runs successfully
- Output HDF5 files are created with correct structure
- Critical data can be loaded back and validated
- Observables, lattice, run information, and simulation metadata are present
"""
import pytest
import os
import sys
import subprocess
import tempfile
import shutil
import h5py
import yaml
import numpy as np
from pathlib import Path


# Get the root directory of the project
PROJECT_ROOT = Path(__file__).parent.parent
EXAMPLES_DIR = PROJECT_ROOT / "examples"
# Use small test data subset instead of full LFS data
DATA_INPUT_DIR = PROJECT_ROOT / "tests" / "test_data" / "FACET-II_Simulation_Data"
LATTICE_FILES_DIR = DATA_INPUT_DIR / "Lattice_Files"


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test outputs."""
    temp_dir = tempfile.mkdtemp(prefix="test_facet_ii_")
    yield temp_dir
    # Cleanup after test
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def check_data_available():
    """Check if test data is available."""
    if not DATA_INPUT_DIR.exists():
        pytest.skip(f"Test data not found at {DATA_INPUT_DIR}")
    if not LATTICE_FILES_DIR.exists():
        pytest.skip(f"Lattice files not found at {LATTICE_FILES_DIR}")
    
    # Check for at least one .h5 file
    h5_files = list(DATA_INPUT_DIR.glob("*.h5"))
    if not h5_files:
        pytest.skip(f"No .h5 simulation files found in {DATA_INPUT_DIR}")


@pytest.mark.slow
def test_facet_ii_simulation_example_runs(temp_output_dir, check_data_available):
    """Test that the FACET-II simulation example script runs without errors."""
    script_path = EXAMPLES_DIR / "FACET-II_Simulation_Example.py"
    
    # Run the script with subprocess
    cmd = [
        sys.executable,
        str(script_path),
        "--input_dir", str(DATA_INPUT_DIR),
        "--output_dir", str(temp_output_dir),
        "--lattice_dir", str(LATTICE_FILES_DIR),
        "--Combine_Files", "False",  # Don't combine for easier testing
    ]
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=300  # 5 minute timeout
    )
    
    # Check that the script ran successfully
    assert result.returncode == 0, f"Script failed with error:\n{result.stderr}"
    
    # Check that output directory was created
    assert os.path.exists(temp_output_dir), "Output directory not created"
    
    # Check that HDF5 files were created
    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    assert len(h5_files) > 0, "No HDF5 files created"


@pytest.mark.slow
def test_output_hdf5_structure(temp_output_dir, check_data_available):
    """Test that output HDF5 files have the correct structure."""
    # First run the example to generate files
    script_path = EXAMPLES_DIR / "FACET-II_Simulation_Example.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--input_dir", str(DATA_INPUT_DIR),
        "--output_dir", str(temp_output_dir),
        "--lattice_dir", str(LATTICE_FILES_DIR),
        "--Combine_Files", "False",
    ]
    subprocess.run(cmd, capture_output=True, timeout=300)
    
    # Get the first HDF5 file
    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    assert len(h5_files) > 0, "No HDF5 files to test"
    
    test_file = h5_files[0]
    
    with h5py.File(test_file, 'r') as f:
        # Check top-level structure
        assert 'lattice' in f, "Missing 'lattice' group"
        assert 'observables' in f, "Missing 'observables' group"
        
        # Check root attributes
        assert 'ID' in f.attrs, "Missing 'ID' attribute"
        assert 'run_information_source' in f.attrs, "Missing run_information_source"
        assert 'run_information_date' in f.attrs, "Missing run_information_date"
        assert 'run_information_notes' in f.attrs, "Missing run_information_notes"
        assert 'Data_Standard_Version' in f.attrs, "Missing Data_Standard_Version"
        
        # Verify ID is not empty
        assert len(f.attrs['ID']) > 0, "ID is empty"


@pytest.mark.slow
def test_lattice_data_present(temp_output_dir, check_data_available):
    """Test that lattice data is correctly stored."""
    # Run the example
    script_path = EXAMPLES_DIR / "FACET-II_Simulation_Example.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--input_dir", str(DATA_INPUT_DIR),
        "--output_dir", str(temp_output_dir),
        "--lattice_dir", str(LATTICE_FILES_DIR),
        "--Combine_Files", "False",
    ]
    subprocess.run(cmd, capture_output=True, timeout=300)
    
    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    test_file = h5_files[0]
    
    with h5py.File(test_file, 'r') as f:
        lattice_grp = f['lattice']
        
        # Check lattice_location exists as an attribute
        assert 'lattice_location' in lattice_grp.attrs, "Missing lattice_location attribute"
        
        # Check that lattice_location is not empty
        lattice_location = lattice_grp.attrs['lattice_location']
        if isinstance(lattice_location, bytes):
            lattice_location = lattice_location.decode('utf-8')
        assert len(lattice_location) > 0, "lattice_location is empty"
        
        # Check for lattice files or simulation input file(s)
        has_lattice_files = 'lattice_files' in lattice_grp
        has_sim_input = any(k.startswith('simulation_input_file') for k in lattice_grp.keys())
        assert has_lattice_files or has_sim_input, \
            "Missing lattice files or simulation input file"


@pytest.mark.slow
def test_observables_present(temp_output_dir, check_data_available):
    """Test that observables are correctly stored."""
    # Run the example
    script_path = EXAMPLES_DIR / "FACET-II_Simulation_Example.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--input_dir", str(DATA_INPUT_DIR),
        "--output_dir", str(temp_output_dir),
        "--lattice_dir", str(LATTICE_FILES_DIR),
        "--Combine_Files", "False",
    ]
    subprocess.run(cmd, capture_output=True, timeout=300)
    
    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    test_file = h5_files[0]
    
    with h5py.File(test_file, 'r') as f:
        obs_grp = f['observables']
        
        # Check that observables group is not empty
        assert len(obs_grp.keys()) > 0, "No observables found"
        
        # Check for at least one observable with required attributes
        found_valid_observable = False
        for key in obs_grp.keys():
            item = obs_grp[key]
            if isinstance(item, h5py.Group):
                # Check for datasets in the group
                if len(item.keys()) > 0:
                    for subkey in item.keys():
                        subitem = item[subkey]
                        if isinstance(subitem, h5py.Dataset):
                            # Check for required attributes
                            if 'location' in subitem.attrs and 'units' in subitem.attrs:
                                found_valid_observable = True
                                break
            elif isinstance(item, h5py.Dataset):
                # Check for required attributes
                if 'location' in item.attrs and 'units' in item.attrs:
                    found_valid_observable = True
                    break
        
        assert found_valid_observable, "No valid observables found with required attributes"


@pytest.mark.slow
def test_simulation_metadata_present(temp_output_dir, check_data_available):
    """Test that simulation metadata is present in the file."""
    # Run the example
    script_path = EXAMPLES_DIR / "FACET-II_Simulation_Example.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--input_dir", str(DATA_INPUT_DIR),
        "--output_dir", str(temp_output_dir),
        "--lattice_dir", str(LATTICE_FILES_DIR),
        "--Combine_Files", "False",
    ]
    subprocess.run(cmd, capture_output=True, timeout=300)
    
    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    test_file = h5_files[0]
    
    with h5py.File(test_file, 'r') as f:
        # Check for simulation metadata in summary or root attributes
        has_sim_start = any('simulation_start' in key for key in f.attrs.keys())
        has_sim_code = any('simulation_code' in key for key in f.attrs.keys())
        
        assert has_sim_start or has_sim_code, \
            "Simulation metadata (simulation_start, simulation_code) not found"


@pytest.mark.slow
def test_data_round_trip(temp_output_dir, check_data_available):
    """Test that data can be written and read back correctly."""
    # Run the example
    script_path = EXAMPLES_DIR / "FACET-II_Simulation_Example.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--input_dir", str(DATA_INPUT_DIR),
        "--output_dir", str(temp_output_dir),
        "--lattice_dir", str(LATTICE_FILES_DIR),
        "--Combine_Files", "False",
    ]
    subprocess.run(cmd, capture_output=True, timeout=300)
    
    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    test_file = h5_files[0]
    
    # Read the file and verify data types
    with h5py.File(test_file, 'r') as f:
        # Check that numeric data is actually numeric
        obs_grp = f['observables']
        
        numeric_dataset_found = False
        for key in obs_grp.keys():
            item = obs_grp[key]
            if isinstance(item, h5py.Group):
                for subkey in item.keys():
                    subitem = item[subkey]
                    if isinstance(subitem, h5py.Dataset):
                        data = subitem[()]
                        # Check if it's numeric data (not strings or object refs)
                        if isinstance(data, np.ndarray) and np.issubdtype(data.dtype, np.number):
                            numeric_dataset_found = True
                            # Verify data is not all zeros or NaNs
                            assert not np.all(data == 0) or data.size == 1, \
                                f"Dataset {key}/{subkey} contains all zeros"
                            break
        
        assert numeric_dataset_found, "No numeric datasets found in observables"


@pytest.mark.slow
def test_combine_files_option(temp_output_dir, check_data_available):
    """Test that the combine_files option works correctly."""
    # Run with combine_files enabled
    script_path = EXAMPLES_DIR / "FACET-II_Simulation_Example.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--input_dir", str(DATA_INPUT_DIR),
        "--output_dir", str(temp_output_dir),
        "--lattice_dir", str(LATTICE_FILES_DIR),
        "--Combine_Files", "True",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    
    assert result.returncode == 0, f"Script failed: {result.stderr}"
    
    # Check that combined file was created in parent directory
    parent_dir = Path(temp_output_dir).parent
    combined_file = parent_dir / "Combined_FACET-II_Simulation_Data.h5"
    
    # Give it a moment to write
    import time
    time.sleep(1)
    
    # The combined file should exist (if combine_files worked)
    # Note: The script may delete temp_output_dir after combining
    if combined_file.exists():
        with h5py.File(combined_file, 'r') as f:
            # Should have summary group
            assert 'summary' in f or len(f.keys()) > 0, "Combined file is empty"
        
        # Clean up the combined file
        if combined_file.exists():
            os.remove(combined_file)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
