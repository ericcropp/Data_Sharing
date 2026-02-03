"""
Tests for PV_table functionality.

These tests verify that:
- PV_table can be added via add_lattice()
- PV_table is correctly saved as datasets in HDF5 files
- PV mapping datasets have correct structure and content
"""
import pytest
import tempfile
import h5py
import numpy as np
from pathlib import Path
from data_standard import DataPoint2


def test_pv_table_save_and_structure():
    """Test that PV_table is saved correctly as datasets in HDF5."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a simple data point with PV_table
        D = DataPoint2()
        
        # Add some test data
        D.add_observable(
            batch_dims=(),
            num_feature_dims=0,
            location=['Screen1'],
            data=np.array(250.0),
            data_name='charge',
            units='pC',
            control=False
        )
        
        # Add lattice with PV_table
        pv_table = {
            'SOLN:IN10:121': 'SOL10121',
            'QUAD:IN10:121': 'CQ10121',
            'QUAD:IN10:122': 'SQ10122',
            'PROF:IN10:571': 'PR10571'
        }
        
        D.add_lattice(
            lattice_location='https://example.com/lattice',
            PV_table=pv_table
        )
        
        # Add run information
        D.add_run_information(
            source='Test',
            date='2026-02-03',
            notes='PV table test'
        )
        
        # Save HDF5 file
        D.saveHDF5(tmpdir)
        
        # Verify file was created
        h5_files = list(Path(tmpdir).glob('*.h5'))
        assert len(h5_files) == 1, "Expected one HDF5 file"
        
        # Open and verify structure
        with h5py.File(h5_files[0], 'r') as f:
            # Check lattice group exists
            assert 'lattice' in f, "Lattice group missing"
            
            # Check lattice_mapping dataset exists
            assert 'lattice_mapping' in f['lattice'], "lattice_mapping dataset missing"
            
            # Read lattice_mapping data
            lattice_mapping = f['lattice/lattice_mapping'][:]
            
            # Verify shape is 2xn
            assert lattice_mapping.shape[0] == 2, "lattice_mapping must have 2 rows"
            assert lattice_mapping.shape[1] == len(pv_table), f"Expected {len(pv_table)} columns"
            
            # Decode and verify content
            pv_dict_from_file = {
                lattice_mapping[0, i].decode(): lattice_mapping[1, i].decode()
                for i in range(lattice_mapping.shape[1])
            }
            
            assert pv_dict_from_file == pv_table, "PV table content mismatch"


def test_pv_table_empty():
    """Test that empty or None PV_table doesn't create datasets."""
    with tempfile.TemporaryDirectory() as tmpdir:
        D = DataPoint2()
        
        D.add_observable(
            batch_dims=(),
            num_feature_dims=0,
            location=['Screen1'],
            data=np.array(100.0),
            data_name='charge',
            units='pC',
            control=False
        )
        
        # Add lattice without PV_table
        D.add_lattice(lattice_location='https://example.com/lattice')
        
        D.add_run_information(
            source='Test',
            date='2026-02-03',
            notes='No PV table test'
        )
        
        D.saveHDF5(tmpdir)
        
        h5_files = list(Path(tmpdir).glob('*.h5'))
        assert len(h5_files) == 1
        
        with h5py.File(h5_files[0], 'r') as f:
            assert 'lattice' in f
            # lattice_mapping should not exist when PV_table is empty
            assert 'lattice_mapping' not in f['lattice'], "lattice_mapping should not exist for empty PV_table"


def test_pv_table_special_characters():
    """Test PV_table with special characters in names."""
    with tempfile.TemporaryDirectory() as tmpdir:
        D = DataPoint2()
        
        D.add_observable(
            batch_dims=(),
            num_feature_dims=0,
            location=['BPM1'],
            data=np.array(5.0),
            data_name='position_x',
            units='mm',
            control=False
        )
        
        # PV names often have colons and numbers
        pv_table = {
            'BPMS:IN10:221:X': 'BPM10221',
            'QUAD:LI10:361:BCTRL': 'QA10361',
            'TCAV:IN20:490:POC1': 'TCY10490'
        }
        
        D.add_lattice(
            lattice_location='https://example.com/lattice',
            PV_table=pv_table
        )
        
        D.add_run_information(source='Test', date='2026-02-03', notes='Special char test')
        D.saveHDF5(tmpdir)
        
        h5_files = list(Path(tmpdir).glob('*.h5'))
        with h5py.File(h5_files[0], 'r') as f:
            lattice_mapping = f['lattice/lattice_mapping'][:]
            
            # Verify shape
            assert lattice_mapping.shape == (2, len(pv_table)), f"Expected shape (2, {len(pv_table)})"
            
            # Decode and create dict from 2xn array
            pv_dict_from_file = {
                lattice_mapping[0, i].decode(): lattice_mapping[1, i].decode()
                for i in range(lattice_mapping.shape[1])
            }
            
            # Verify all special characters preserved
            assert pv_dict_from_file == pv_table, "PV table with special characters mismatch"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
