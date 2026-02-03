"""
Test that the quickstart example from README.md actually works.

This ensures the documentation stays in sync with the API.
"""
import pytest
import numpy as np
import tempfile
import shutil
from pathlib import Path
from data_standard import DataPoint2, SimulatedDataPoint2
import h5py


def test_quickstart_example():
    """Test that the README quickstart example runs without errors."""
    # Use a temporary directory for output
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / 'output'
        output_dir.mkdir()
        
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
            control=False
        )

        # Add a 2D image (e.g., screen image)
        image_array = np.zeros((100, 200))
        D.add_observable(
            batch_dims=(),
            num_feature_dims=2,  # 2D image
            location=['Screen1'],
            data=image_array,
            data_name='image',
            units='counts',
            attrs={'bin_size': 1e-6, 'offset': 0.0}  # Required for feature dims > 0
        )

        # Add lattice and metadata
        D.add_lattice(lattice_location='https://github.com/slaclab/facet2-lattice')
        D.add_run_information(source='FACET-II', date='2025-01-26', notes='Example run')

        # Finalize and save
        D.saveHDF5(str(output_dir))
        
        # Verify the file was created
        h5_files = list(output_dir.glob('*.h5'))
        assert len(h5_files) == 1, "Expected exactly one HDF5 file to be created"
        
        # Verify basic file structure
        with h5py.File(h5_files[0], 'r') as f:
            assert 'lattice' in f, "Missing lattice group"
            assert 'observables' in f, "Missing observables group"
            assert 'ID' in f.attrs, "Missing ID attribute"
            
            # Verify observables were saved
            obs_grp = f['observables']
            assert 'ICT1' in obs_grp, "Missing ICT1 location group"
            assert 'Screen1' in obs_grp, "Missing Screen1 location group"
            
            # Verify charge observable
            assert 'charge' in obs_grp['ICT1'], "Missing charge dataset"
            charge_ds = obs_grp['ICT1']['charge']
            assert charge_ds[()] == 250.0, "Charge value incorrect"
            assert charge_ds.attrs['units'] == 'C', "Charge units incorrect (should be base unit)"
            assert charge_ds.attrs['unit_multiplier'] == 1e-12, "Charge unit_multiplier incorrect"
            
            # Verify image observable
            assert 'image' in obs_grp['Screen1'], "Missing image dataset"
            image_ds = obs_grp['Screen1']['image']
            assert image_ds.shape == (100, 200), "Image shape incorrect"
            assert image_ds.attrs['num_feature_dims'] == 2, "num_feature_dims incorrect"
