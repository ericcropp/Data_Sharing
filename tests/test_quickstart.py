"""
Test that the quickstart example from README.md actually works.

This ensures the documentation stays in sync with the API.
"""
import pytest
import numpy as np
import tempfile
import shutil
from pathlib import Path
from data_standard import DataPoint2, SimulatedDataPoint2, validate_file
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
            read_only=True  # measured quantity (legacy equivalent: control=False)
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

        # Optional per-shot timestamps (ISO 8601 with UTC offset), shaped like batch_dims
        D.add_timestamp('2025-01-26T13:55:40.050-08:00')

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
            
            # Each location holds a single observable, so both are stored at the top
            # level of observables/ with their location in the @location attribute.
            obs_grp = f['observables']
            assert 'charge' in obs_grp, "Missing charge dataset"
            charge_ds = obs_grp['charge']
            assert charge_ds[()] == 250.0, "Charge value incorrect"
            assert charge_ds.attrs['location'] == 'ICT1'
            assert charge_ds.attrs['units'] == 'pC', "Units must be stored with their SI prefix"
            assert 'unit_multiplier' not in charge_ds.attrs
            assert bool(charge_ds.attrs['read_only']) is True
            assert 'control' not in charge_ds.attrs
            
            assert 'image' in obs_grp, "Missing image dataset"
            image_ds = obs_grp['image']
            assert image_ds.attrs['location'] == 'Screen1'
            assert image_ds.shape == (100, 200), "Image shape incorrect"
            assert image_ds.attrs['num_feature_dims'] == 2, "num_feature_dims incorrect"

            assert obs_grp['Timestamp'].asstr()[()] == '2025-01-26T13:55:40.050-08:00'

        report = validate_file(h5_files[0])
        assert report.ok, report.format()
        assert report.warnings == []
