"""
Tests for the bin_size attribute type rules.

These tests verify that:
- bin_size may be a float (coerced to float) when num_feature_dims > 0
- bin_size may be a string naming another observable (a key) whose data has
  dimension batch_dims, enabling per-shot bin sizes for stacked images
- A string bin_size is validated (key must exist and match batch_dims) on the
  strict/final check, and round-trips through HDF5
"""
import pytest
import tempfile
import h5py
import numpy as np
from pathlib import Path

from data_standard import DataPoint2
from data_standard.Data_Standard_2 import SingleObservable, Observables


def test_bin_size_float_is_coerced_to_float():
    """A float bin_size is accepted and stored as a float (backward compatible)."""
    obs = SingleObservable(
        batch_dims=(3,),
        num_feature_dims=2,
        location=['Screen1'],
        data=np.zeros((3, 4, 5)),
        attrs={'bin_size': 1e-6, 'offset': 0.0},
        data_name='image',
        units='um',
    )
    assert isinstance(obs.attrs['bin_size'], float)
    assert obs.attrs['bin_size'] == 1e-6


def test_bin_size_int_is_coerced_to_float():
    """An integer bin_size is coerced to float."""
    obs = SingleObservable(
        batch_dims=(),
        num_feature_dims=2,
        location=['Screen1'],
        data=np.zeros((4, 5)),
        attrs={'bin_size': 1, 'offset': 0},
        data_name='image',
        units='um',
    )
    assert isinstance(obs.attrs['bin_size'], float)
    assert obs.attrs['bin_size'] == 1.0


def test_bin_size_string_key_valid_passes_checker():
    """A string bin_size naming a key whose data shape == batch_dims is valid."""
    obs = Observables()
    obs.add_observable(
        batch_dims=(3,),
        num_feature_dims=0,
        location=['Screen1'],
        data=np.array([1e-6, 2e-6, 3e-6]),
        data_name='PixelResolution',
        units='um',
    )
    obs.add_observable(
        batch_dims=(3,),
        num_feature_dims=2,
        location=['Screen1'],
        data=np.zeros((3, 100, 200)),
        attrs={'bin_size': 'PixelResolution', 'offset': 0.0},
        data_name='image',
        units='counts',
    )
    # Strict check should not raise.
    obs.observable_checker()
    # The string key is preserved, not coerced to float.
    image = [o for o in obs if o.data_name == 'image'][0]
    assert image.attrs['bin_size'] == 'PixelResolution'


def test_bin_size_string_key_multidim_batch():
    """The key dimension check works for multi-dimensional batch_dims."""
    obs = Observables()
    obs.add_observable(
        batch_dims=(2, 3),
        num_feature_dims=0,
        location=['Screen1'],
        data=np.zeros((2, 3)),
        data_name='PixelResolution',
        units='um',
    )
    obs.add_observable(
        batch_dims=(2, 3),
        num_feature_dims=2,
        location=['Screen1'],
        data=np.zeros((2, 3, 100, 200)),
        attrs={'bin_size': 'PixelResolution', 'offset': 0.0},
        data_name='image',
        units='counts',
    )
    obs.observable_checker()


def test_bin_size_string_key_missing_raises():
    """A string bin_size that matches no observable data_name is rejected."""
    obs = Observables()
    obs.add_observable(
        batch_dims=(3,),
        num_feature_dims=2,
        location=['Screen1'],
        data=np.zeros((3, 100, 200)),
        attrs={'bin_size': 'NoSuchKey', 'offset': 0.0},
        data_name='image',
        units='counts',
    )
    with pytest.raises(ValueError, match="must match the data_name"):
        obs.observable_checker()


def test_bin_size_string_key_wrong_dimension_raises():
    """A string bin_size whose key does not have dimension batch_dims is rejected."""
    obs = Observables()
    # Multi-location scalar -> data shape (3, 2), not equal to batch_dims (3,).
    obs.add_observable(
        batch_dims=(3,),
        num_feature_dims=0,
        location=['A', 'B'],
        data=np.zeros((3, 2)),
        data_name='WrongCal',
        units='m',
        location_units='m',
        location_primary=False,
    )
    obs.add_observable(
        batch_dims=(3,),
        num_feature_dims=2,
        location=['Screen1'],
        data=np.zeros((3, 100, 200)),
        attrs={'bin_size': 'WrongCal', 'offset': 0.0},
        data_name='image',
        units='counts',
    )
    with pytest.raises(ValueError, match="must reference data with dimension"):
        obs.observable_checker()


def test_bin_size_string_requires_nonempty_batch_dims():
    """A string bin_size is only meaningful with non-empty batch_dims."""
    with pytest.raises(ValueError, match="string key when batch_dims is non-empty"):
        SingleObservable(
            batch_dims=(),
            num_feature_dims=2,
            location=['Screen1'],
            data=np.zeros((4, 5)),
            attrs={'bin_size': 'SomeKey', 'offset': 0.0},
            data_name='image',
            units='um',
        )


def test_bin_size_string_key_may_be_added_after_reference():
    """The key may be added after the referencing observable; only the final check enforces it."""
    obs = Observables()
    # Add the referencing image before the key exists: incremental checks must not raise.
    obs.add_observable(
        batch_dims=(3,),
        num_feature_dims=2,
        location=['Screen1'],
        data=np.zeros((3, 100, 200)),
        attrs={'bin_size': 'PixelResolution', 'offset': 0.0},
        data_name='image',
        units='counts',
    )
    # Now add the key referenced above.
    obs.add_observable(
        batch_dims=(3,),
        num_feature_dims=0,
        location=['Screen1'],
        data=np.array([1e-6, 2e-6, 3e-6]),
        data_name='PixelResolution',
        units='um',
    )
    # The strict check now passes.
    obs.observable_checker()


def test_bin_size_invalid_type_raises():
    """A bin_size that is neither float-convertible nor a string is rejected."""
    with pytest.raises(TypeError, match="bin_size must be a float or a string key"):
        SingleObservable(
            batch_dims=(3,),
            num_feature_dims=2,
            location=['Screen1'],
            data=np.zeros((3, 4, 5)),
            attrs={'bin_size': [1, 2, 3], 'offset': 0.0},
            data_name='image',
            units='um',
        )


def test_bin_size_string_round_trip_hdf5():
    """A string bin_size key is validated on save and persists in the HDF5 file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        D = DataPoint2()
        D.add_observable(
            batch_dims=(3,),
            num_feature_dims=0,
            location=['Screen1'],
            data=np.array([1e-6, 2e-6, 3e-6]),
            data_name='PixelResolution',
            units='um',
            control=False,
        )
        D.add_observable(
            batch_dims=(3,),
            num_feature_dims=2,
            location=['Screen1'],
            data=np.zeros((3, 100, 200)),
            data_name='image',
            units='counts',
            attrs={'bin_size': 'PixelResolution', 'offset': 0.0},
        )
        D.add_lattice(lattice_location='https://example.com/lattice')
        D.add_run_information(source='Test', date='2026-09-17', notes='string bin_size round-trip')
        D.saveHDF5(tmpdir)

        h5_files = list(Path(tmpdir).glob('*.h5'))
        assert len(h5_files) == 1
        with h5py.File(h5_files[0], 'r') as f:
            image = f['observables']['Screen1']['image']
            assert str(image.attrs['bin_size']) == 'PixelResolution'


def test_bin_size_string_key_missing_rejected_on_save():
    """saveHDF5 finalizes the data point, so an invalid string key is rejected on save."""
    with tempfile.TemporaryDirectory() as tmpdir:
        D = DataPoint2()
        D.add_observable(
            batch_dims=(3,),
            num_feature_dims=2,
            location=['Screen1'],
            data=np.zeros((3, 100, 200)),
            data_name='image',
            units='counts',
            attrs={'bin_size': 'NoSuchKey', 'offset': 0.0},
        )
        D.add_lattice(lattice_location='https://example.com/lattice')
        D.add_run_information(source='Test', date='2026-09-17', notes='invalid key')
        with pytest.raises(ValueError, match="must match the data_name"):
            D.saveHDF5(tmpdir)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
