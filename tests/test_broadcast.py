"""
Tests for batch broadcasting: a size-1 batch dimension applies to every shot, so data that is
constant across (part of) a scan is stored once.
"""
import h5py
import numpy as np
import pytest
from pathlib import Path
from pmd_beamphysics import ParticleGroup

from data_standard import DataPoint2, validate_file
from data_standard.Data_Standard_2 import SingleObservable, Observables, is_broadcastable_batch_shape


def _particle_group(n=10):
    return ParticleGroup(data={
        'x': np.random.randn(n), 'px': np.zeros(n), 'y': np.zeros(n), 'py': np.zeros(n),
        'z': np.zeros(n), 'pz': np.ones(n) * 1e6, 't': np.zeros(n), 'status': np.ones(n),
        'weight': np.ones(n) * 1e-15, 'species': 'electron',
    })


@pytest.mark.parametrize("shape, batch_dims, ok", [
    ((5,), (5,), True),
    ((1,), (5,), True),
    ((), (), True),
    ((1, 4), (3, 4), True),
    ((3, 1), (3, 4), True),
    ((1, 1), (3, 4), True),
    ((2,), (5,), False),
    ((5,), (), False),
    ((4,), (3, 4), False),
])
def test_is_broadcastable_batch_shape(shape, batch_dims, ok):
    assert is_broadcastable_batch_shape(shape, batch_dims) is ok


@pytest.mark.parametrize("data_shape", [(1,), (5,)])
def test_scalar_broadcast_accepted(data_shape):
    obs = SingleObservable(batch_dims=(5,), location='GUNF', data=np.ones(data_shape), data_name='GUNF:rf_field_scale', units='V/m')
    assert obs.batch_shape == data_shape


def test_constant_image_stored_once(tmp_path):
    D = DataPoint2()
    D.add_observable(batch_dims=(5,), location='BPM1', data=np.arange(5.0), data_name='BPM1:X', units='mm')
    D.add_observable(batch_dims=(5,), num_feature_dims=2, location='VCCF', data=np.ones((1, 40, 30)),
                     data_name='CAMR:LT10:900:Image', units='counts', attrs={'bin_size': 1e-6, 'offset': 0.0},
                     read_only=False)
    D.add_lattice(lattice_location='https://example.com/lattice')
    D.add_run_information(source='Test', date='2026-10-20', notes='broadcast')
    D.saveHDF5(str(tmp_path))
    path = next(Path(tmp_path).glob('*.h5'))
    with h5py.File(path, 'r') as f:
        assert f['observables/CAMR:LT10:900:Image'].shape == (1, 40, 30)
    report = validate_file(path)
    assert report.ok and report.warnings == [], report.format()


def test_partial_broadcast_two_dims():
    obs = Observables()
    obs.add_observable(batch_dims=(3, 4), location='A', data=np.zeros((3, 4)), data_name='A:X', units='m')
    obs.add_observable(batch_dims=(3, 4), location='B', data=np.zeros((1, 4)), data_name='B:X', units='m')
    obs.add_observable(batch_dims=(3, 4), location='C', data=np.zeros((3, 1)), data_name='C:X', units='m')
    obs.observable_checker()


@pytest.mark.parametrize("data_shape", [(2,), (5, 1)])
def test_wrong_batch_shape_rejected(data_shape):
    with pytest.raises(ValueError, match="dimension"):
        SingleObservable(batch_dims=(5,), location='A', data=np.zeros(data_shape), data_name='A:X', units='m')


def test_broadcast_multi_location_data():
    obs = Observables()
    obs.add_observable(batch_dims=(5,), location=[0.0, 1.0, 2.0], data=np.zeros((1, 3)), data_name='sigma_x',
                       units='m', location_units='s', location_primary=False)
    obs.observable_checker()


def test_partially_broadcast_particle_groups(tmp_path):
    pgs = np.empty((1, 2), dtype=object)
    for idx in np.ndindex(pgs.shape):
        pgs[idx] = _particle_group()
    D = DataPoint2()
    D.add_observable(batch_dims=(3, 2), location='VCCF', data=pgs, data_name='initial_particles', units='', read_only=False)
    D.add_observable(batch_dims=(3, 2), location='BPM1', data=np.zeros((3, 2)), data_name='BPM1:X', units='mm')
    D.add_lattice(lattice_location='https://example.com/lattice')
    D.add_run_information(source='Test', date='2026-10-20', notes='broadcast PGs')
    D.saveHDF5(str(tmp_path))
    path = next(Path(tmp_path).glob('*.h5'))
    with h5py.File(path, 'r') as f:
        assert set(f['observables/VCCF'].keys()) == {'initial_particles_0_0', 'initial_particles_0_1'}
    report = validate_file(path)
    assert report.ok and report.warnings == [], report.format()


def test_bin_size_key_may_be_broadcast():
    obs = Observables()
    obs.add_observable(batch_dims=(3,), location='PR10571', data=np.array([1e-6]), data_name='PROF:IN10:571:RESOLUTION', units='m')
    obs.add_observable(batch_dims=(3,), num_feature_dims=2, location='PR10571', data=np.zeros((3, 4, 5)),
                       data_name='PROF:IN10:571:Image', units='counts',
                       attrs={'bin_size': 'PROF:IN10:571:RESOLUTION', 'offset': 0.0})
    obs.observable_checker()


def test_broadcast_changes_id():
    def make(data):
        D = DataPoint2()
        D.add_observable(batch_dims=(3,), location='A', data=data, data_name='A:X', units='m')
        D.add_lattice(lattice_location='x')
        return D.make_ID().ID
    assert make(np.ones(1)) != make(np.ones(3))


def test_validator_flags_bad_particle_group_grid(tmp_path):
    pgs = np.empty(3, dtype=object)
    for i in range(3):
        pgs[i] = _particle_group()
    D = DataPoint2()
    D.add_observable(batch_dims=(3,), location='VCCF', data=pgs, data_name='initial_particles', units='')
    D.add_observable(batch_dims=(3,), location='BPM1', data=np.zeros(3), data_name='BPM1:X', units='mm')
    D.add_lattice(lattice_location='https://example.com/lattice')
    D.add_run_information(source='Test', date='2026-10-20', notes='bad grid')
    D.saveHDF5(str(tmp_path))
    path = next(Path(tmp_path).glob('*.h5'))
    with h5py.File(path, 'a') as f:
        del f['observables/VCCF/initial_particles_2']
    report = validate_file(path)
    assert any("do not form a grid" in e for e in report.errors), report.format()


def _save(D, tmp_path):
    D.add_lattice(lattice_location='https://example.com/lattice')
    D.add_run_information(source='Test', date='2026-10-20', notes='broadcast')
    D.saveHDF5(str(tmp_path))
    return next(Path(tmp_path).glob('*.h5'))


def test_broadcast_timestamp_and_multi_location_validate(tmp_path):
    D = DataPoint2()
    D.add_observable(batch_dims=(3,), location='BPM1', data=np.arange(3.0), data_name='BPM1:X', units='mm')
    D.add_timestamp(['2026-10-20T13:00:00-07:00'], batch_dims=(3,))
    D.add_observable(batch_dims=(3,), location=[0.0, 1.0, 2.0, 3.0], data=np.zeros((1, 4)), data_name='sigma_x',
                     units='m', location_units='s', location_group='multi_location_data_impact')
    path = _save(D, tmp_path)
    with h5py.File(path, 'r') as f:
        assert f['observables/Timestamp'].shape == (1,)
        assert f['observables/multi_location_data_impact/sigma_x'].shape == (1, 4)
    report = validate_file(path)
    assert report.ok and report.warnings == [], report.format()


@pytest.mark.parametrize("bad_shape", [(2,), (3, 1)])
def test_validator_rejects_non_broadcastable_shape(tmp_path, bad_shape):
    D = DataPoint2()
    D.add_observable(batch_dims=(3,), location='BPM1', data=np.arange(3.0), data_name='BPM1:X', units='mm')
    D.add_observable(batch_dims=(3,), location='GUNF', data=[0.5], data_name='GUNF:rf_field_scale', units='V/m')
    path = _save(D, tmp_path)
    with h5py.File(path, 'a') as f:
        ds = f['observables/GUNF:rf_field_scale']
        attrs = dict(ds.attrs)
        del f['observables/GUNF:rf_field_scale']
        f.create_dataset('observables/GUNF:rf_field_scale', data=np.zeros(bad_shape)).attrs.update(attrs)
    report = validate_file(path)
    assert any("GUNF:rf_field_scale" in e for e in report.errors), report.format()


def test_validator_rejects_non_broadcastable_bin_size_key(tmp_path):
    D = DataPoint2()
    D.add_observable(batch_dims=(3,), location='PR10571', data=[1e-6], data_name='PROF:IN10:571:RESOLUTION', units='m')
    D.add_observable(batch_dims=(3,), num_feature_dims=2, location='PR10571', data=np.zeros((3, 4, 5)),
                     data_name='PROF:IN10:571:Image', units='counts',
                     attrs={'bin_size': 'PROF:IN10:571:RESOLUTION', 'offset': 0.0})
    path = _save(D, tmp_path)
    assert validate_file(path).ok
    with h5py.File(path, 'a') as f:
        attrs = dict(f['observables/PR10571/PROF:IN10:571:RESOLUTION'].attrs)
        del f['observables/PR10571/PROF:IN10:571:RESOLUTION']
        f.create_dataset('observables/PR10571/PROF:IN10:571:RESOLUTION', data=np.zeros(2)).attrs.update(attrs)
    report = validate_file(path)
    assert any("must reference data with dimension" in e for e in report.errors), report.format()
