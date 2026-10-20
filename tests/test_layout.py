"""
Tests for the observables/ layout:

- a location holding exactly one observable is stored directly under observables/
  with its location in @location
- a location holding several observables keeps its observables/<location>/ group
- every location-based entry (dataset or ParticleGroup group) carries @location
"""
from pathlib import Path

import h5py
import numpy as np
import pytest
from pmd_beamphysics import ParticleGroup

from data_standard import DataPoint2, validate_file
from data_standard.Data_Standard_2 import Observables


def _particle_group(n=10):
    return ParticleGroup(data={
        'x': np.random.randn(n), 'px': np.zeros(n), 'y': np.zeros(n), 'py': np.zeros(n),
        'z': np.zeros(n), 'pz': np.ones(n) * 1e6, 't': np.zeros(n), 'status': np.ones(n),
        'weight': np.ones(n) * 1e-15, 'species': 'electron',
    })


def _save(D, tmp_path):
    D.add_lattice(lattice_location='https://example.com/lattice')
    D.add_run_information(source='Test', date='2026-10-16', notes='layout')
    D.saveHDF5(str(tmp_path))
    return next(Path(tmp_path).glob('*.h5'))


def test_single_and_shared_locations(tmp_path):
    D = DataPoint2()
    D.add_observable(batch_dims=(2,), location='BPM10781', data=[1.0, 2.0], data_name='BPMS:IN10:781:TMIT', units='')
    D.add_observable(batch_dims=(2,), location='BPM10781', data=[0.1, 0.2], data_name='BPMS:IN10:781:X', units='mm')
    D.add_observable(batch_dims=(2,), location='QA10361', data=[3.0, 4.0], data_name='QUAD:IN10:361:BCTRL', units='kG', read_only=False)
    path = _save(D, tmp_path)
    with h5py.File(path, 'r') as f:
        obs = f['observables']
        assert set(obs.keys()) == {'BPM10781', 'QUAD:IN10:361:BCTRL'}
        assert set(obs['BPM10781'].keys()) == {'BPMS:IN10:781:TMIT', 'BPMS:IN10:781:X'}
        for name in ('BPMS:IN10:781:TMIT', 'BPMS:IN10:781:X'):
            assert obs['BPM10781'][name].attrs['location'] == 'BPM10781'
        assert obs['QUAD:IN10:361:BCTRL'].attrs['location'] == 'QA10361'
    report = validate_file(path)
    assert report.ok and report.warnings == [], report.format()


@pytest.mark.parametrize("batch_dims", [(), (3,), (2, 2)])
def test_particle_groups_flattened_and_nested_have_location(batch_dims, tmp_path):
    pgs = np.empty(batch_dims, dtype=object)
    for idx in np.ndindex(batch_dims):
        pgs[idx] = _particle_group()
    D = DataPoint2()
    # Only observable at VCCF -> flattened
    D.add_observable(batch_dims=batch_dims, location='VCCF', data=pgs, data_name='initial_particles', units='', read_only=False)
    # PR10241 has a ParticleGroup and a scalar -> nested
    D.add_observable(batch_dims=batch_dims, location='PR10241', data=pgs.copy(), data_name='241_particles', units='')
    D.add_observable(batch_dims=batch_dims, location='PR10241', data=np.ones(batch_dims), data_name='PROF:IN10:241:XRMS', units='um')
    path = _save(D, tmp_path)
    with h5py.File(path, 'r') as f:
        obs = f['observables']
        for idx in np.ndindex(batch_dims):
            i = '_'.join(map(str, idx)) if idx else '0'
            flat = obs[f'initial_particles_{i}']
            nested = obs['PR10241'][f'241_particles_{i}']
            assert flat.attrs['location'] == 'VCCF'
            assert nested.attrs['location'] == 'PR10241'
            ParticleGroup(h5=flat)  # still readable as openPMD
        assert obs['PR10241']['PROF:IN10:241:XRMS'].attrs['location'] == 'PR10241'
    report = validate_file(path)
    assert report.ok and report.warnings == [], report.format()


def test_layout_collision_between_flattened_names_rejected():
    obs = Observables()
    obs.add_observable(location='CQ10121', data=np.array(1.0), data_name='b1_gradient', units='T/m')
    obs.add_observable(location='SQ10122', data=np.array(2.0), data_name='b1_gradient', units='T/m')
    with pytest.raises(ValueError, match="already used"):
        obs.observable_checker()


def test_layout_collision_between_group_and_dataset_rejected():
    obs = Observables()
    obs.add_observable(location='BPM1', data=np.array(1.0), data_name='BPM2', units='mm')
    obs.add_observable(location='BPM2', data=np.array(1.0), data_name='X', units='mm')
    obs.add_observable(location='BPM2', data=np.array(1.0), data_name='Y', units='mm')
    with pytest.raises(ValueError, match="both a group and"):
        obs.observable_checker()


def test_duplicate_name_within_location_rejected():
    obs = Observables()
    obs.add_observable(location='BPM1', data=np.array(1.0), data_name='X', units='mm')
    obs.add_observable(location='BPM1', data=np.array(2.0), data_name='X', units='mm')
    with pytest.raises(ValueError, match="Duplicate observable"):
        obs.observable_checker()


def test_data_name_with_slash_rejected():
    obs = Observables()
    with pytest.raises(ValueError, match="must not contain '/'"):
        obs.add_observable(location='BPM1', data=np.array(1.0), data_name='BPM1/X', units='mm')


def test_multi_location_consistency_checked_standalone():
    obs = Observables()
    obs.add_observable(batch_dims=(2,), location=[0.0, 1.0], data=np.zeros((2, 2)), data_name='sigma_x',
                       units='m', location_units='m', location_primary=False)
    obs.add_observable(batch_dims=(2,), location=[0.0, 2.0], data=np.zeros((2, 2)), data_name='sigma_y',
                       units='m', location_units='m', location_primary=False)
    with pytest.raises(ValueError, match="same location"):
        obs.observable_checker()


def test_multi_location_requires_location_units():
    obs = Observables()
    with pytest.raises(ValueError, match="location_units must be specified"):
        obs.add_observable(batch_dims=(2,), location=[0.0, 1.0], data=np.zeros((2, 2)), data_name='sigma_x',
                           units='m', location_primary=False)
