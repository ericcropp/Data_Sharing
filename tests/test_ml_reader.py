"""
Fast tests for examples/FACET-II_to_ML_Example.py on small synthetic Data Standard files.

Covers the reader behaviour added in 0.1.2:
- multi-location data in any 'multi_location_data*' group, with time as DATA_LOCATIONS and the
  screen index chosen per shot from 'mean_z'
- broadcast (size-1 batch dimension) scalars, multi-location data and ParticleGroups
- per-shot ParticleGroups in location groups and single ParticleGroups under their plain name
- files in the earlier layouts (z-based DATA_LOCATIONS, top-level per-shot ParticleGroups)
"""
import importlib.util
import json
from pathlib import Path

import h5py
import numpy as np
import pytest
from pmd_beamphysics import ParticleGroup

from data_standard import DataPoint2, combine_files, validate_file

ML_EXAMPLE = Path(__file__).parent.parent / "examples" / "FACET-II_to_ML_Example.py"
N = 3
SCREEN = "241"  # screen z = 0.94 m; PGs: initial_particles @ VCCF, 241_particles @ PR10241

# Beam position along the time steps for each shot; the step closest to z = 0.94 m differs per shot.
MEAN_Z = np.array([
    [0.00, 0.50, 0.94, 1.50],   # shot 0 -> step 2
    [0.00, 0.94, 1.20, 1.50],   # shot 1 -> step 1
    [0.20, 0.40, 0.60, 0.95],   # shot 2 -> step 3
])
PER_SHOT_STEP = np.array([2, 1, 3])
SIGMA_X = np.array([[10.0 * i + j for j in range(4)] for i in range(N)])   # varies per shot
SIGMA_Y = np.array([[100.0, 101.0, 102.0, 103.0]])                         # broadcast, shape (1, 4)


@pytest.fixture(scope="module")
def ml():
    spec = importlib.util.spec_from_file_location("facet_ii_to_ml_example", ML_EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _particle_group(x0):
    n = 8
    return ParticleGroup(data={
        'x': np.full(n, float(x0)), 'px': np.zeros(n), 'y': np.zeros(n), 'py': np.zeros(n),
        'z': np.zeros(n), 'pz': np.ones(n) * 1e6, 't': np.zeros(n), 'status': np.ones(n),
        'weight': np.ones(n) * 1e-15, 'species': 'electron',
    })


def _pg_array(x0s):
    arr = np.empty(len(x0s), dtype=object)
    for i, x0 in enumerate(x0s):
        arr[i] = _particle_group(x0)
    return arr


def _write_combined(tmp_path):
    D = DataPoint2()
    bd = (N,)
    D.add_observable(batch_dims=bd, location='GUNF', data=[1.0, 2.0, 3.0], data_name='GUNF:theta0_deg', units='', read_only=False)
    D.add_observable(batch_dims=bd, location='GUNF', data=[0.5], data_name='GUNF:rf_field_scale', units='V/m', read_only=False)
    D.add_observable(batch_dims=bd, location='SOL10111', data=[7.0, 8.0, 9.0], data_name='SOL10111:solenoid_field_scale', units='T', read_only=False)
    t = np.array([0.0, 1e-9, 2e-9, 3e-9])
    for name, data in (('mean_z', MEAN_Z), ('sigma_x', SIGMA_X), ('sigma_y', SIGMA_Y)):
        D.add_observable(batch_dims=bd, location=t, location_units='s', data=data, data_name=name, units='m',
                         location_group='multi_location_data_impact')
    # One cathode distribution for every shot, and one screen distribution per shot
    D.add_observable(batch_dims=bd, location='VCCF', data=_pg_array([42.0]), data_name='initial_particles', units='', read_only=False)
    D.add_observable(batch_dims=bd, location='PR10241', data=_pg_array([0.0, 1.0, 2.0]), data_name='241_particles', units='')
    D.add_lattice(lattice_location='https://example.com/lattice')
    D.add_run_information(source='Test', date='2026-10-20', notes='ML reader')
    work = tmp_path / 'work'
    work.mkdir()
    D.saveHDF5(str(work))
    path = combine_files(str(work), str(tmp_path / 'synthetic.h5'))
    assert validate_file(path).ok
    return path


def _to_legacy(path):
    """Rewrite into the earlier layout: shared z-based DATA_LOCATIONS in 'multi_location_data'
    (no mean_z) and per-shot ParticleGroups directly under observables/."""
    with h5py.File(path, 'a') as f:
        obs = f[f.attrs['IDs'][0]]['observables']
        mld = obs.create_group('multi_location_data')
        locs = mld.create_dataset('DATA_LOCATIONS', data=MEAN_Z[0])
        locs.attrs['units'] = 'm'
        for name in ('sigma_x', 'sigma_y'):
            obs.copy(obs[f'multi_location_data_impact/{name}'], mld, name=name)
        del obs['multi_location_data_impact']
        for i in range(N):
            obs.move(f'PR10241/241_particles_{i}', f'241_particles_{i}')
        del obs['PR10241']
    return path


def _convert(ml, path, tmp_path):
    out = ml.convert(str(path), str(tmp_path / 'ml'), screen=SCREEN)
    return h5py.File(out, 'r')


def test_targets_use_per_shot_mean_z(ml, tmp_path):
    with _convert(ml, _write_combined(tmp_path), tmp_path) as f:
        targets = f[f'targets_{SCREEN}/table'][()]
        np.testing.assert_array_equal(targets['sigma_x'], SIGMA_X[np.arange(N), PER_SHOT_STEP])
        # Broadcast multi-location data, sampled at each shot's own step
        np.testing.assert_array_equal(targets['sigma_y'], SIGMA_Y[0, PER_SHOT_STEP])


def test_constant_scalars_broadcast_to_every_shot(ml, tmp_path):
    with _convert(ml, _write_combined(tmp_path), tmp_path) as f:
        features = f[f'features_{SCREEN}/table'][()]
        meta = json.loads(f.attrs['meta'])
        assert meta['n_shots'] == N
        np.testing.assert_array_equal(features['GUNF:theta0_deg'], [1.0, 2.0, 3.0])
        np.testing.assert_array_equal(features['GUNF:rf_field_scale'], [0.5] * N)
        np.testing.assert_array_equal(features['SOL10111:solenoid_field_scale'], [7.0, 8.0, 9.0])


def test_single_and_per_shot_particle_groups_copied(ml, tmp_path):
    with _convert(ml, _write_combined(tmp_path), tmp_path) as f:
        initial = f[f'features_{SCREEN}/initial_particles']
        screen = f[f'targets_{SCREEN}/241_particles']
        assert sorted(initial.keys(), key=int) == [str(i) for i in range(N)]
        assert sorted(screen.keys(), key=int) == [str(i) for i in range(N)]
        for i in range(N):
            assert ParticleGroup(h5=initial[str(i)])['mean_x'] == pytest.approx(42.0)   # broadcast single PG
            assert ParticleGroup(h5=screen[str(i)])['mean_x'] == pytest.approx(float(i))  # per shot


def test_screen_auto_detected_from_location_groups(ml, tmp_path):
    out = ml.convert(str(_write_combined(tmp_path)), str(tmp_path / 'ml'))
    with h5py.File(out, 'r') as f:
        assert json.loads(f.attrs['meta'])['screen'] == SCREEN


def test_legacy_layout_still_read(ml, tmp_path):
    """Regression: files with z-based DATA_LOCATIONS and top-level per-shot ParticleGroups."""
    path = _to_legacy(_write_combined(tmp_path))
    with _convert(ml, path, tmp_path) as f:
        targets = f[f'targets_{SCREEN}/table'][()]
        # Shared z = MEAN_Z[0] -> the same step (2) for every shot
        np.testing.assert_array_equal(targets['sigma_x'], SIGMA_X[:, 2])
        screen = f[f'targets_{SCREEN}/241_particles']
        for i in range(N):
            assert ParticleGroup(h5=screen[str(i)])['mean_x'] == pytest.approx(float(i))
