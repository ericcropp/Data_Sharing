"""
Tests for named multi-location groups ('multi_location_data' and 'multi_location_data_<suffix>').
"""
import h5py
import numpy as np
import pytest
from pathlib import Path

from data_standard import DataPoint2, validate_file
from data_standard.Data_Standard_2 import Observables, SingleObservable, is_multi_location_group_name


@pytest.mark.parametrize("name, ok", [
    ("multi_location_data", True),
    ("multi_location_data_impact", True),
    ("multi_location_data_bmad_v2", True),
    ("multi_location_data_", False),
    ("multi_location_dataX", False),
    ("mld_impact", False),
    ("multi_location_data/x", False),
    (None, False),
])
def test_group_name_rule(name, ok):
    assert is_multi_location_group_name(name) is ok


def test_location_primary_false_uses_default_group():
    obs = SingleObservable(batch_dims=(2,), location=[0.0, 1.0], data=np.zeros((2, 2)), data_name='sigma_x',
                           units='m', location_units='m', location_primary=False)
    assert obs.location_group == 'multi_location_data'
    assert obs.location_primary is False


def test_location_group_implies_multi_location():
    obs = SingleObservable(batch_dims=(2,), location=[0.0, 1.0], data=np.zeros((2, 2)), data_name='sigma_x',
                           units='m', location_units='m', location_group='multi_location_data_impact')
    assert obs.location_primary is False
    assert obs.location_group == 'multi_location_data_impact'


def test_location_group_with_location_primary_true_rejected():
    with pytest.raises(ValueError, match="requires location_primary=False"):
        SingleObservable(batch_dims=(2,), location=[0.0, 1.0], data=np.zeros((2, 2)), data_name='sigma_x',
                         units='m', location_units='m', location_group='multi_location_data_impact', location_primary=True)


def test_invalid_group_name_rejected():
    with pytest.raises(ValueError, match="location_group must be"):
        SingleObservable(batch_dims=(2,), location=[0.0, 1.0], data=np.zeros((2, 2)), data_name='sigma_x',
                         units='m', location_units='m', location_group='impact_data')


@pytest.mark.parametrize("kwargs", [
    dict(location='multi_location_data_x', data_name='X'),
    dict(location='BPM1', data_name='multi_location_data_x'),
])
def test_reserved_prefix_rejected(kwargs):
    with pytest.raises(ValueError, match="reserved prefix"):
        SingleObservable(data=np.array(1.0), units='m', **kwargs)


def test_rules_apply_per_group(tmp_path):
    """Each group has its own DATA_LOCATIONS, location_units and num_feature_dims."""
    D = DataPoint2()
    D.add_observable(batch_dims=(2,), location=[0.0, 1e-9, 2e-9], data=np.zeros((2, 3)), data_name='sigma_x',
                     units='m', location_units='s', location_group='multi_location_data_impact')
    D.add_observable(batch_dims=(2,), location=[0.0, 1e-9, 2e-9], data=np.zeros((2, 3)), data_name='mean_z',
                     units='m', location_units='s', location_group='multi_location_data_impact')
    D.add_observable(batch_dims=(2,), location=[0.0, 0.5], data=np.zeros((2, 2, 4)), data_name='sigma_x',
                     units='m', location_units='m', location_group='multi_location_data_bmad', num_feature_dims=1,
                     attrs={'bin_size': 1.0, 'offset': 0.0})
    D.add_lattice(lattice_location='https://example.com/lattice')
    D.add_run_information(source='Test', date='2026-10-20', notes='groups')
    D.saveHDF5(str(tmp_path))
    path = next(Path(tmp_path).glob('*.h5'))
    with h5py.File(path, 'r') as f:
        obs = f['observables']
        assert set(obs.keys()) == {'multi_location_data_impact', 'multi_location_data_bmad'}
        assert obs['multi_location_data_impact/DATA_LOCATIONS'].attrs['units'] == 's'
        assert obs['multi_location_data_bmad/DATA_LOCATIONS'].attrs['units'] == 'm'
        assert len(obs['multi_location_data_impact/DATA_LOCATIONS']) == 3
        assert len(obs['multi_location_data_bmad/DATA_LOCATIONS']) == 2
    report = validate_file(path)
    assert report.ok and report.warnings == [], report.format()


@pytest.mark.parametrize("second, message", [
    (dict(location=[0.0, 2.0]), "same location"),
    (dict(location_units='s'), "same location_units"),
])
def test_inconsistency_within_group_rejected(second, message):
    obs = Observables()
    base = dict(batch_dims=(2,), location=[0.0, 1.0], data=np.zeros((2, 2)), units='m', location_units='m',
                location_group='multi_location_data_impact')
    obs.add_observable(data_name='sigma_x', **base)
    kw = dict(base, data_name='sigma_y')
    kw.update(second)
    obs.add_observable(**kw)
    with pytest.raises(ValueError, match=message):
        obs.observable_checker()


def test_same_data_name_in_different_groups_allowed():
    obs = Observables()
    for group in ('multi_location_data_impact', 'multi_location_data_bmad'):
        obs.add_observable(batch_dims=(2,), location=[0.0, 1.0], data=np.zeros((2, 2)), data_name='sigma_x',
                           units='m', location_units='m', location_group=group)
    obs.observable_checker()


def test_validator_rejects_misnamed_group(tmp_path):
    D = DataPoint2()
    D.add_observable(batch_dims=(2,), location=[0.0, 1.0], data=np.zeros((2, 2)), data_name='sigma_x',
                     units='m', location_units='m', location_group='multi_location_data_impact')
    D.add_lattice(lattice_location='https://example.com/lattice')
    D.add_run_information(source='Test', date='2026-10-20', notes='groups')
    D.saveHDF5(str(tmp_path))
    path = next(Path(tmp_path).glob('*.h5'))
    with h5py.File(path, 'a') as f:
        f.move('observables/multi_location_data_impact', 'observables/multi_location_dataImpact')
    report = validate_file(path)
    assert any("reserved for multi-location groups" in e for e in report.errors), report.format()


def test_validator_checks_each_group_data_locations(tmp_path):
    D = DataPoint2()
    for group in ('multi_location_data_impact', 'multi_location_data_bmad'):
        D.add_observable(batch_dims=(2,), location=[0.0, 1.0], data=np.zeros((2, 2)), data_name='sigma_x',
                         units='m', location_units='m', location_group=group)
    D.add_lattice(lattice_location='https://example.com/lattice')
    D.add_run_information(source='Test', date='2026-10-20', notes='groups')
    D.saveHDF5(str(tmp_path))
    path = next(Path(tmp_path).glob('*.h5'))
    with h5py.File(path, 'a') as f:
        del f['observables/multi_location_data_bmad/DATA_LOCATIONS']
    report = validate_file(path)
    assert any("multi_location_data_bmad" in e and "missing DATA_LOCATIONS" in e for e in report.errors), report.format()
    assert not any("multi_location_data_impact" in e for e in report.errors)


def _two_groups_file(tmp_path):
    D = DataPoint2()
    D.add_observable(batch_dims=(2,), location=[0.0, 1.0, 2.0], data=np.zeros((2, 3)), data_name='sigma_x',
                     units='m', location_units='s', location_group='multi_location_data_impact')
    D.add_observable(batch_dims=(2,), location=[0.0, 1.0, 2.0], data=np.zeros((2, 3)), data_name='sigma_y',
                     units='m', location_units='s', location_group='multi_location_data_impact')
    D.add_observable(batch_dims=(2,), location=[0.0, 0.5], data=np.zeros((2, 2)), data_name='sigma_x',
                     units='m', location_units='m', location_group='multi_location_data_bmad')
    D.add_lattice(lattice_location='https://example.com/lattice')
    D.add_run_information(source='Test', date='2026-10-20', notes='groups')
    D.saveHDF5(str(tmp_path))
    return next(Path(tmp_path).glob('*.h5'))


def test_validator_checks_location_dimension_against_own_group(tmp_path):
    path = _two_groups_file(tmp_path)
    with h5py.File(path, 'a') as f:
        # Valid for the impact group (3 locations), but moved into the bmad group (2 locations)
        f.copy(f['observables/multi_location_data_impact/sigma_y'], f['observables/multi_location_data_bmad'], name='sigma_y')
    report = validate_file(path)
    assert any("multi_location_data_bmad/sigma_y" in e and "location dimension" in e for e in report.errors), report.format()
    assert not any("multi_location_data_impact" in e for e in report.errors)


def test_validator_checks_num_feature_dims_per_group(tmp_path):
    path = _two_groups_file(tmp_path)
    with h5py.File(path, 'a') as f:
        grp = f['observables/multi_location_data_impact']
        ds = grp.create_dataset('profile', data=np.zeros((2, 3, 4)))
        ds.attrs.update({'read_only': True, 'num_feature_dims': 1, 'units': 'm', 'bin_size': 1.0, 'offset': 0.0})
    report = validate_file(path)
    assert any("multi_location_data_impact" in e and "same num_feature_dims" in e for e in report.errors), report.format()


def test_validator_rejects_dataset_with_reserved_prefix(tmp_path):
    path = _two_groups_file(tmp_path)
    with h5py.File(path, 'a') as f:
        f.create_dataset('observables/multi_location_data_extra', data=np.zeros(2))
    report = validate_file(path)
    assert any("multi-location entries must be groups" in e for e in report.errors), report.format()
