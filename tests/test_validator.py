"""
Tests for standalone validation: validate_datapoint (in-memory) and validate_file (HDF5).
"""
import shutil
from pathlib import Path

import h5py
import numpy as np
import pytest

from data_standard import (
    DataPoint2, SimulatedDataPoint2, combine_files, validate_datapoint, validate_file,
    DataStandardValidationError,
)
from data_standard.Data_Standard_2 import VERSION
from data_standard.validate import main as validate_main


def _datapoint(batch_dims=(3,)):
    n = batch_dims[0] if batch_dims else None
    shape = batch_dims
    D = DataPoint2()
    D.add_observable(batch_dims=batch_dims, location='BPM10781', data=np.arange(n or 1, dtype=float).reshape(shape),
                     data_name='BPMS:IN10:781:TMIT', units='')
    D.add_observable(batch_dims=batch_dims, location='BPM10781', data=np.ones(shape), data_name='BPMS:IN10:781:X', units='mm')
    D.add_observable(batch_dims=batch_dims, location='QA10361', data=np.ones(shape), data_name='QUAD:IN10:361:BCTRL',
                     units='kG', read_only=False)
    D.add_observable(batch_dims=batch_dims, location='PR10571', data=np.full(shape, 1e-6), data_name='PROF:IN10:571:RESOLUTION', units='m')
    D.add_observable(batch_dims=batch_dims, num_feature_dims=2, location='PR10571', data=np.zeros(shape + (4, 5)),
                     data_name='PROF:IN10:571:Image', units='counts',
                     attrs={'bin_size': 'PROF:IN10:571:RESOLUTION', 'offset': 0.0})
    D.add_observable(batch_dims=batch_dims, location=[0.0, 1.0, 2.0], data=np.zeros(shape + (3,)), data_name='sigma_x',
                     units='um', location_units='m', location_primary=False)
    D.add_timestamp(["2026-10-16T13:55:40.050-07:00"] * 3)
    D.add_lattice(lattice_location='https://example.com/lattice', PV_table={'BPMS:IN10:781': 'BPM10781'})
    D.add_run_information(source='Test', date='2026-10-16', notes='validator')
    return D


@pytest.fixture
def valid_file(tmp_path):
    D = _datapoint()
    D.saveHDF5(str(tmp_path))
    return next(tmp_path.glob('*.h5'))


# ---------------------------------------------------------------- in-memory

def test_validate_datapoint_valid():
    report = validate_datapoint(_datapoint())
    assert report.ok and report.errors == []


def test_validate_datapoint_does_not_need_finalize():
    D = _datapoint()
    validate_datapoint(D)
    assert D.ID == ""


def test_validate_datapoint_collects_all_errors():
    D = DataPoint2()
    D.add_observable(batch_dims=(3,), num_feature_dims=2, location='PR10571', data=np.zeros((3, 4, 5)),
                     data_name='img', units='counts', attrs={'bin_size': 'NoSuchKey', 'offset': 0.0})
    report = validate_datapoint(D, raise_on_error=False)
    sections = {e.split(':')[0] for e in report.errors}
    assert {'lattice', 'observables', 'run_information'} <= sections
    with pytest.raises(DataStandardValidationError):
        validate_datapoint(D)


def test_validate_datapoint_simulation_metadata():
    D = SimulatedDataPoint2()
    D.add_observable(location='VCCF', data=np.array(1.0), data_name='GUNF:theta0_deg', units='', read_only=False)
    D.add_lattice(lattice_location='https://example.com/lattice')
    D.add_run_information(source='Test', date='2026-10-16', notes='sim')
    D.add_simulation_data(simulation_start=0.0, simulation_end=1.0, simulation_code='Impact',
                          simulation_input_file='input', simulation_version='1.0')
    assert validate_datapoint(D).ok
    D.simulation_metadata.simulation_code = ""
    report = validate_datapoint(D, raise_on_error=False)
    assert any(e.startswith('simulation_metadata') for e in report.errors)


def test_single_observable_checker_standalone():
    D = _datapoint()
    obs = D.observables[0]
    obs.checker()
    obs.data = np.zeros((2,))
    with pytest.raises(ValueError, match="batch dimension"):
        obs.checker()


def test_validate_datapoint_rejects_other_types():
    with pytest.raises(TypeError):
        validate_datapoint({'not': 'a datapoint'})


# ---------------------------------------------------------------- files

def test_valid_single_file(valid_file):
    report = validate_file(valid_file)
    assert report.ok, report.format()
    assert report.warnings == []


def test_valid_combined_file(tmp_path):
    work = tmp_path / 'work'
    work.mkdir()
    for notes in ('a', 'b'):
        D = _datapoint()
        D.add_run_information(source='Test', date='2026-10-16', notes=notes)
        D.observables[0].data = D.observables[0].data + (1 if notes == 'b' else 0)
        D.saveHDF5(str(work))
    out = combine_files(str(work), str(tmp_path / 'combined.h5'), validate=True)
    assert Path(out).name == f'combined_v{VERSION}.h5'
    report = validate_file(out)
    assert report.ok and report.warnings == [], report.format()


def test_combine_files_validation_failure_keeps_inputs(tmp_path, valid_file):
    work = tmp_path / 'work'
    work.mkdir()
    target = work / valid_file.name
    shutil.copy(valid_file, target)
    with h5py.File(target, 'a') as f:
        del f['observables/QUAD:IN10:361:BCTRL'].attrs['read_only']
    with pytest.raises(DataStandardValidationError, match="read_only"):
        combine_files(str(work), str(tmp_path / 'combined.h5'), validate=True)
    assert target.exists()


def _mutate(path, fn):
    with h5py.File(path, 'a') as f:
        fn(f)
    return validate_file(path)


@pytest.mark.parametrize("mutation, message", [
    (lambda f: f['observables/QUAD:IN10:361:BCTRL'].attrs.__delitem__('read_only'), "missing @read_only"),
    (lambda f: f['observables/QUAD:IN10:361:BCTRL'].attrs.__setitem__('read_only', 1), "@read_only must be a boolean"),
    (lambda f: f['observables/QUAD:IN10:361:BCTRL'].attrs.__delitem__('location'), "missing @location"),
    (lambda f: f['observables/BPM10781/BPMS:IN10:781:X'].attrs.__setitem__('location', 'BPM1'), "does not match parent group"),
    (lambda f: f['observables/BPM10781/BPMS:IN10:781:X'].attrs.__setitem__('control', True), "inconsistent"),
    (lambda f: f['observables/PR10571/PROF:IN10:571:Image'].attrs.__setitem__('bin_size', 'Missing'), "does not match any observable"),
    (lambda f: f['observables/PR10571/PROF:IN10:571:Image'].attrs.__delitem__('offset'), "missing @offset"),
    (lambda f: f['observables/PR10571/PROF:IN10:571:Image'].attrs.__setitem__('num_feature_dims', 1), "expected 2 dimensions"),
    (lambda f: f['observables/QUAD:IN10:361:BCTRL'].attrs.__setitem__('unit_multiplier', -1.0), "@unit_multiplier must be a positive"),
    (lambda f: f['observables/Timestamp'].__setitem__(0, '2026-10-16T13:55:40'), "not ISO 8601"),
    (lambda f: f['observables/multi_location_data/DATA_LOCATIONS'].attrs.__delitem__('units'), "location units"),
    (lambda f: f.attrs.__setitem__('run_information_date', '16/10/2026'), "YYYY-MM-DD"),
    (lambda f: f.attrs.__setitem__('batch_dims', np.array([0])), "positive integers"),
    (lambda f: f.attrs.__delitem__('ID'), "missing @ID"),
    (lambda f: f['lattice'].attrs.__setitem__('lattice_location', 'included'), "lattice_files/ is missing"),
    (lambda f: f.create_dataset('observables/BPM10781/strings', data=np.array([b'a', b'b', b'c'])), "must be numeric"),
])
def test_invalid_files_reported(valid_file, mutation, message):
    report = _mutate(valid_file, mutation)
    assert not report.ok
    assert any(message in e for e in report.errors), report.format()


def test_legacy_layout_passes_with_warnings_and_fails_strict(tmp_path):
    """A file in the pre-0.1.2 style (@control, @unit_multiplier, base units, nested single observables)."""
    path = tmp_path / 'legacy.h5'
    with h5py.File(path, 'w') as f:
        f.attrs.update({'ID': 'abc', 'batch_dims': np.array([]), 'Data_Standard_Version': '0.1.1',
                        'run_information_source': 'Test', 'run_information_date': '2026-01-01',
                        'run_information_notes': 'legacy'})
        f.create_group('lattice').attrs['lattice_location'] = 'https://example.com/lattice'
        ds = f.create_dataset('observables/ICT1/charge', data=250.0)
        ds.attrs.update({'location': 'ICT1', 'control': False, 'num_feature_dims': 0, 'units': 'C', 'unit_multiplier': 1e-12})
    report = validate_file(path)
    assert report.ok, report.format()
    joined = "\n".join(report.warnings)
    for expected in ("legacy @control", "legacy @unit_multiplier", "single observable", "Data_Standard_Version 0.1.1"):
        assert expected in joined
    assert not validate_file(path, strict=True).ok


def test_lattice_files_with_directories_accepted(tmp_path, monkeypatch):
    """Lattice files given as relative paths are stored as nested groups and remain valid."""
    (tmp_path / 'lattices').mkdir()
    (tmp_path / 'lattices' / 'beamline.lte').write_text('Q1: QUAD, L=0.1')
    monkeypatch.chdir(tmp_path)
    D = _datapoint()
    D.add_lattice(lattice_location='included', lattice_files=['lattices/beamline.lte'])
    D.saveHDF5(str(tmp_path))
    report = validate_file(next(tmp_path.glob('*.h5')))
    assert report.ok, report.format()


def test_combined_file_id_mismatch(tmp_path):
    work = tmp_path / 'work'
    work.mkdir()
    _datapoint().saveHDF5(str(work))
    out = combine_files(str(work), str(tmp_path / 'combined.h5'))
    report = _mutate(out, lambda f: f.attrs.__setitem__('IDs', ['not-there']))
    assert any("has no group" in e for e in report.errors)
    assert any("not listed in root @IDs" in e for e in report.errors)


def test_not_a_data_standard_file(tmp_path):
    path = tmp_path / 'other.h5'
    with h5py.File(path, 'w') as f:
        f.create_dataset('x', data=1)
    report = validate_file(path)
    assert not report.ok and "not a Data Standard file" in report.errors[0]
    with pytest.raises(DataStandardValidationError):
        validate_file(path, raise_on_error=True)


def test_unreadable_file(tmp_path):
    path = tmp_path / 'broken.h5'
    path.write_text('not hdf5')
    assert not validate_file(path).ok


def test_cli_exit_codes(valid_file, tmp_path, capsys):
    assert validate_main([str(valid_file)]) == 0
    assert "PASS" in capsys.readouterr().out
    bad = tmp_path / 'bad.h5'
    bad.write_text('nope')
    assert validate_main([str(valid_file), str(bad)]) == 1
    out = capsys.readouterr().out
    assert "FAIL" in out and "PASS" in out
