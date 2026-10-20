"""
Tests for the read_only flag and its backward-compatible inverse, control.
"""
import tempfile
from pathlib import Path

import h5py
import numpy as np
import pytest

from data_standard import DataPoint2
from data_standard.Data_Standard_2 import SingleObservable, resolve_read_only


def _obs(**kwargs):
    kw = dict(batch_dims=(), location=['ICT1'], data=np.array(1.0), data_name='charge', units='pC')
    kw.update(kwargs)
    return SingleObservable(**kw)


def test_default_is_read_only():
    obs = _obs()
    assert obs.read_only is True
    assert obs.control is False


@pytest.mark.parametrize("read_only", [True, False])
def test_read_only_sets_inverse_control(read_only):
    obs = _obs(read_only=read_only)
    assert obs.read_only is read_only
    assert obs.control is (not read_only)


@pytest.mark.parametrize("control", [True, False])
def test_legacy_control_is_inverted(control):
    obs = _obs(control=control)
    assert obs.read_only is (not control)
    assert obs.control is control


def test_consistent_read_only_and_control_accepted():
    obs = _obs(read_only=False, control=True)
    assert obs.read_only is False


def test_inconsistent_read_only_and_control_rejected():
    with pytest.raises(ValueError, match="inconsistent"):
        _obs(read_only=True, control=True)


@pytest.mark.parametrize("kwargs", [dict(read_only=1), dict(control="yes"), dict(read_only=[True])])
def test_non_boolean_rejected(kwargs):
    with pytest.raises(TypeError, match="must be a single boolean"):
        _obs(**kwargs)


def test_resolve_read_only_accepts_numpy_bool():
    assert resolve_read_only(read_only=np.bool_(False)) is False
    assert resolve_read_only(control=np.bool_(False)) is True


def test_to_dict_uses_control_semantics():
    assert _obs(read_only=False).to_dict()["name"] == "charge"
    assert _obs(read_only=True).to_dict() == {}


def test_hdf5_stores_only_read_only():
    with tempfile.TemporaryDirectory() as tmpdir:
        D = DataPoint2()
        D.add_observable(location='QA10361', data=np.array(1.0), data_name='QUAD:IN10:361:BCTRL', units='kG', read_only=False)
        D.add_observable(location='BPM10781', data=np.array(2.0), data_name='BPMS:IN10:781:X', units='mm', control=False)
        D.add_lattice(lattice_location='https://example.com/lattice')
        D.add_run_information(source='Test', date='2026-10-16', notes='read_only')
        D.saveHDF5(tmpdir)
        with h5py.File(next(Path(tmpdir).glob('*.h5')), 'r') as f:
            bctrl = f['observables/QUAD:IN10:361:BCTRL']
            x = f['observables/BPMS:IN10:781:X']
            assert bool(bctrl.attrs['read_only']) is False
            assert bool(x.attrs['read_only']) is True
            assert 'control' not in bctrl.attrs and 'control' not in x.attrs
