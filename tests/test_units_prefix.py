"""
Tests for unit handling: units are stored with their SI prefix (e.g. 'mW'), and the
prefix multiplier is only used internally to validate the unit.
"""
import tempfile
from pathlib import Path

import h5py
import numpy as np
import pytest

from data_standard import DataPoint2
from data_standard.Data_Standard_2 import SingleObservable, unit_checker


@pytest.mark.parametrize("unit, multiplier, base", [
    ("W", 1.0, "W"),
    ("mW", 1e-3, "W"),
    ("uW", 1e-6, "W"),
    ("µW", 1e-6, "W"),
    ("pC", 1e-12, "C"),
    ("MeV", 1e6, "eV"),
    ("mm", 1e-3, "m"),
    ("dam", 10.0, "m"),
    ("kg", 1.0, "kg"),
])
def test_unit_checker_prefixes(unit, multiplier, base):
    prefix, valid = unit_checker(unit)
    assert prefix == pytest.approx(multiplier)
    assert valid != "Custom Unit"
    assert getattr(valid, "unitSymbol", str(valid)) == base


@pytest.mark.parametrize("unit", ["counts", "unitless", "arb", "kGm"])
def test_unit_checker_custom_units(unit):
    assert unit_checker(unit) == (1.0, "Custom Unit")


def test_unit_checker_rejects_non_string():
    with pytest.raises(ValueError, match="unit must be a string"):
        unit_checker(None)


def test_observable_keeps_prefixed_units():
    obs = SingleObservable(location=['LASER'], data=np.array(3.0), data_name='LASR:LT10:930:PWR', units='mW')
    assert obs.units == 'mW'
    assert obs.units_str == 'mW'
    assert obs.unit_multiplier == pytest.approx(1e-3)
    assert getattr(obs.base_units, "unitSymbol", None) == 'W'


@pytest.mark.parametrize("unit", ["uW", "mW", "pC", "counts"])
def test_hdf5_units_stored_as_given_without_multiplier(unit):
    with tempfile.TemporaryDirectory() as tmpdir:
        D = DataPoint2()
        D.add_observable(location='LASER', data=np.array(3.0), data_name='PWR', units=unit)
        D.add_lattice(lattice_location='https://example.com/lattice')
        D.add_run_information(source='Test', date='2026-10-16', notes='units')
        D.saveHDF5(tmpdir)
        with h5py.File(next(Path(tmpdir).glob('*.h5')), 'r') as f:
            ds = f['observables/PWR']
            assert ds.attrs['units'] == unit
            assert 'unit_multiplier' not in ds.attrs
