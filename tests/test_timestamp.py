"""
Tests for the optional per-shot 'Timestamp' observable (ISO 8601 date-times with UTC offset).
"""
import datetime
import tempfile
from pathlib import Path

import h5py
import numpy as np
import pytest

from data_standard import DataPoint2, validate_file
from data_standard.Data_Standard_2 import SingleObservable, is_iso8601_timestamp, to_iso8601_string


@pytest.mark.parametrize("value", [
    "2026-10-16T13:55:40-07:00",
    "2026-10-16T13:55:40.050-07:00",
    "2026-10-16T20:55:40.123456Z",
    "2026-10-16T13:55:40,5+05:30",
    "2016-12-31T23:59:60Z",
])
def test_valid_iso8601(value):
    assert is_iso8601_timestamp(value)
    assert is_iso8601_timestamp(value.encode())


@pytest.mark.parametrize("value", [
    "2026-10-16",                   # date only
    "2026-10-16T13:55:40",          # no UTC offset
    "2026-10-16 13:55:40Z",         # space instead of T
    "20261016T135540Z",             # basic format
    "2026-13-01T00:00:00Z",         # invalid month
    "2026-02-30T00:00:00Z",         # invalid day
    "2026-10-16T24:00:00Z",         # invalid hour
    "2026-10-16T13:55:40+25:00",    # invalid offset
    "",
    1760648140.0,
])
def test_invalid_iso8601(value):
    assert not is_iso8601_timestamp(value)


def test_to_iso8601_string_converts_aware_datetime():
    dt = datetime.datetime(2026, 10, 16, 13, 55, 40, 50000, tzinfo=datetime.timezone(datetime.timedelta(hours=-7)))
    assert to_iso8601_string(dt) == "2026-10-16T13:55:40.050000-07:00"


def test_to_iso8601_string_rejects_naive_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        to_iso8601_string(datetime.datetime(2026, 10, 16))


def _datapoint_with_timestamps(timestamps, **kwargs):
    D = DataPoint2()
    D.add_observable(batch_dims=(3,), location='BPM10781', data=np.arange(3.0), data_name='BPMS:IN10:781:TMIT', units='')
    D.add_timestamp(timestamps, **kwargs)
    D.add_lattice(lattice_location='https://example.com/lattice')
    D.add_run_information(source='Test', date='2026-10-16', notes='timestamps')
    return D


def test_add_timestamp_round_trip():
    stamps = [
        "2026-10-16T13:55:40.050-07:00",
        datetime.datetime(2026, 10, 16, 20, 55, 41, tzinfo=datetime.timezone.utc),
        "2026-10-16T20:55:42Z",
    ]
    D = _datapoint_with_timestamps(stamps)
    ts = [o for o in D.observables if o.data_name == 'Timestamp'][0]
    assert ts.batch_dims == (3,)
    assert ts.read_only is True
    with tempfile.TemporaryDirectory() as tmpdir:
        D.saveHDF5(tmpdir)
        path = next(Path(tmpdir).glob('*.h5'))
        with h5py.File(path, 'r') as f:
            ds = f['observables/Timestamp']
            assert list(ds.asstr()[()]) == [
                "2026-10-16T13:55:40.050-07:00", "2026-10-16T20:55:41+00:00", "2026-10-16T20:55:42Z"]
            assert 'location' not in ds.attrs
            assert bool(ds.attrs['read_only']) is True
            assert ds.attrs['num_feature_dims'] == 0
        assert validate_file(path).ok


def test_timestamp_with_location_is_stored_with_location():
    D = _datapoint_with_timestamps(["2026-10-16T20:55:40Z"] * 3, location='EVR')
    with tempfile.TemporaryDirectory() as tmpdir:
        D.saveHDF5(tmpdir)
        with h5py.File(next(Path(tmpdir).glob('*.h5')), 'r') as f:
            assert f['observables/Timestamp'].attrs['location'] == 'EVR'


def test_single_shot_timestamp():
    D = DataPoint2()
    D.add_observable(location='ICT1', data=np.array(250.0), data_name='charge', units='pC')
    D.add_timestamp("2026-10-16T13:55:40-07:00")
    assert [o for o in D.observables if o.is_timestamp][0].data.shape == ()


def test_timestamp_shape_must_match_batch_dims():
    with pytest.raises(ValueError, match="batch dimension|dimensions"):
        _datapoint_with_timestamps(["2026-10-16T20:55:40Z"] * 2)


def test_timestamp_without_offset_rejected():
    with pytest.raises(ValueError, match="ISO 8601"):
        _datapoint_with_timestamps(["2026-10-16T20:55:40"] * 3)


def test_timestamp_must_be_read_only():
    with pytest.raises(ValueError, match="read_only"):
        SingleObservable(data=np.array("2026-10-16T20:55:40Z"), data_name='Timestamp', read_only=False)


def test_only_one_timestamp_allowed():
    D = _datapoint_with_timestamps(["2026-10-16T20:55:40Z"] * 3)
    with pytest.raises(ValueError, match="At most one"):
        D.add_timestamp(["2026-10-16T20:55:40Z"] * 3, location='EVR')


def test_timestamps_change_id():
    a = _datapoint_with_timestamps(["2026-10-16T20:55:40Z"] * 3).make_ID().ID
    b = _datapoint_with_timestamps(["2026-10-16T20:55:41Z"] * 3).make_ID().ID
    assert a != b
