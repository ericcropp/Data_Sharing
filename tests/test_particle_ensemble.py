"""
Tests for ParticleGroup ensemble storage format.

Verifies that ParticleGroup observables are written using write_particle_ensemble
and can be read back with read_particle_ensemble, producing N-D datasets
(*scan_shape, n_particle) with numDistributions/ensembleShape attributes.
"""

import numpy as np
import h5py
import pytest

from pmd_beamphysics import ParticleGroup, write_particle_ensemble, read_particle_ensemble
from data_standard.Data_Standard_2 import DataPoint2


def _make_particle_group(n_particle=10, seed=42):
    """Create a simple test ParticleGroup with electron species."""
    rng = np.random.default_rng(seed)
    data = {
        "x": rng.normal(0, 1e-3, n_particle),
        "px": rng.normal(0, 1e-6, n_particle),
        "y": rng.normal(0, 1e-3, n_particle),
        "py": rng.normal(0, 1e-6, n_particle),
        "z": rng.normal(0, 1e-4, n_particle),
        "pz": rng.normal(1e9, 1e6, n_particle),
        "t": rng.normal(0, 1e-12, n_particle),
        "weight": np.ones(n_particle),
        "status": np.ones(n_particle, dtype=int),
        "species": "electron",
    }
    return ParticleGroup(data=data)


def _make_pg_array(shape, n_particle=10):
    """Create a properly shaped object array of ParticleGroups.

    Parameters
    ----------
    shape : int or tuple of int
        Batch shape. An int is treated as (n,).
    """
    if isinstance(shape, int):
        shape = (shape,)
    n_total = int(np.prod(shape))
    pg_list = [_make_particle_group(n_particle, seed=i) for i in range(n_total)]
    arr = np.empty(n_total, dtype=object)
    arr[:] = pg_list
    return arr.reshape(shape)


def _make_datapoint_with_pg(batch_dims, n_particle=10, location="screen1"):
    """Create a DataPoint2 with a batched ParticleGroup observable."""
    if isinstance(batch_dims, int):
        batch_dims = (batch_dims,)
    pgs = _make_pg_array(batch_dims, n_particle)
    dp = DataPoint2(
        lattice_location="external",
        lattice_files={},
        run_information={"source": "test", "date": "2024-01-01", "notes": "unit test"},
    )
    dp.add_observable(
        batch_dims=batch_dims,
        data=pgs,
        data_name="beam",
        location=[location],
        location_primary=True,
        units="Custom Unit",
        control=False,
    )
    return dp


class TestParticleEnsembleStorage:
    """Tests for ensemble-based ParticleGroup HDF5 storage."""

    def test_batched_particlegroup_ensemble_format(self, tmp_path):
        """1-D ParticleGroup batch stored with correct attributes."""
        n_batch = 5
        n_particle = 10
        dp = _make_datapoint_with_pg(n_batch, n_particle)

        out_file = str(tmp_path / "test_ensemble.h5")
        dp.saveHDF5(out_file)

        with h5py.File(out_file, "r") as f:
            loc_grp = f["observables/screen1"]
            assert "ParticleGroup" in loc_grp
            assert "beam" not in loc_grp
            pg_grp = loc_grp["ParticleGroup"]
            assert pg_grp.attrs["control"] == False
            assert pg_grp.attrs["location"] == "screen1"
            assert pg_grp.attrs["num_feature_dims"] == n_particle
            assert "electron" in pg_grp
            species_grp = pg_grp["electron"]
            assert species_grp.attrs["numDistributions"] == n_batch
            assert tuple(species_grp.attrs["ensembleShape"]) == (n_batch,)
            assert species_grp["position/x"].shape == (n_batch, n_particle)
            assert species_grp["momentum/y"].shape == (n_batch, n_particle)

    def test_single_particlegroup_ensemble_format(self, tmp_path):
        """Single ParticleGroup uses ensemble with numDistributions=1."""
        dp = _make_datapoint_with_pg(1, n_particle=8)

        out_file = str(tmp_path / "test_single_pg.h5")
        dp.saveHDF5(out_file)

        with h5py.File(out_file, "r") as f:
            species_grp = f["observables/screen1/ParticleGroup/electron"]
            assert species_grp.attrs["numDistributions"] == 1
            assert species_grp["position/x"].shape == (1, 8)

    def test_nd_batch_particlegroup_format(self, tmp_path):
        """N-D batch_dims stored with correct ensembleShape and dataset shape."""
        batch_dims = (3, 2)
        n_particle = 7
        dp = _make_datapoint_with_pg(batch_dims, n_particle)

        out_file = str(tmp_path / "test_nd.h5")
        dp.saveHDF5(out_file)

        with h5py.File(out_file, "r") as f:
            species_grp = f["observables/screen1/ParticleGroup/electron"]
            assert species_grp.attrs["numDistributions"] == 6
            assert tuple(species_grp.attrs["ensembleShape"]) == (3, 2)
            # datasets have shape (*batch_dims, n_particle)
            assert species_grp["position/x"].shape == (3, 2, n_particle)

    def test_3d_batch_particlegroup_format(self, tmp_path):
        """3-D batch_dims produce correct ensemble metadata."""
        batch_dims = (2, 3, 4)
        n_particle = 5
        dp = _make_datapoint_with_pg(batch_dims, n_particle)

        out_file = str(tmp_path / "test_3d.h5")
        dp.saveHDF5(out_file)

        with h5py.File(out_file, "r") as f:
            species_grp = f["observables/screen1/ParticleGroup/electron"]
            assert species_grp.attrs["numDistributions"] == 24
            assert tuple(species_grp.attrs["ensembleShape"]) == (2, 3, 4)
            assert species_grp["position/x"].shape == (2, 3, 4, n_particle)

    def test_ensemble_round_trip_1d(self, tmp_path):
        """1-D write + read_particle_ensemble preserves particle data."""
        n_batch = 4
        n_particle = 15
        pgs = [_make_particle_group(n_particle, seed=i) for i in range(n_batch)]

        out_file = str(tmp_path / "round_trip_1d.h5")
        write_particle_ensemble(out_file, pgs)
        restored = read_particle_ensemble(out_file)

        assert restored.shape == (n_batch,)
        for orig, rest in zip(pgs, restored.flat):
            np.testing.assert_allclose(rest.x, orig.x, rtol=1e-12)
            np.testing.assert_allclose(rest.px, orig.px, rtol=1e-12)
            np.testing.assert_allclose(rest.pz, orig.pz, rtol=1e-12)

    def test_ensemble_round_trip_nd(self, tmp_path):
        """N-D write + read_particle_ensemble preserves shape and data."""
        batch_dims = (3, 2)
        n_particle = 10
        pgs = _make_pg_array(batch_dims, n_particle)

        out_file = str(tmp_path / "round_trip_nd.h5")
        write_particle_ensemble(out_file, pgs)
        restored = read_particle_ensemble(out_file)

        assert restored.shape == batch_dims
        for idx in np.ndindex(batch_dims):
            np.testing.assert_allclose(restored[idx].x, pgs[idx].x, rtol=1e-12)
            np.testing.assert_allclose(restored[idx].pz, pgs[idx].pz, rtol=1e-12)

    def test_no_old_index_groups(self, tmp_path):
        """Old-style per-index groups (beam_0, beam_1, ...) must not exist."""
        dp = _make_datapoint_with_pg(3, n_particle=5)

        out_file = str(tmp_path / "test_no_old_idx.h5")
        dp.saveHDF5(out_file)

        with h5py.File(out_file, "r") as f:
            loc_grp = f["observables/screen1"]
            assert "ParticleGroup" in loc_grp, "Expected fixed 'ParticleGroup' group"
            assert "beam" not in loc_grp, "data_name 'beam' should not be a direct child"
            for key in loc_grp.keys():
                assert not key.startswith("beam_"), f"Found old-style index group '{key}'"

    def test_particlegroup_has_observable_attrs(self, tmp_path):
        """ParticleGroup group carries @control, @location, @num_feature_dims attrs."""
        n_particle = 12
        dp = _make_datapoint_with_pg(3, n_particle, location="injector")

        out_file = str(tmp_path / "test_pg_attrs.h5")
        dp.saveHDF5(out_file)

        with h5py.File(out_file, "r") as f:
            pg_grp = f["observables/injector/ParticleGroup"]
            assert pg_grp.attrs["control"] == False
            assert pg_grp.attrs["location"] == "injector"
            assert pg_grp.attrs["num_feature_dims"] == n_particle

