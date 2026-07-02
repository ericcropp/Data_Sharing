"""
Tests for FACET-II_to_ML_Example.py

These tests verify that the Data Standard -> ML-ready conversion:
- Runs successfully against test data
- Produces output with correct HDF5 structure
- Features and targets tables have expected compound dtypes
- ParticleGroups are correctly copied and sliced
- Metadata is present and valid
"""
import pytest
import os
import sys
import subprocess
import tempfile
import shutil
import json
import h5py
import numpy as np
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent
EXAMPLES_DIR = PROJECT_ROOT / "examples"
# The ML script reads the combined Data Standard file
DS_OUTPUT_DIR = PROJECT_ROOT / "examples" / "data" / "output" / "FACET-II_Simulation_Example"
DS_FILE = DS_OUTPUT_DIR / "FACET-II_Simulation_Example_v0.1.1.h5"


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test outputs."""
    temp_dir = tempfile.mkdtemp(prefix="test_facet_ii_ml_")
    yield temp_dir
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def check_ds_file_available():
    """Check if the Data Standard output file is available."""
    if not DS_FILE.exists():
        pytest.skip(
            f"Data Standard file not found at {DS_FILE}. "
            "Run FACET-II_Simulation_Example.py first."
        )


@pytest.mark.slow
def test_to_ml_script_runs(temp_output_dir, check_ds_file_available):
    """Test that the ML conversion script runs without errors."""
    script_path = EXAMPLES_DIR / "FACET-II_to_ML_Example.py"

    cmd = [
        sys.executable,
        str(script_path),
        "--input", str(DS_FILE),
        "--output-dir", temp_output_dir,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, f"Script failed:\n{result.stderr}"

    # Check output file was created
    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    assert len(h5_files) == 1, f"Expected 1 output file, got {len(h5_files)}"


@pytest.mark.slow
def test_output_structure(temp_output_dir, check_ds_file_available):
    """Test that the ML output has features/targets groups with tables."""
    script_path = EXAMPLES_DIR / "FACET-II_to_ML_Example.py"
    cmd = [
        sys.executable, str(script_path),
        "--input", str(DS_FILE),
        "--output-dir", temp_output_dir,
    ]
    subprocess.run(cmd, capture_output=True, timeout=120)

    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    assert len(h5_files) == 1

    with h5py.File(h5_files[0], "r") as f:
        # Check metadata attribute
        assert "meta" in f.attrs, "Missing 'meta' attribute"
        meta = json.loads(f.attrs["meta"])
        assert "screen" in meta
        assert "n_shots" in meta
        assert meta["n_shots"] > 0

        screen = meta["screen"]

        # Check features group
        feat_key = "features_%s" % screen
        assert feat_key in f, "Missing features group"
        fg = f[feat_key]
        assert "table" in fg, "Missing features table"
        assert "initial_particles" in fg, "Missing initial_particles group"

        # Check targets group
        tgt_key = "targets_%s" % screen
        assert tgt_key in f, "Missing targets group"
        tg = f[tgt_key]
        assert "table" in tg, "Missing targets table"


@pytest.mark.slow
def test_features_table_dtype(temp_output_dir, check_ds_file_available):
    """Test that the features table has the expected compound dtype structure."""
    script_path = EXAMPLES_DIR / "FACET-II_to_ML_Example.py"
    cmd = [
        sys.executable, str(script_path),
        "--input", str(DS_FILE),
        "--output-dir", temp_output_dir,
    ]
    subprocess.run(cmd, capture_output=True, timeout=120)

    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    with h5py.File(h5_files[0], "r") as f:
        meta = json.loads(f.attrs["meta"])
        screen = meta["screen"]
        tbl = f["features_%s/table" % screen]

        # Should be compound dtype
        assert tbl.dtype.names is not None, "Features table should have compound dtype"

        # Must include known feature columns
        names = tbl.dtype.names
        assert "GUNF:theta0_deg" in names
        assert "GUNF:rf_field_scale" in names
        assert "SOL10111:solenoid_field_scale" in names
        assert "initial_particles_path" in names

        # Numeric columns should be float64
        for nm in names:
            if nm.endswith("_path"):
                continue
            assert tbl.dtype[nm] == np.float64, f"{nm} should be float64"


@pytest.mark.slow
def test_targets_table_has_values(temp_output_dir, check_ds_file_available):
    """Test that the targets table contains non-zero beam property values."""
    script_path = EXAMPLES_DIR / "FACET-II_to_ML_Example.py"
    cmd = [
        sys.executable, str(script_path),
        "--input", str(DS_FILE),
        "--output-dir", temp_output_dir,
    ]
    subprocess.run(cmd, capture_output=True, timeout=120)

    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    with h5py.File(h5_files[0], "r") as f:
        meta = json.loads(f.attrs["meta"])
        screen = meta["screen"]
        tbl = f["targets_%s/table" % screen]

        assert tbl.dtype.names is not None
        # Check that sigma_x values are present and non-zero
        if "sigma_x" in tbl.dtype.names:
            sigma_x = tbl["sigma_x"]
            assert not np.all(sigma_x == 0), "sigma_x should not be all zeros"
            assert np.all(np.isfinite(sigma_x)), "sigma_x should be finite"


@pytest.mark.slow
def test_particle_groups_copied(temp_output_dir, check_ds_file_available):
    """Test that ParticleGroups are correctly stored in the output."""
    script_path = EXAMPLES_DIR / "FACET-II_to_ML_Example.py"
    cmd = [
        sys.executable, str(script_path),
        "--input", str(DS_FILE),
        "--output-dir", temp_output_dir,
    ]
    subprocess.run(cmd, capture_output=True, timeout=120)

    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    with h5py.File(h5_files[0], "r") as f:
        meta = json.loads(f.attrs["meta"])
        screen = meta["screen"]
        n_shots = meta["n_shots"]

        # Check initial_particles
        ip_grp = f["features_%s/initial_particles" % screen]
        assert len(ip_grp) == n_shots, (
            "Expected %d initial_particles, got %d" % (n_shots, len(ip_grp))
        )

        # Verify first particle group has openPMD structure
        pg0 = ip_grp["0"]
        assert "electron" in pg0, "Missing 'electron' species group"
        electron = pg0["electron"]
        assert "momentum" in electron, "Missing momentum group"
        assert "position" in electron, "Missing position group"

        # Check momentum has x, y, z arrays
        mom = electron["momentum"]
        assert "x" in mom
        px = mom["x"][:]
        assert px.ndim == 1, "Particle momentum should be 1-D per shot"
        assert px.shape[0] > 0, "Particle array should not be empty"


@pytest.mark.slow
def test_n_shots_matches_source(temp_output_dir, check_ds_file_available):
    """Test that total N in ML file matches sum of batch_dims in source."""
    script_path = EXAMPLES_DIR / "FACET-II_to_ML_Example.py"
    cmd = [
        sys.executable, str(script_path),
        "--input", str(DS_FILE),
        "--output-dir", temp_output_dir,
    ]
    subprocess.run(cmd, capture_output=True, timeout=120)

    # Count expected N from source
    with h5py.File(DS_FILE, "r") as src:
        ids = [x.decode() if isinstance(x, bytes) else x for x in src.attrs["IDs"]]
        expected_n = 0
        for uid in ids:
            bd = np.asarray(src[uid].attrs["batch_dims"]).ravel()
            expected_n += int(bd[0]) if bd.size > 0 else 1

    # Check ML output
    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    with h5py.File(h5_files[0], "r") as f:
        meta = json.loads(f.attrs["meta"])
        assert meta["n_shots"] == expected_n


@pytest.mark.slow
def test_screen_autodetection(temp_output_dir, check_ds_file_available):
    """Test that the script auto-detects the correct screen."""
    script_path = EXAMPLES_DIR / "FACET-II_to_ML_Example.py"
    cmd = [
        sys.executable, str(script_path),
        "--input", str(DS_FILE),
        "--output-dir", temp_output_dir,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0

    h5_files = list(Path(temp_output_dir).glob("*.h5"))
    with h5py.File(h5_files[0], "r") as f:
        meta = json.loads(f.attrs["meta"])
        # The simulation example data has PR10241, so screen should be "241"
        assert meta["screen"] == "L0AFEND"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
