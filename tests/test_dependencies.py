"""
Regression tests for the dependency mix-up that broke CI in 0.1.2.

CI installed `pmd-beamphysics`, the project's old PyPI name (last release 0.5.6). That release
calls `np.string_` (removed in NumPy 2) and its unit table has no 'W', so ParticleGroup writes
and prefixed power units ('mW', 'uW') failed. The package is `openpmd-beamphysics`.
"""
import importlib.metadata
import re
from pathlib import Path

import pytest

WORKFLOW = Path(__file__).parent.parent / ".github" / "workflows" / "tests.yml"


def test_openpmd_beamphysics_is_installed():
    try:
        importlib.metadata.version("openpmd-beamphysics")
    except importlib.metadata.PackageNotFoundError:
        pytest.fail("Install 'openpmd-beamphysics' (not the obsolete 'pmd-beamphysics')")


def test_unit_table_knows_watt():
    from pmd_beamphysics import units
    assert "W" in units.known_unit


def test_ci_installs_openpmd_beamphysics():
    yaml = pytest.importorskip("yaml")
    if not WORKFLOW.exists():
        pytest.skip(f"{WORKFLOW} not found")
    workflow = yaml.safe_load(WORKFLOW.read_text())
    commands = "\n".join(
        step.get("run", "")
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
    )
    pip_lines = [line for line in commands.splitlines() if "pip install" in line]
    assert any("openpmd-beamphysics" in line for line in pip_lines), pip_lines
    assert not any(re.search(r"(?<![\w-])pmd-beamphysics\b", line) for line in pip_lines), pip_lines
