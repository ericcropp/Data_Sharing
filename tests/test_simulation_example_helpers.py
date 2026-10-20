"""
Fast tests for helpers in examples/FACET-II_Simulation_Example.py.

batch_array() stores values that are identical across a batch once (size-1 batch dimension,
broadcast by the Data Standard), which keeps constant inputs such as header settings from being
repeated for every simulation.
"""
import importlib.util
from pathlib import Path

import numpy as np
import pytest
from pmd_beamphysics import ParticleGroup

pytest.importorskip("impact", reason="lume-impact is needed to import the simulation example")

SIM_EXAMPLE = Path(__file__).parent.parent / "examples" / "FACET-II_Simulation_Example.py"


@pytest.fixture(scope="module")
def sim():
    spec = importlib.util.spec_from_file_location("facet_ii_simulation_example", SIM_EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _particle_group(x0=0.0, n=8):
    return ParticleGroup(data={
        'x': np.full(n, float(x0)), 'px': np.zeros(n), 'y': np.zeros(n), 'py': np.zeros(n),
        'z': np.zeros(n), 'pz': np.ones(n) * 1e6, 't': np.zeros(n), 'status': np.ones(n),
        'weight': np.ones(n) * 1e-15, 'species': 'electron',
    })


def test_scalar_unbatched(sim):
    out = sim.batch_array([3.0], (), same=np.array_equal)
    assert out.shape == () and out == 3.0


def test_scalar_varying_per_shot(sim):
    out = sim.batch_array([1.0, 2.0, 3.0], (3,), same=np.array_equal)
    np.testing.assert_array_equal(out, [1.0, 2.0, 3.0])


def test_scalar_constant_stored_once(sim):
    out = sim.batch_array([5.0, 5.0, 5.0], (3,), same=np.array_equal)
    assert out.shape == (1,) and out[0] == 5.0


def test_arrays_constant_and_varying(sim):
    steps = np.linspace(0, 1, 4)
    assert sim.batch_array([steps, steps.copy()], (2,), same=np.array_equal).shape == (1, 4)
    assert sim.batch_array([steps, steps + 1], (2,), same=np.array_equal).shape == (2, 4)
    assert sim.batch_array([steps], (), same=np.array_equal).shape == (4,)


def test_particle_groups(sim):
    single = sim.batch_array([_particle_group()], (), same=sim.particle_groups_equal)
    assert single.shape == () and single.dtype == object and isinstance(single[()], ParticleGroup)

    same = sim.batch_array([_particle_group(1.0), _particle_group(1.0)], (2,), same=sim.particle_groups_equal)
    assert same.shape == (1,) and isinstance(same[0], ParticleGroup)

    varying = sim.batch_array([_particle_group(1.0), _particle_group(2.0)], (2,), same=sim.particle_groups_equal)
    assert varying.shape == (2,) and varying[1]['mean_x'] == pytest.approx(2.0)


def test_particle_groups_equal(sim):
    assert sim.particle_groups_equal(_particle_group(1.0), _particle_group(1.0))
    assert not sim.particle_groups_equal(_particle_group(1.0), _particle_group(2.0))
    assert not sim.particle_groups_equal(_particle_group(1.0, n=8), _particle_group(1.0, n=9))
