"""FACET-II_to_ML_Example.py - Convert Data Standard HDF5 to ML-ready format.

This script reads a Data Standard HDF5 file produced by FACET-II_Simulation_Example.py
and converts it into a single ML-ready HDF5 file with the following structure:

    features_<screen>/
        table           compound dtype with scalar feature columns + PG path
        initial_particles/<i>/   openPMD ParticleGroup per shot (from VCCF)

    targets_<screen>/
        table           compound dtype with target columns + PG path(s)
        <screen>_particles/<i>/  openPMD ParticleGroup per shot

    @meta               JSON metadata attribute

The ML-ready format is designed for training surrogate models: features are the
simulation input parameters (gun phase, solenoid strength, etc.) and targets are
beam properties at downstream screen locations (sigma_x, sigma_y, etc.).

This example is intended to be run immediately after FACET-II_Simulation_Example.py,
which produces the combined Data Standard file consumed here.

Usage:
    # Run after FACET-II_Simulation_Example.py has produced its output
    python FACET-II_to_ML_Example.py

    # Or with custom paths
    python FACET-II_to_ML_Example.py \
        --input ./examples/data/output/FACET-II_Simulation_Example/FACET-II_Simulation_Example_v0.1.1.h5 \
        --output-dir ./examples/data/output/FACET-II_to_ML_Example/

    # Specify screen location
    python FACET-II_to_ML_Example.py --screen 241

Requires: h5py, numpy
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import h5py


# -- Configuration -----------------------------------------------------------
# Default paths relative to repository root (assumes running from repo root)
DEFAULT_INPUT = "./examples/data/output/FACET-II_Simulation_Example/FACET-II_Simulation_Example_v0.1.1.h5"
DEFAULT_OUTPUT_DIR = "./examples/data/output/FACET-II_to_ML_Example/"

# Feature columns: scalar input parameters available in the Data Standard file.
# These are stored as observables/<location>/<name> with shape (n,) for batched
# data or shape () for single-shot data.
FEATURES_241 = [
    "GUNF:theta0_deg", "GUNF:rf_field_scale", "SOL10111:solenoid_field_scale",
    "CQ10121:b1_gradient", "SQ10122:b1_gradient",
]
FEATURES_L0AFEND = ["L0AF_phase:theta0_deg", "L0AF_scale:rf_field_scale"]
FEATURES_571 = [
    "L0BF_phase:theta0_deg", "L0BF_scale:rf_field_scale",
    "QA10361", "QA10371", "QE10425", "QE10441", "QE10511", "QE10525",
]

SCREEN_FEATURES = {
    "L0AFEND": FEATURES_241 + FEATURES_L0AFEND + FEATURES_571,
    "241": FEATURES_241,
    "571": FEATURES_241 + FEATURES_L0AFEND + FEATURES_571,
}

# Target columns: beam properties extracted from multi_location_data at the
# screen z-position. These are the quantities the ML model will predict.
TARGET_COLUMNS = [
    "sigma_x", "sigma_y", "norm_emit_x", "norm_emit_y", "mean_kinetic_energy",
]

# Screen z-positions (meters) for extracting targets from multi_location_data.
# These correspond to the profile monitor locations in the FACET-II injector.
SCREEN_Z_POSITIONS = {"L0AFEND": 4.13, "241": 0.94, "571": 5.71}

# Locations of particle groups for each screen.
# initial_particles are the cathode distribution (at VCCF location).
# Screen particles are at the corresponding profile monitor location.
SCREEN_LOCATION = {"L0AFEND": "L0AFEND", "241": "PR10241", "571": "PR10571"}
PG_LOCATIONS = {
    "L0AFEND": {
        "initial_particles": "VCCF",
        "241_particles": "PR10241",
        "L0AFEND_particles": "L0AFEND",
    },
    "241": {"initial_particles": "VCCF", "241_particles": "PR10241"},
    "571": {
        "initial_particles": "VCCF",
        "241_particles": "PR10241",
        "L0AFEND_particles": "L0AFEND",
        "571_particles": "PR10571",
    },
}
# -----------------------------------------------------------------------------


def _dec(x):
    """Decode bytes to str (h5py returns bytes for variable-length strings)."""
    return x.decode() if isinstance(x, (bytes, bytearray)) else str(x)


def _batch_ids(f):
    """Get ordered list of batch/UUID identifiers from a Data Standard file.

    Uses the root @IDs attribute (set by combine_files); falls back to
    enumerating top-level groups that aren't lattice or metadata.
    """
    if "IDs" in f.attrs:
        return [_dec(x) for x in f.attrs["IDs"]]
    return [k for k in f.keys() if k not in ("lattice",)]


def _get_n_shots(g):
    """Determine number of shots from a UUID group's batch_dims attribute.

    For batch_dims=[] (single shot), returns 1.
    For batch_dims=[N], returns N.
    """
    bd = np.asarray(g.attrs["batch_dims"]).ravel()
    if bd.size == 0:
        return 1
    return int(bd[0])


def _collect_scalars(obs, n):
    """Extract all scalar numeric datasets from an observables group.

    Walks every location group, finds datasets with shape (n,) (batched) or
    shape () (single shot, treated as n=1). Builds column names as
    "location:dataset_name".

    Parameters
    ----------
    obs : h5py.Group
        The /<UUID>/observables group.
    n : int
        Expected number of shots.

    Returns
    -------
    dict : {column_name: ndarray float64 of shape (n,)}
    """
    out = {}
    for loc in obs:
        g = obs[loc]
        if not isinstance(g, h5py.Group):
            continue
        # Skip groups that contain particle data or multi-location data
        if loc in ("multi_location_data", "final_particles"):
            continue
        for nm in g:
            it = g[nm]
            if not isinstance(it, h5py.Dataset):
                continue
            if it.dtype.kind not in "fiu":
                continue
            # Handle both batched (n,) and unbatched () shapes
            if it.ndim == 0 and n == 1:
                colname = "%s:%s" % (loc, nm)
                out[colname] = np.array([float(it[()])], dtype=np.float64)
            elif it.ndim == 1 and it.shape[0] == n:
                colname = "%s:%s" % (loc, nm)
                out[colname] = np.asarray(it[:], dtype=np.float64)
    return out


def _extract_targets_at_screen(obs, n, screen, target_columns):
    """Extract target values from multi_location_data at the screen z-position.

    The Data Standard stores simulation statistics (sigma_x, sigma_y, etc.)
    along the full beamline as 2D arrays: (n_shots, n_z_locations). This
    function finds the z-index closest to the screen location and extracts
    the corresponding column for each target.

    Parameters
    ----------
    obs : h5py.Group
        The /<UUID>/observables group.
    n : int
        Number of shots.
    screen : str
        Screen identifier ("241" or "571").
    target_columns : list of str
        Target observable names to extract.

    Returns
    -------
    dict : {target_name: ndarray float64 of shape (n,)}
    """
    out = {}
    if "multi_location_data" not in obs:
        return out

    mld = obs["multi_location_data"]
    screen_z = SCREEN_Z_POSITIONS.get(screen)
    if screen_z is None:
        return out

    # Find the z-index closest to the screen location
    if "DATA_LOCATIONS" not in mld:
        return out
    z_locs = mld["DATA_LOCATIONS"][:]
    z_idx = int(np.argmin(np.abs(z_locs - screen_z)))

    for col in target_columns:
        if col not in mld:
            continue
        ds = mld[col]
        if ds.ndim == 2 and ds.shape[0] == n:
            # Batched: shape (n, n_z), extract column at z_idx
            out[col] = np.asarray(ds[:, z_idx], dtype=np.float64)
        elif ds.ndim == 1 and n == 1:
            # Single shot: shape (n_z,), extract value at z_idx
            out[col] = np.array([float(ds[z_idx])], dtype=np.float64)

    return out


def _copy_particle_groups(src_path, dst_path, ids, batch_sizes, screen):
    """Copy openPMD ParticleGroups from Data Standard source into ML output.

    In the Data Standard, particle groups are stored per-shot as:
        /<UUID>/observables/<location>/<data_name>_<i>/electron/...

    For example: /UUID/observables/VCCF/initial_particles_0/electron/...
                 /UUID/observables/PR10241/241_particles_0/electron/...

    In the ML output, each shot gets its own group:
        features_<screen>/initial_particles/<i>/electron/...
        targets_<screen>/<pg_type>/<i>/electron/...

    Parameters
    ----------
    src_path : str
        Data Standard source file path.
    dst_path : str
        ML output file path (opened in append mode).
    ids : list of str
        Batch UUIDs in order.
    batch_sizes : list of int
        Number of shots per batch.
    screen : str
        Screen identifier.
    """
    pg_locs = PG_LOCATIONS.get(screen, {})
    feat_key = "features_%s" % screen
    tgt_key = "targets_%s" % screen

    # Pre-create destination groups
    with h5py.File(dst_path, "a") as dst:
        fg = dst[feat_key]
        tg = dst[tgt_key]
        if "initial_particles" not in fg:
            fg.create_group("initial_particles")
        for pt in pg_locs:
            if pt == "initial_particles":
                continue
            if pt not in tg:
                tg.create_group(pt)

    # Copy particle groups from source to destination
    n_copied = 0
    offset = 0
    with h5py.File(src_path, "r") as src, h5py.File(dst_path, "a") as dst:
        fg = dst[feat_key]
        tg = dst[tgt_key]

        for uuid, n in zip(ids, batch_sizes):
            obs = src[uuid]["observables"]

            for pt, src_loc in pg_locs.items():
                if src_loc not in obs:
                    continue
                loc_grp = obs[src_loc]

                # Determine destination parent
                if pt == "initial_particles":
                    dst_parent = fg["initial_particles"]
                else:
                    dst_parent = tg[pt]

                # Per-shot particle groups are named <data_name>_<i>
                for i in range(n):
                    src_name = "%s_%d" % (pt, i)
                    if src_name in loc_grp:
                        loc_grp.copy(src_name, dst_parent, name=str(offset + i))
                        n_copied += 1

            offset += n

    print("[pg_copy] %d ParticleGroups copied" % n_copied)




def convert(ds_path, output_dir, screen=None):
    """Convert a Data Standard file into an ML-ready HDF5.

    Pipeline:
      1. Scan all UUID groups, determine n_shots per group.
      2. Collect scalar input parameters (features).
      3. Extract target values from multi_location_data at screen location.
      4. Write compound-dtype tables with features and targets.
      5. Copy openPMD ParticleGroups from source into output.

    Parameters
    ----------
    ds_path : str
        Path to the Data Standard .h5 file (from FACET-II_Simulation_Example.py).
    output_dir : str
        Output directory for the ML-ready file.
    screen : str, optional
        Screen identifier ("241" or "571"). Auto-detected if not specified.

    Returns
    -------
    str : path to the output ML-ready .h5 file.
    """
    t_wall = time.perf_counter()

    with h5py.File(ds_path, "r") as src:
        ids = _batch_ids(src)

        # Auto-detect screen from available locations
        if screen is None:
            for cand, loc in SCREEN_LOCATION.items():
                if any(loc in src[u].get("observables", {}) for u in ids):
                    screen = cand
                    break
        if screen is None:
            raise RuntimeError("Cannot determine screen from %s" % ds_path)

        print("[info] %s  screen=%s  %d batches" % (Path(ds_path).name, screen, len(ids)))

        # --- Phase 1: extract scalars and targets from each UUID ---
        t1 = time.perf_counter()
        all_scalars = {}
        all_targets = {}
        batch_sizes = []

        for uuid in ids:
            g = src[uuid]
            n = _get_n_shots(g)
            batch_sizes.append(n)
            obs = g["observables"]

            # Collect scalar features
            sc = _collect_scalars(obs, n)
            for k, v in sc.items():
                all_scalars.setdefault(k, []).append(v)

            # Extract targets at screen location
            tgt = _extract_targets_at_screen(obs, n, screen, TARGET_COLUMNS)
            for k, v in tgt.items():
                all_targets.setdefault(k, []).append(v)

        N = sum(batch_sizes)
        print("[phase1] scalars+targets  N=%d  %d feature_cols  %d target_cols  t=%.1fs" % (
            N, len(all_scalars), len(all_targets), time.perf_counter() - t1))

        # --- Phase 2: classify features / targets / misc ---
        feat_names = [c for c in SCREEN_FEATURES.get(screen, []) if c in all_scalars]
        tgt_names = [c for c in TARGET_COLUMNS if c in all_targets]
        used = set(feat_names)
        misc_names = [c for c in sorted(all_scalars) if c not in used]

        def _concat_scalars(cols, source):
            if not cols:
                return np.zeros((N, 0), dtype=np.float64)
            return np.column_stack([np.concatenate(source[c]) for c in cols])

        features = _concat_scalars(feat_names, all_scalars)
        targets = _concat_scalars(tgt_names, all_targets)
        misc = _concat_scalars(misc_names, all_scalars)

    # --- Phase 3: write output ---
    t3 = time.perf_counter()
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, Path(ds_path).stem + "_ml.h5")
    print("[phase3] writing %s ..." % Path(out_path).name)

    # Build compound dtype for features table (float columns + string PG path)
    all_feat_float = feat_names + misc_names
    str_dt = h5py.string_dtype()

    feat_dt_fields = [(c, np.float64) for c in all_feat_float]
    feat_dt_fields.append(("initial_particles_path", str_dt))
    feat_compound = np.dtype(feat_dt_fields)

    feat_table = np.empty(N, dtype=feat_compound)
    # Fill float columns from features + misc arrays
    feat_arrays = np.column_stack([features] + ([misc] if misc.shape[1] > 0 else []))
    for ci, col in enumerate(all_feat_float):
        feat_table[col] = feat_arrays[:, ci]
    feat_table["initial_particles_path"] = [
        "features_%s/initial_particles/%d" % (screen, i) for i in range(N)]

    # Build compound dtype for targets table (float columns + string PG paths)
    pg_screen_types = [pt for pt in PG_LOCATIONS.get(screen, {}) if pt != "initial_particles"]
    tgt_dt_fields = [(c, np.float64) for c in tgt_names]
    for pt in pg_screen_types:
        tgt_dt_fields.append(("%s_path" % pt, str_dt))
    tgt_compound = np.dtype(tgt_dt_fields)

    tgt_table = np.empty(N, dtype=tgt_compound)
    for ci, col in enumerate(tgt_names):
        tgt_table[col] = targets[:, ci] if ci < targets.shape[1] else np.nan
    for pt in pg_screen_types:
        tgt_table["%s_path" % pt] = [
            "targets_%s/%s/%d" % (screen, pt, i) for i in range(N)]

    with h5py.File(out_path, "w") as dst:
        # Features group: compound table
        fg = dst.create_group("features_%s" % screen)
        fg.create_dataset("table", data=feat_table)

        # Targets group: compound table
        tg = dst.create_group("targets_%s" % screen)
        tg.create_dataset("table", data=tgt_table)

        # Metadata
        dst.attrs["meta"] = json.dumps({
            "screen": screen,
            "source": str(ds_path),
            "n_shots": int(N),
            "n_batches": int(len(ids)),
            "feature_names": list(feat_compound.names),
            "target_names": list(tgt_compound.names),
            "pg_types": list(PG_LOCATIONS.get(screen, {}).keys()),
        })

    print("[phase3] tables written  t=%.1fs" % (time.perf_counter() - t3))

    # --- Phase 4: copy ParticleGroups ---
    t4 = time.perf_counter()
    _copy_particle_groups(ds_path, out_path, ids, batch_sizes, screen)
    print("[phase4] PGs copied  t=%.1fs" % (time.perf_counter() - t4))

    t_wall = time.perf_counter() - t_wall
    print("[done] %s  N=%d  wall=%.1fs" % (out_path, N, t_wall))
    return out_path



def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Convert Data Standard HDF5 to ML-ready format (features/targets/PGs).")
    ap.add_argument("--input", default=DEFAULT_INPUT,
                    help="Data Standard .h5 file (default: %%(default)s)")
    ap.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                    help="Output directory (default: %%(default)s)")
    ap.add_argument("--screen", default=None, choices=["L0AFEND", "241", "571"],
                    help="Screen to extract targets for (auto-detected if omitted)")
    args = ap.parse_args(argv)

    if not os.path.isfile(args.input):
        print("[ERROR] Input file not found: %s" % args.input, file=sys.stderr)
        print("  Run FACET-II_Simulation_Example.py first to generate the Data Standard file.",
              file=sys.stderr)
        return 1

    try:
        convert(args.input, args.output_dir, screen=args.screen)
    except Exception as exc:
        print("[FAIL] %s: %s" % (args.input, exc), file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
