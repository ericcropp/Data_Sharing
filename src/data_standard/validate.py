"""
validate.py

Standalone validation for the Data Standard.

Two entry points are provided:

- :func:`validate_datapoint` validates an in-memory ``DataPoint2`` / ``SimulatedDataPoint2``
  (runs every component checker without finalizing or saving).
- :func:`validate_file` validates an HDF5 file on disk against SPEC.md. Both single
  data-point files (written by ``saveHDF5``) and combined files (written by
  ``combine_files``) are supported.

Both return a :class:`ValidationReport` holding ``errors`` (violations of the standard)
and ``warnings`` (legacy or discouraged constructs that are still accepted, e.g. the
pre-0.1.2 ``@control`` and ``@unit_multiplier`` attributes).

Command line:
    python -m data_standard <file.h5> [<file.h5> ...] [--strict] [--quiet]
    data-standard-validate <file.h5> ...          (console script, when the package is installed)

The exit code is 0 if every file is valid and 1 otherwise.
"""

import argparse
import re
import sys
from dataclasses import dataclass, field

import h5py
import numpy as np

from .Data_Standard_2 import (
    VERSION,
    TIMESTAMP_NAME,
    MULTI_LOCATION_GROUP,
    DATA_LOCATIONS_NAME,
    DataPoint2,
    SimulationMetadata,
    is_iso8601_timestamp,
    is_multi_location_group_name,
    is_broadcastable_batch_shape,
)

__all__ = [
    "ValidationReport",
    "DataStandardValidationError",
    "validate_datapoint",
    "validate_file",
    "main",
]

_VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATAPOINT_ATTRS = ("Data_Standard_Version", "ID", "batch_dims",
                    "run_information_source", "run_information_date", "run_information_notes")
_SIMULATION_ATTRS = ("simulation_code", "simulation_version", "simulation_start", "simulation_end")


class DataStandardValidationError(ValueError):
    """Raised when validation finds one or more errors."""

    def __init__(self, errors):
        self.errors = list(errors)
        super().__init__("Data Standard validation failed:\n  - " + "\n  - ".join(self.errors))


@dataclass
class ValidationReport:
    """Result of a validation run."""

    target: str
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)

    @property
    def ok(self):
        """True if no errors were found (warnings are allowed)."""
        return not self.errors

    def error(self, path, message):
        self.errors.append(f"{path}: {message}")

    def warn(self, path, message):
        self.warnings.append(f"{path}: {message}")

    def raise_if_errors(self):
        """Raise :class:`DataStandardValidationError` if there are errors; otherwise return self."""
        if self.errors:
            raise DataStandardValidationError(self.errors)
        return self

    def format(self, show_warnings=True):
        status = "PASS" if self.ok else "FAIL"
        lines = [f"{status} {self.target} ({len(self.errors)} error(s), {len(self.warnings)} warning(s))"]
        lines += [f"  error: {e}" for e in self.errors]
        if show_warnings:
            lines += [f"  warning: {w}" for w in self.warnings]
        return "\n".join(lines)

    def __str__(self):
        return self.format()


# ==============================================
# In-memory validation
# ==============================================
def validate_datapoint(datapoint, raise_on_error=True):
    """
    Validate an in-memory data point without finalizing or saving it.

    Runs the lattice, observables, run information and (if present) simulation metadata
    checkers independently, so every failing component is reported.

    Args:
        datapoint (DataPoint2 or SimulatedDataPoint2): Data point to validate.
        raise_on_error (bool): Raise DataStandardValidationError if any check fails.

    Returns:
        ValidationReport
    """
    if not isinstance(datapoint, DataPoint2):
        raise TypeError(f"validate_datapoint expects a DataPoint2, got {type(datapoint)}")
    report = ValidationReport(target=type(datapoint).__name__)
    checks = [
        ("lattice", datapoint.lattice.lattice_checker),
        ("observables", datapoint.observables.observable_checker),
        ("run_information", datapoint.run_information.run_info_checker),
    ]
    simulation_metadata = getattr(datapoint, "simulation_metadata", None)
    if isinstance(simulation_metadata, SimulationMetadata):
        checks.append(("simulation_metadata", simulation_metadata.sim_data_checker))
    for name, check in checks:
        try:
            check()
        except (TypeError, ValueError, AssertionError) as e:
            report.error(name, str(e))
    if raise_on_error:
        report.raise_if_errors()
    return report


# ==============================================
# HDF5 file validation
# ==============================================
def _decode(value):
    if isinstance(value, (bytes, np.bytes_)):
        return value.decode("utf-8")
    if isinstance(value, np.str_):
        return str(value)
    return value


def _is_bool(value):
    return isinstance(value, (bool, np.bool_))


def _is_int(value):
    return isinstance(value, (int, np.integer)) and not _is_bool(value)


def _is_real(value):
    return isinstance(value, (int, float, np.integer, np.floating)) and not _is_bool(value)


def _is_string_dataset(ds):
    return h5py.check_string_dtype(ds.dtype) is not None or ds.dtype.kind == "S"


def _split_pg_name(name, rank):
    """Split '<data_name>_<i>[_<j>...]' (``rank`` indices) into (data_name, index), or None."""
    parts = name.rsplit("_", rank)
    if len(parts) != rank + 1 or parts[0] == "" or not all(p.isdigit() for p in parts[1:]):
        return None
    return parts[0], tuple(int(p) for p in parts[1:])


def _group_particle_groups(parent_path, names, batch_dims, report):
    """
    Group the ParticleGroup entries of one parent group into observables.

    A ParticleGroup observable is either a single group named '<data_name>' (one ParticleGroup for
    every shot) or a full grid of '<data_name>_<i>[_<j>...]' groups, one index per batch dimension,
    whose index ranges are batch_dims[i] or 1 (broadcast).

    Returns:
        dict: observable name -> number of HDF5 entries
    """
    observables = {}
    if batch_dims is None:
        return {n: 1 for n in names}
    if len(batch_dims) == 0:
        for n in names:
            observables[n] = 1
            if _split_pg_name(n, 1) == (n[:-2], (0,)):
                report.warn(f"{parent_path}/{n}", "legacy ParticleGroup name '<data_name>_0' for batch_dims=(); "
                                                  "since 0.1.2 a single ParticleGroup is named '<data_name>'")
        return observables

    rank = len(batch_dims)
    by_base = {}
    for n in names:
        parsed = _split_pg_name(n, rank)
        if parsed is None:
            observables[n] = 1
        else:
            by_base.setdefault(parsed[0], []).append((n, parsed[1]))
    for base, entries in by_base.items():
        if len(entries) == 1:
            observables[entries[0][0]] = 1
            continue
        indices = [idx for _, idx in entries]
        shape = tuple(max(idx[d] for idx in indices) + 1 for d in range(rank))
        if (len(indices) != len(set(indices)) or set(indices) != set(np.ndindex(*shape))
                or not is_broadcastable_batch_shape(shape, batch_dims)):
            report.error(f"{parent_path}/{base}_*",
                         f"ParticleGroup indices {sorted(indices)} do not form a grid matching batch_dims {batch_dims} "
                         f"(each index range must be batch_dims[i] or 1)")
        observables[base] = len(entries)
    return observables


def _is_particle_group(grp):
    """openPMD ParticleGroups contain a species subgroup carrying speciesType/numParticles."""
    for child in grp.values():
        if isinstance(child, h5py.Group) and ("speciesType" in child.attrs or "numParticles" in child.attrs):
            return True
    return False


class _ObservablesContext:
    """Collects information across one observables/ tree for cross-reference checks."""

    def __init__(self, batch_dims):
        self.batch_dims = batch_dims
        self.dataset_shapes = {}
        self.bin_size_refs = []
        self.timestamps = []


def _check_version(value, path, report):
    value = _decode(value)
    if not isinstance(value, str) or not _VERSION_RE.match(value):
        report.error(path, f"@Data_Standard_Version must be a string of the form X.Y.Z, got {value!r}")
        return None
    if value != VERSION:
        report.warn(path, f"file uses Data_Standard_Version {value}; this validator implements {VERSION}")
    return value


def _check_batch_dims(attrs, path, report):
    if "batch_dims" not in attrs:
        report.error(path, "missing @batch_dims")
        return None
    bd = np.asarray(attrs["batch_dims"])
    if bd.size == 0:
        return ()
    if bd.ndim != 1 or bd.dtype.kind not in "iu" or np.any(bd <= 0):
        report.error(path, f"@batch_dims must be a 1-D array of positive integers, got {bd!r}")
        return None
    return tuple(int(d) for d in bd)


def _check_lattice(lg, path, report):
    if not isinstance(lg, h5py.Group):
        report.error(path, "lattice must be a group")
        return
    if "lattice_location" not in lg.attrs:
        report.error(path, "missing @lattice_location")
        lattice_location = None
    else:
        lattice_location = _decode(lg.attrs["lattice_location"])
        if not isinstance(lattice_location, str):
            report.error(path, f"@lattice_location must be a string, got {type(lattice_location).__name__}")
            lattice_location = None

    files = lg.get("lattice_files")
    n_files = 0
    if files is not None:
        if not isinstance(files, h5py.Group):
            report.error(f"{path}/lattice_files", "must be a group")
        else:
            # File names given as relative paths are stored as nested groups.
            entries = []
            files.visititems(lambda name, item: entries.append((name, item)) if isinstance(item, h5py.Dataset) else None)
            for name, item in entries:
                if not (_is_string_dataset(item) or item.dtype.kind in "SV"):
                    report.error(f"{path}/lattice_files/{name}", "lattice files must be string/bytes datasets")
            n_files = len(entries)
    if lattice_location == "included" and n_files == 0:
        report.error(path, "@lattice_location is 'included' but lattice_files/ is missing or empty")
    elif lattice_location not in (None, "", "included") and n_files > 0:
        report.warn(path, "lattice files are stored although @lattice_location is an external reference")

    if "lattice_mapping" in lg:
        mapping = lg["lattice_mapping"]
        if not isinstance(mapping, h5py.Dataset) or mapping.ndim != 2 or mapping.shape[0] != 2:
            report.error(f"{path}/lattice_mapping", "must be a 2 x n dataset (row 0: PV names, row 1: lattice elements)")
        elif not _is_string_dataset(mapping):
            report.error(f"{path}/lattice_mapping", f"must contain strings, got dtype {mapping.dtype}")

    for name in lg.keys():
        if name not in ("lattice_files", "lattice_mapping") and not name.startswith("simulation_input_file"):
            report.warn(f"{path}/{name}", "unexpected entry in lattice group")


def _check_datapoint_attrs(g, path, report, root_version=None):
    attrs = g.attrs
    for key in _DATAPOINT_ATTRS:
        if key not in attrs:
            report.error(path, f"missing @{key}")

    if "Data_Standard_Version" in attrs:
        version = _check_version(attrs["Data_Standard_Version"], path, report)
        if root_version is not None and version is not None and version != root_version:
            report.error(path, f"@Data_Standard_Version {version} does not match root version {root_version}")

    if "ID" in attrs:
        id_value = _decode(attrs["ID"])
        if not isinstance(id_value, str) or id_value == "":
            report.error(path, "@ID must be a non-empty string")

    source = _decode(attrs.get("run_information_source", ""))
    if "run_information_source" in attrs and (not isinstance(source, str) or source == ""):
        report.error(path, "@run_information_source must be a non-empty string")
    date = _decode(attrs.get("run_information_date", ""))
    if "run_information_date" in attrs and (not isinstance(date, str) or (date and not _DATE_RE.match(date))):
        report.error(path, f"@run_information_date must be YYYY-MM-DD or empty, got {date!r}")
    notes = _decode(attrs.get("run_information_notes", ""))
    if "run_information_notes" in attrs and not isinstance(notes, str):
        report.error(path, "@run_information_notes must be a string")

    present = [k for k in _SIMULATION_ATTRS if k in attrs]
    if present:
        for key in _SIMULATION_ATTRS:
            if key not in attrs:
                report.error(path, f"simulation data point is missing @{key}")
        for key in ("simulation_code", "simulation_version"):
            if key in attrs:
                value = _decode(attrs[key])
                if not isinstance(value, str) or value == "":
                    report.error(path, f"@{key} must be a non-empty string")
        bounds = {}
        for key in ("simulation_start", "simulation_end"):
            if key in attrs:
                try:
                    bounds[key] = float(_decode(attrs[key]))
                except (TypeError, ValueError):
                    report.error(path, f"@{key} must be numeric, got {_decode(attrs[key])!r}")
        if len(bounds) == 2 and not bounds["simulation_start"] < bounds["simulation_end"]:
            report.error(path, "@simulation_start must be less than @simulation_end")

    return _check_batch_dims(attrs, path, report)


def _check_read_only(attrs, path, report):
    """Returns the effective read_only flag (or None if undeterminable)."""
    read_only = attrs.get("read_only")
    control = attrs.get("control")
    if read_only is None and control is None:
        report.error(path, "missing @read_only")
        return None
    if read_only is not None and not _is_bool(read_only):
        report.error(path, f"@read_only must be a boolean, got {type(read_only).__name__}")
        read_only = None
    if control is not None:
        if not _is_bool(control):
            report.error(path, f"@control must be a boolean, got {type(control).__name__}")
            control = None
        else:
            report.warn(path, "legacy @control attribute; use @read_only (= not control)")
    if read_only is not None and control is not None and bool(read_only) == bool(control):
        report.error(path, "@read_only and legacy @control are inconsistent (read_only must equal not control)")
    if read_only is not None:
        return bool(read_only)
    if control is not None:
        return not bool(control)
    return None


def _check_dataset(ds, path, ctx, mode, expected_location=None, n_locations=None, report=None):
    """
    Validate one observable dataset.

    mode: 'top' (flattened, directly under observables/), 'nested' (inside a location group),
    or 'multi' (inside a multi-location group).
    """
    name = path.rsplit("/", 1)[-1]
    attrs = ds.attrs
    bd = ctx.batch_dims
    is_timestamp = name == TIMESTAMP_NAME

    location = _decode(attrs.get("location")) if "location" in attrs else None
    if mode == "multi":
        if location is not None:
            report.warn(path, "@location is ignored in multi-location groups (locations are in DATA_LOCATIONS)")
    elif location is None:
        if not (is_timestamp and mode == "top"):
            report.error(path, "missing @location")
    elif not isinstance(location, str) or location == "":
        report.error(path, "@location must be a non-empty string")
    elif expected_location is not None and location != expected_location:
        report.error(path, f"@location '{location}' does not match parent group '{expected_location}'")

    read_only = _check_read_only(attrs, path, report)

    nfd = attrs.get("num_feature_dims")
    if nfd is None:
        report.error(path, "missing @num_feature_dims")
    elif not _is_int(nfd) or nfd < 0:
        report.error(path, f"@num_feature_dims must be an integer >= 0, got {nfd!r}")
        nfd = None
    else:
        nfd = int(nfd)

    if "units" not in attrs:
        report.error(path, "missing @units")
    elif not isinstance(_decode(attrs["units"]), str):
        report.error(path, "@units must be a string")

    if "unit_multiplier" in attrs:
        multiplier = attrs["unit_multiplier"]
        if not _is_real(multiplier) or not multiplier > 0:
            report.error(path, f"@unit_multiplier must be a positive number, got {multiplier!r}")
        else:
            report.warn(path, "legacy @unit_multiplier attribute; units are now stored with their SI prefix")

    if nfd is not None and nfd > 0:
        if "bin_size" not in attrs:
            report.error(path, "missing @bin_size (required when num_feature_dims > 0)")
        else:
            bin_size = _decode(attrs["bin_size"])
            if isinstance(bin_size, str):
                if len(bd or ()) == 0:
                    report.error(path, "@bin_size may only be a string key when batch_dims is non-empty")
                else:
                    ctx.bin_size_refs.append((path, bin_size))
            elif not _is_real(bin_size):
                report.error(path, f"@bin_size must be a float or a string key, got {bin_size!r}")
        if "offset" not in attrs:
            report.error(path, "missing @offset (required when num_feature_dims > 0)")
        elif not _is_real(attrs["offset"]):
            report.error(path, f"@offset must be a float, got {attrs['offset']!r}")

    if bd is not None and nfd is not None:
        n_loc_dims = 1 if (mode == "multi" and n_locations is not None and n_locations != 1) else 0
        expected_ndim = len(bd) + n_loc_dims + nfd
        if ds.ndim != expected_ndim:
            report.error(path, f"expected {expected_ndim} dimensions (batch={len(bd)}, location={n_loc_dims}, "
                               f"feature={nfd}), got shape {ds.shape}")
        else:
            if not is_broadcastable_batch_shape(ds.shape[:len(bd)], bd):
                report.error(path, f"leading dimensions {ds.shape[:len(bd)]} do not match batch_dims {bd} "
                                   f"(each must be batch_dims[i] or 1)")
            if n_loc_dims and ds.shape[len(bd)] != n_locations:
                report.error(path, f"location dimension is {ds.shape[len(bd)]}, expected {n_locations} (DATA_LOCATIONS)")

    if is_timestamp:
        ctx.timestamps.append(path)
        if mode == "multi":
            report.error(path, "Timestamp must not be stored in a multi-location group")
        if nfd not in (None, 0):
            report.error(path, "Timestamp must have num_feature_dims = 0")
        if read_only is False:
            report.error(path, "Timestamp must be read_only")
        if not _is_string_dataset(ds):
            report.error(path, f"Timestamp must contain ISO 8601 strings, got dtype {ds.dtype}")
        else:
            values = np.atleast_1d(np.asarray(ds[()], dtype=object))
            bad = [_decode(v) for v in values.ravel() if not is_iso8601_timestamp(_decode(v))]
            if bad:
                report.error(path, f"{len(bad)} value(s) are not ISO 8601 date-times with UTC offset, e.g. {bad[0]!r}")
    elif ds.dtype.kind not in "biufc":
        report.error(path, f"observable data must be numeric, got dtype {ds.dtype}")

    ctx.dataset_shapes.setdefault(name, []).append(tuple(ds.shape))


def _check_particle_group(grp, path, require_location, expected_location, report):
    if "location" not in grp.attrs:
        if require_location:
            report.error(path, "missing @location")
        else:
            report.warn(path, "ParticleGroup has no @location attribute (required since 0.1.2)")
    else:
        location = _decode(grp.attrs["location"])
        if not isinstance(location, str) or location == "":
            report.error(path, "@location must be a non-empty string")
        elif expected_location is not None and location != expected_location:
            report.error(path, f"@location '{location}' does not match parent group '{expected_location}'")


def _check_multi_location(mg, path, ctx, report):
    group = path.rsplit("/", 1)[-1]
    n_locations = None
    if DATA_LOCATIONS_NAME not in mg or not isinstance(mg[DATA_LOCATIONS_NAME], h5py.Dataset):
        report.error(path, f"missing {DATA_LOCATIONS_NAME} dataset")
    else:
        locs = mg[DATA_LOCATIONS_NAME]
        if locs.ndim > 1:
            report.error(f"{path}/{DATA_LOCATIONS_NAME}", f"must be 1-D (shared by all shots), got shape {locs.shape}")
        else:
            n_locations = 1 if locs.ndim == 0 else locs.shape[0]
        units = _decode(locs.attrs.get("units"))
        if not isinstance(units, str) or units == "":
            report.error(f"{path}/{DATA_LOCATIONS_NAME}", "@units (location units) must be a non-empty string")

    feature_dims = set()
    pg_names = []
    for name, item in mg.items():
        if name == DATA_LOCATIONS_NAME:
            continue
        p = f"{path}/{name}"
        if isinstance(item, h5py.Dataset):
            _check_dataset(item, p, ctx, "multi", n_locations=n_locations, report=report)
            if "num_feature_dims" in item.attrs:
                feature_dims.add(int(item.attrs["num_feature_dims"]) if _is_int(item.attrs["num_feature_dims"]) else None)
        elif _is_particle_group(item):
            _check_particle_group(item, p, False, None, report)
            pg_names.append(name)
        else:
            report.error(p, f"unexpected group in {group}")
    _group_particle_groups(path, pg_names, ctx.batch_dims, report)
    if len(feature_dims) > 1:
        report.error(path, f"all observables in {group} must have the same num_feature_dims, found {sorted(d for d in feature_dims if d is not None)}")


def _check_observables(og, path, batch_dims, report):
    if not isinstance(og, h5py.Group):
        report.error(path, "observables must be a group")
        return
    ctx = _ObservablesContext(batch_dims)

    top_pg_names = []
    for name, item in og.items():
        p = f"{path}/{name}"
        if name.startswith(MULTI_LOCATION_GROUP):
            if not is_multi_location_group_name(name):
                report.error(p, f"names starting with '{MULTI_LOCATION_GROUP}' are reserved for multi-location groups "
                                f"('{MULTI_LOCATION_GROUP}' or '{MULTI_LOCATION_GROUP}_<suffix>')")
            elif isinstance(item, h5py.Group) and not _is_particle_group(item):
                _check_multi_location(item, p, ctx, report)
            else:
                report.error(p, "multi-location entries must be groups")
        elif isinstance(item, h5py.Dataset):
            _check_dataset(item, p, ctx, "top", report=report)
        elif _is_particle_group(item):
            _check_particle_group(item, p, True, None, report)
            top_pg_names.append(name)
        else:
            # Location group
            members = list(item.items())
            if not members:
                report.warn(p, "empty location group")
                continue
            n_datasets = 0
            pg_names = []
            for sub_name, sub in members:
                sp = f"{p}/{sub_name}"
                if isinstance(sub, h5py.Dataset):
                    _check_dataset(sub, sp, ctx, "nested", expected_location=name, report=report)
                    n_datasets += 1
                elif _is_particle_group(sub):
                    _check_particle_group(sub, sp, False, name, report)
                    pg_names.append(sub_name)
                else:
                    report.error(sp, "unexpected nested group (location groups may only contain observables)")
            pg_observables = _group_particle_groups(p, pg_names, batch_dims, report)
            if n_datasets + len(pg_observables) == 1 and all(n == 1 for n in pg_observables.values()):
                report.warn(p, "location group holds a single observable stored as one entry; since 0.1.2 it should "
                               "be stored directly under observables/ with an @location attribute")

    for base, n_entries in _group_particle_groups(path, top_pg_names, batch_dims, report).items():
        if n_entries > 1:
            report.warn(f"{path}/{base}_*", "per-shot ParticleGroups should be stored in an observables/<location>/ "
                                            "group (early 0.1.2 layout)")

    if len(ctx.timestamps) > 1:
        report.error(path, f"at most one {TIMESTAMP_NAME} observable is allowed, found {len(ctx.timestamps)}")

    if batch_dims is not None:
        for ref_path, key in ctx.bin_size_refs:
            shapes = ctx.dataset_shapes.get(key)
            if not shapes:
                report.error(ref_path, f"@bin_size key '{key}' does not match any observable")
            elif not any(is_broadcastable_batch_shape(shape, batch_dims) for shape in shapes):
                report.error(ref_path, f"@bin_size key '{key}' must reference data with dimension batch_dims={batch_dims} "
                                       f"(size-1 dimensions allowed), got {shapes}")


def _check_datapoint(g, path, report, root_version=None, require_lattice=True):
    batch_dims = _check_datapoint_attrs(g, path, report, root_version=root_version)
    prefix = "" if path == "/" else path
    if require_lattice:
        if "lattice" not in g:
            report.error(path, "missing lattice group")
        else:
            _check_lattice(g["lattice"], f"{prefix}/lattice", report)
    elif "lattice" in g:
        report.error(f"{prefix}/lattice", "combined files must store the lattice once at the root")
    if "observables" not in g:
        report.error(path, "missing observables group")
    else:
        _check_observables(g["observables"], f"{prefix}/observables", batch_dims, report)
    for name in g.keys():
        if name not in ("lattice", "observables"):
            report.warn(f"{prefix}/{name}", "unexpected entry in data point")


def _check_combined(f, report):
    root_version = None
    if "Data_Standard_Version" not in f.attrs:
        report.error("/", "missing @Data_Standard_Version")
    else:
        root_version = _check_version(f.attrs["Data_Standard_Version"], "/", report)

    ids = [_decode(x) for x in np.atleast_1d(np.asarray(f.attrs["IDs"], dtype=object))]
    if any(not isinstance(i, str) or i == "" for i in ids):
        report.error("/", "@IDs must be non-empty strings")
    if len(ids) != len(set(ids)):
        report.error("/", "@IDs contains duplicates")

    if "lattice" not in f:
        report.error("/", "missing root lattice group")
    else:
        _check_lattice(f["lattice"], "/lattice", report)

    groups = [k for k in f.keys() if k != "lattice"]
    for missing in sorted(set(ids) - set(groups)):
        report.error("/", f"ID '{missing}' listed in @IDs has no group")
    for extra in sorted(set(groups) - set(ids)):
        report.error(f"/{extra}", "group is not listed in root @IDs")

    for id_ in ids:
        if id_ not in f or not isinstance(f[id_], h5py.Group):
            continue
        g = f[id_]
        _check_datapoint(g, f"/{id_}", report, root_version=root_version, require_lattice=False)
        if "ID" in g.attrs and _decode(g.attrs["ID"]) != id_:
            report.warn(f"/{id_}", f"@ID '{_decode(g.attrs['ID'])}' differs from group name")


def validate_file(path, strict=False, raise_on_error=False):
    """
    Validate a Data Standard HDF5 file.

    Args:
        path (str or path-like): HDF5 file (single data point or combined file).
        strict (bool): Treat warnings (legacy constructs) as errors.
        raise_on_error (bool): Raise DataStandardValidationError if the file is invalid.

    Returns:
        ValidationReport
    """
    report = ValidationReport(target=str(path))
    try:
        f = h5py.File(path, "r")
    except OSError as e:
        report.error("/", f"cannot open HDF5 file: {e}")
    else:
        with f:
            if "IDs" in f.attrs:
                _check_combined(f, report)
            elif "observables" in f or "ID" in f.attrs:
                _check_datapoint(f, "/", report)
            else:
                report.error("/", "not a Data Standard file: expected root @IDs (combined file) "
                                  "or an observables/ group (single data point)")
    if strict and report.warnings:
        report.errors.extend(f"(strict) {w}" for w in report.warnings)
        report.warnings = []
    if raise_on_error:
        report.raise_if_errors()
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="data-standard-validate",
        description=f"Validate HDF5 files against the Data Standard (v{VERSION}).",
    )
    parser.add_argument("files", nargs="+", help="HDF5 files to validate")
    parser.add_argument("--strict", action="store_true", help="treat warnings (legacy constructs) as errors")
    parser.add_argument("-q", "--quiet", action="store_true", help="do not print warnings")
    args = parser.parse_args(argv)

    exit_code = 0
    for path in args.files:
        report = validate_file(path, strict=args.strict)
        print(report.format(show_warnings=not args.quiet))
        if not report.ok:
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
