#!/usr/bin/env python3
"""
Merge selected HDF5 files into one, putting each *file* under its own group.

Features:
- Accept a list of files (CLI args, --files list.txt, or --files - for stdin)
- Optional mirroring of relative paths under a root (explicit --root or inferred)
- Copies *all* attributes (metadata), including file-level attrs onto each group's attrs
- Optional expansion of soft/external/object refs
"""

import argparse, os, sys, pathlib, re
import h5py

def copy_attrs(src_obj, dst_obj):
    for k, v in src_obj.attrs.items():
        dst_obj.attrs.create(k, v)

def copy_entire_file_into_group(src: h5py.File, dst_group: h5py.Group,
                                expand_soft=False, expand_external=False, expand_refs=False):
    # Copy file-level (root) attributes onto the group's attrs
    copy_attrs(src["/"], dst_group)
    # Copy all top-level members (datasets/groups/links) preserving structure & attributes
    for name in src["/"].keys():
        src.copy(src["/"][name], dst_group, name=name,
                 shallow=False,
                 expand_soft=expand_soft,
                 expand_external=expand_external,
                 expand_refs=expand_refs,
                 without_attrs=False)

def ensure_group(dst: h5py.File, path: str) -> h5py.Group:
    g = dst
    for part in [p for p in path.split("/") if p]:
        g = g.require_group(part)
    return g

def infer_common_root(paths):
    if not paths:
        return None
    try:
        return pathlib.Path(os.path.commonpath([str(pathlib.Path(p).resolve()) for p in paths]))
    except Exception:
        return None

def norm_paths(seq):
    out = []
    for s in seq:
        s = s.strip()
        if not s:
            continue
        out.append(str(pathlib.Path(s).expanduser()))
    return out

def read_file_list(file_arg):
    if file_arg == "-":
        return [line.rstrip("\n") for line in sys.stdin]
    else:
        with open(file_arg, "r", encoding="utf-8") as fh:
            return [line.rstrip("\n") for line in fh]

def compute_group_path(p: pathlib.Path, mirror: bool, root: pathlib.Path | None):
    if mirror and root:
        rel = p.resolve().relative_to(root)
        parts = list(rel.parts)
        # last part becomes stem (drop extension)
        parts[-1] = pathlib.Path(parts[-1]).stem
        return "/".join(parts)
    elif mirror:
        # If asked to mirror but no usable root, fall back to stem-only
        return p.stem
    else:
        return p.stem

def merge_selected(out_path, inputs, mirror=False, root=None,
                   expand_soft=False, expand_external=False, expand_refs=False):
    # Resolve/validate root if provided
    root_path = pathlib.Path(root).resolve() if root else None

    # If mirror is on and no root provided, try to infer a sensible one
    if mirror and root_path is None:
        inferred = infer_common_root(inputs)
        # Only use the inferred root if it’s an actual directory parent of all files
        if inferred and inferred.is_dir():
            root_path = inferred

    with h5py.File(out_path, "w") as dst:
        for in_path in inputs:
            p = pathlib.Path(in_path)
            if not p.is_file():
                print(f"[warn] Skipping missing file: {in_path}", file=sys.stderr)
                continue

            group_path = compute_group_path(p, mirror=mirror, root=root_path)
            # De-dup if needed
            base = group_path
            i = 2
            while group_path in dst:
                group_path = f"{base}_{i}"
                i += 1

            g = ensure_group(dst, group_path)
            with h5py.File(p, "r") as src:
                copy_entire_file_into_group(
                    src, g,
                    expand_soft=expand_soft,
                    expand_external=expand_external,
                    expand_refs=expand_refs
                )
            # provenance
            g.attrs["source_file"] = str(p.resolve())

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Merge specified HDF5 files into a single file with one group per source.")
    ap.add_argument("output", help="Path to output merged HDF5")
    ap.add_argument("inputs", nargs="*", help="Input HDF5 files (in addition to --files)")
    ap.add_argument("--files", help="Text file with one HDF5 path per line, or '-' to read from stdin")
    ap.add_argument("--mirror", action="store_true",
                    help="Mirror relative directory structure under a root (see --root). Without --root, a common ancestor is inferred when possible.")
    ap.add_argument("--root", help="Root directory to make inputs relative to when using --mirror")
    ap.add_argument("--expand-soft", action="store_true", help="Copy targets of soft links instead of links")
    ap.add_argument("--expand-external", action="store_true", help="Copy targets of external links instead of links")
    ap.add_argument("--expand-refs", action="store_true", help="Copy referenced objects (e.g., object/region refs)")
    args = ap.parse_args()

    listed = []
    if args.files:
        listed = read_file_list(args.files)
    all_inputs = norm_paths(list(set(listed + args.inputs)))

    if not all_inputs:
        sys.exit("No input files provided. Give files as args, or use --files list.txt, or --files - with stdin.")

    merge_selected(
        out_path=args.output,
        inputs=all_inputs,
        mirror=args.mirror,
        root=args.root,
        expand_soft=args.expand_soft,
        expand_external=args.expand_external,
        expand_refs=args.expand_refs,
    )
