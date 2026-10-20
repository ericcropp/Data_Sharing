"""Command-line entry point: ``python -m data_standard <file.h5> [...]`` validates HDF5 files."""
import sys

from .validate import main

if __name__ == "__main__":
    sys.exit(main())
