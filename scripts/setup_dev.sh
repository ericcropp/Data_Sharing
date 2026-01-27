#!/bin/bash
# Development environment setup script for data_standard

set -e

echo "==================================="
echo "Data Standard Development Setup"
echo "==================================="

# Check if conda/mamba is available
if command -v mamba &> /dev/null; then
    CONDA_CMD="mamba"
    echo "✓ Using mamba"
elif command -v conda &> /dev/null; then
    CONDA_CMD="conda"
    echo "✓ Using conda"
else
    echo "✗ Neither conda nor mamba found. Please install Miniconda or Anaconda."
    exit 1
fi

# Create environment
echo ""
echo "Creating development environment..."
$CONDA_CMD env create -f environment.dev.yml

echo ""
echo "Activating environment..."
eval "$($CONDA_CMD shell.bash hook)"
$CONDA_CMD activate data_standard_dev

# Install package in editable mode
echo ""
echo "Installing package in editable mode..."
pip install -e .

# Install pre-commit hooks
echo ""
echo "Installing pre-commit hooks..."
pre-commit install

echo ""
echo "==================================="
echo "✓ Setup complete!"
echo "==================================="
echo ""
echo "To activate the environment, run:"
echo "  conda activate data_standard_dev"
echo ""
echo "To run tests:"
echo "  pytest tests/ -v"
echo ""
echo "Pre-commit hooks are now installed and will run automatically on commit."
echo "To run manually: pre-commit run --all-files"
