"""
Smoke tests for data_standard package imports and public API.

These tests verify that:
- The package can be imported
- All public symbols are accessible
- Version information is available
- Core classes can be instantiated
"""
import pytest


def test_package_import():
    """Test that data_standard package can be imported."""
    import data_standard
    assert data_standard is not None


def test_version_available():
    """Test that __version__ is defined and is a string."""
    import data_standard
    assert hasattr(data_standard, '__version__')
    assert isinstance(data_standard.__version__, str)
    assert len(data_standard.__version__) > 0


def test_public_symbols_accessible():
    """Test that all public symbols listed in __all__ are importable."""
    import data_standard
    
    # Check that __all__ exists and is a list
    assert hasattr(data_standard, '__all__')
    assert isinstance(data_standard.__all__, list)
    
    # Verify all symbols in __all__ are accessible
    expected_symbols = [
        'DataPoint2',
        'SimulatedDataPoint2',
        'combine_files',
    ]
    
    for symbol in expected_symbols:
        assert symbol in data_standard.__all__, f"{symbol} not in __all__"
        assert hasattr(data_standard, symbol), f"{symbol} not accessible from package"


def test_datapoint2_import():
    """Test that DataPoint2 can be imported and instantiated."""
    from data_standard import DataPoint2
    
    # Verify it's a class
    assert isinstance(DataPoint2, type)
    
    # Create an instance
    dp = DataPoint2()
    assert dp is not None
    assert hasattr(dp, 'observables')
    assert hasattr(dp, 'lattice')
    assert hasattr(dp, 'summary')
    assert hasattr(dp, 'run_information')
    assert hasattr(dp, 'ID')


def test_simulated_datapoint2_import():
    """Test that SimulatedDataPoint2 can be imported and instantiated."""
    from data_standard import SimulatedDataPoint2
    
    # Verify it's a class
    assert isinstance(SimulatedDataPoint2, type)
    
    # Create an instance
    sdp = SimulatedDataPoint2()
    assert sdp is not None
    assert hasattr(sdp, 'observables')
    assert hasattr(sdp, 'lattice')
    assert hasattr(sdp, 'summary')
    assert hasattr(sdp, 'run_information')
    assert hasattr(sdp, 'simulation_metadata')
    assert hasattr(sdp, 'ID')


def test_combine_files_import():
    """Test that combine_files function can be imported."""
    from data_standard import combine_files
    
    # Verify it's a callable function
    assert callable(combine_files)


def test_direct_module_imports():
    """Test that classes can be imported directly from submodules."""
    from data_standard.Data_Standard_2 import DataPoint2, SimulatedDataPoint2
    from data_standard.Combine_Files import combine_files
    
    assert DataPoint2 is not None
    assert SimulatedDataPoint2 is not None
    assert combine_files is not None


def test_datapoint2_basic_api():
    """Test basic DataPoint2 API methods exist."""
    from data_standard import DataPoint2
    
    dp = DataPoint2()
    
    # Check that key methods exist
    required_methods = [
        'add_observable',
        'add_lattice',
        'add_run_information',
        'make_ID',
        'get_summary',
        'checker',
        'finalize',
        'saveHDF5',
    ]
    
    for method_name in required_methods:
        assert hasattr(dp, method_name), f"DataPoint2 missing method: {method_name}"
        assert callable(getattr(dp, method_name)), f"DataPoint2.{method_name} is not callable"


def test_simulated_datapoint2_extends_datapoint2():
    """Test that SimulatedDataPoint2 inherits from DataPoint2."""
    from data_standard import DataPoint2, SimulatedDataPoint2
    
    assert issubclass(SimulatedDataPoint2, DataPoint2)
    
    # Check that SimulatedDataPoint2 has additional method
    sdp = SimulatedDataPoint2()
    assert hasattr(sdp, 'add_simulation_data')
    assert callable(sdp.add_simulation_data)


def test_package_docstring():
    """Test that the package has a docstring."""
    import data_standard
    assert data_standard.__doc__ is not None
    assert len(data_standard.__doc__) > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
