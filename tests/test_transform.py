from utils.transform import transform_data
import pandas as pd
from datetime import datetime
import pytest


@pytest.fixture
def sample_data():
    """Provides sample data for transformation tests."""
    valid_raw_data = [
        {
            'title': 'Test Product 1',
            'price': '19.99',
            'rating': '4.5',
            'colors': '3',
            'size': 'M',
            'gender': 'Men'
        },
        {
            'title': 'Test Product 2',
            'price': '29.99',
            'rating': '3.8',
            'colors': '2',
            'size': 'L',
            'gender': 'Women'
        },
        {
            'title': 'Test Product 3',
            'price': '39.99',
            'rating': None,
            'colors': None,
            'size': None,
            'gender': 'Unisex'
        }
    ]

    invalid_raw_data = [
        {
            'title': 'Unknown Product',
            'price': 'invalid',
            'rating': 'not a number',
            'colors': 'many',
            'size': 'S',
            'gender': 'Men'
        },
        {
            'title': 'Missing Price',
            'price': None,
            'rating': '4.0',
            'colors': '5',
            'size': 'XL',
            'gender': 'Men'
        },
        {
            'title': 'Test Product 1',  # Duplicate
            'price': '19.99',
            'rating': '4.5',
            'colors': '3',
            'size': 'M',
            'gender': 'Men'
        }
    ]

    test_datetime = datetime(2023, 1, 1, 12, 0, 0)
    test_datetime_str = '2023-01-01 12:00:00'

    return {
        "valid_raw_data": valid_raw_data,
        "invalid_raw_data": invalid_raw_data,
        "test_datetime": test_datetime,
        "test_datetime_str": test_datetime_str
    }


def test_transform_data_valid_input(sample_data, mocker):
    """Test transform_data with valid input data."""
    mock_datetime = mocker.patch('utils.transform.datetime')
    mock_datetime.now.return_value = sample_data["test_datetime"]

    result = transform_data(sample_data["valid_raw_data"])

    assert isinstance(result, pd.DataFrame)
    expected_columns = ['title', 'price', 'rating',
                        'colors', 'size', 'gender', 'timestamp']
    assert list(result.columns) == expected_columns
    assert len(result) == 3
    assert result['price'].iloc[0] == 19.99 * 16000
    assert result['rating'].iloc[0] == 4.5
    assert result['colors'].iloc[0] == 3
    assert result['rating'].iloc[2] == 0.0
    assert result['colors'].iloc[2] == 1
    assert result['size'].iloc[2] == 'One Size'
    assert result['timestamp'].iloc[0] == sample_data["test_datetime_str"]


def test_transform_data_empty_and_none_input(mocker):
    """Test transform_data with empty or None input data."""
    mock_logger = mocker.patch('utils.transform.logger')

    # Test with empty list
    result_empty = transform_data([])
    assert isinstance(result_empty, pd.DataFrame)
    assert result_empty.empty

    # Test with None
    result_none = transform_data(None)
    assert isinstance(result_none, pd.DataFrame)
    assert result_none.empty

    # Assert warning was logged
    mock_logger.warning.assert_called_with("No data to transform")


def test_transform_data_invalid_input(sample_data, mocker):
    """Test transform_data with invalid and duplicate data."""
    mock_datetime = mocker.patch('utils.transform.datetime')
    mock_datetime.now.return_value = sample_data["test_datetime"]

    mixed_data = sample_data["valid_raw_data"] + \
        sample_data["invalid_raw_data"]
    result = transform_data(mixed_data)

    assert isinstance(result, pd.DataFrame)

    assert len(result) == 4
    assert 'Unknown Product' not in result['title'].values
    assert 'Missing Price' in result['title'].values


def test_transform_data_exception_handling(sample_data, mocker):
    """Test transform_data exception handling during DataFrame creation."""
    mock_logger = mocker.patch('utils.transform.logger')
    # Mock a specific operation inside the try block to cause an exception
    mocker.patch('pandas.DataFrame.reindex',
                 side_effect=Exception("Test reindex error"))

    result = transform_data(sample_data["valid_raw_data"])

    # Check that the generic error message is logged
    mock_logger.error.assert_called_with(
        "An error occurred during data transformation: Test reindex error")
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_transform_data_missing_columns(sample_data, mocker):
    """Test transform_data with data having missing columns."""
    mock_datetime = mocker.patch('utils.transform.datetime')
    mock_datetime.now.return_value = sample_data["test_datetime"]

    incomplete_data = [
        {
            'title': 'Test Product 1',
            'price': '19.99',
            'gender': 'Men'
            # Missing rating, colors, size
        }
    ]

    result = transform_data(incomplete_data)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert 'rating' in result.columns
    assert 'colors' in result.columns
    assert 'size' in result.columns
    assert result['rating'].iloc[0] == 0.0
    assert result['colors'].iloc[0] == 1
    assert result['size'].iloc[0] == 'One Size'


def test_transform_data_error_during_deduplication(sample_data, mocker):
    """Test transform_data with an error during the deduplication step."""
    mocker.patch('utils.transform.datetime')
    mock_logger = mocker.patch('utils.transform.logger')
    mocker.patch('utils.transform.pd.DataFrame.drop_duplicates',
                 side_effect=Exception("Duplication error"))

    result = transform_data(sample_data["valid_raw_data"])

    # Assert that the correct, generic error message is logged
    mock_logger.error.assert_called_with(
        "An error occurred during data transformation: Duplication error")
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_transform_data_column_selection(mocker):
    """Test that transform_data selects and orders columns correctly."""
    mocker.patch('utils.transform.datetime')
    extra_columns_data = [
        {
            'title': 'Test Product 1',
            'price': '19.99',
            'rating': '4.5',
            'colors': '3',
            'size': 'M',
            'gender': 'Men',
            'extra_column1': 'Should be removed',
            'extra_column2': 'Also should be removed'
        }
    ]

    result = transform_data(extra_columns_data)

    assert isinstance(result, pd.DataFrame)
    expected_columns = ['title', 'price', 'rating',
                        'colors', 'size', 'gender', 'timestamp']
    assert list(result.columns) == expected_columns
