from utils.load import save_to_csv, save_to_google_sheets, save_to_postgresql, load_data
import os
import pandas as pd
import csv
from datetime import datetime
import pytest


@pytest.fixture
def sample_dataframe():
    """Provides a sample DataFrame for testing load functions."""
    return pd.DataFrame({
        'title': ['Test Product 1', 'Test Product 2'],
        'price': [19.99, 29.99],
        'rating': [4.5, 3.8],
        'colors': [3, 2],
        'size': ['M', 'L'],
        'gender': ['Men', 'Women'],
        'timestamp': [datetime.now(), datetime.now()]
    })


def test_save_to_csv_success(mocker, sample_dataframe):
    """Test successful CSV saving."""
    mock_logger = mocker.patch('utils.load.logger')
    mock_to_csv = mocker.patch('pandas.DataFrame.to_csv')

    result = save_to_csv(sample_dataframe, 'test_output.csv')

    mock_to_csv.assert_called_once_with(
        'test_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC
    )
    mock_logger.info.assert_called_once()
    assert result is True


def test_save_to_csv_failure(mocker, sample_dataframe):
    """Test CSV saving with failure."""
    mock_logger = mocker.patch('utils.load.logger')
    mocker.patch('pandas.DataFrame.to_csv',
                 side_effect=Exception('Test exception'))

    result = save_to_csv(sample_dataframe, 'test_output.csv')

    mock_logger.error.assert_called_once()
    assert result is False


def test_save_to_google_sheets_success(mocker, sample_dataframe):
    """Test successful Google Sheets saving."""
    mocker.patch('utils.load.os.path.exists', return_value=True)
    mocker.patch('utils.load.ServiceAccountCredentials.from_json_keyfile_name')
    mock_build = mocker.patch('utils.load.build')
    mock_logger = mocker.patch('utils.load.logger')

    # Mock the chained calls for the Google Sheets API
    mock_build.return_value.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {
        'updatedCells': 14}

    result = save_to_google_sheets(sample_dataframe)

    mock_build.assert_called_once()
    mock_logger.info.assert_called_once()
    assert result is True


def test_save_to_google_sheets_missing_credentials(mocker, sample_dataframe):
    """Test Google Sheets saving with missing credentials file."""
    mocker.patch('utils.load.os.path.exists', return_value=False)
    mock_logger = mocker.patch('utils.load.logger')

    result = save_to_google_sheets(sample_dataframe)

    mock_logger.error.assert_called_once()
    assert result is False


def test_save_to_google_sheets_api_error(mocker, sample_dataframe):
    """Test Google Sheets saving with API error."""
    mocker.patch('utils.load.os.path.exists', return_value=True)
    mocker.patch('utils.load.ServiceAccountCredentials.from_json_keyfile_name')
    mocker.patch('utils.load.build', side_effect=Exception('API Error'))
    mock_logger = mocker.patch('utils.load.logger')

    result = save_to_google_sheets(sample_dataframe)

    mock_logger.error.assert_called_once()
    assert result is False


def test_save_to_postgresql_success(mocker, sample_dataframe):
    """Test successful PostgreSQL saving."""
    mock_logger = mocker.patch('utils.load.logger')
    mock_get_conn = mocker.patch('utils.load.get_postgres_connection')
    mock_create_table = mocker.patch('utils.load.create_table_if_not_exists')
    mock_execute_values = mocker.patch('utils.load.extras.execute_values')

    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_sql_builder = mocker.patch('utils.load.sql')

    result = save_to_postgresql(sample_dataframe)

    mock_get_conn.assert_called_once()
    mock_create_table.assert_called_once()
    mock_execute_values.assert_called_once()
    mock_logger.info.assert_called_once()
    assert result is True


def test_save_to_postgresql_connection_error(mocker, sample_dataframe):
    """Test PostgreSQL saving with connection error."""
    mock_logger = mocker.patch('utils.load.logger')
    mocker.patch('utils.load.get_postgres_connection',
                 side_effect=Exception('Connection error'))

    result = save_to_postgresql(sample_dataframe)

    mock_logger.error.assert_called()
    assert result is False


def test_load_data_all_success(mocker, sample_dataframe):
    """Test load_data with all destinations succeeding."""
    mocker.patch('utils.load.datetime')
    mock_csv = mocker.patch('utils.load.save_to_csv', return_value=True)
    mock_sheets = mocker.patch(
        'utils.load.save_to_google_sheets', return_value=True)
    mock_pg = mocker.patch(
        'utils.load.save_to_postgresql', return_value=True)

    result = load_data(sample_dataframe)

    mock_csv.assert_called_once()
    mock_sheets.assert_called_once()
    mock_pg.assert_called_once()
    assert result is True


def test_load_data_partial_failure(mocker, sample_dataframe):
    """Test load_data with some destinations failing."""
    mocker.patch('utils.load.datetime')
    mock_logger = mocker.patch('utils.load.logger')
    mocker.patch('utils.load.save_to_csv', return_value=False)
    mocker.patch('utils.load.save_to_google_sheets', return_value=True)
    mocker.patch('utils.load.save_to_postgresql', return_value=True)

    result = load_data(sample_dataframe)

    mock_logger.warning.assert_called_with("Failed to save data to CSV")
    assert result is False


def test_load_data_empty_dataframe(mocker):
    """Test load_data with an empty DataFrame."""
    mock_logger = mocker.patch('utils.load.logger')
    empty_df = pd.DataFrame()
    result = load_data(empty_df)

    mock_logger.warning.assert_called_once_with("No data to load")
    assert result is False


def test_load_data_unhandled_exception(mocker, sample_dataframe):
    """Test load_data with an unhandled exception."""
    mocker.patch('utils.load.datetime')
    mock_logger = mocker.patch('utils.load.logger')
    mocker.patch('utils.load.save_to_csv',
                 side_effect=Exception('Unexpected error'))

    result = load_data(sample_dataframe)

    mock_logger.error.assert_called_once()
    assert result is False
