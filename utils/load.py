import os
import csv
import logging
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2 import extras, sql
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def save_to_csv(data: pd.DataFrame, filename: str) -> bool:
    try:
        data.to_csv(filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        logger.info(f"Data successfully saved to CSV: {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        return False


def get_google_sheets_service(credentials_file: str, scopes: list):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        credentials_file, scopes)
    return build('sheets', 'v4', credentials=credentials)


def dataframe_to_sheets_values(df: pd.DataFrame):
    return [df.columns.tolist()] + df.values.tolist()


def col_letter(n: int) -> str:
    result = ''
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def save_to_google_sheets(data: pd.DataFrame) -> bool:
    SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_SHEET_CREDENTIALS_PATH')
    SPREADSHEET_ID = os.getenv('GOOGLE_SHEET_ID')
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SHEET_NAME = 'fashion'

    if not SERVICE_ACCOUNT_FILE or not SPREADSHEET_ID:
        logger.error(
            "Variabel lingkungan GOOGLE_SHEET_CREDENTIALS_PATH atau GOOGLE_SHEET_ID tidak ditemukan.")
        return False

    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        logger.error(
            f"Google Sheets credentials file not found at {SERVICE_ACCOUNT_FILE}")
        return False

    try:
        # Make a copy to avoid modifying the original DataFrame
        data_to_load = data.copy()
        # Convert datetime objects to strings for JSON serialization
        if 'timestamp' in data_to_load.columns:
            data_to_load['timestamp'] = data_to_load['timestamp'].astype(str)

        service = get_google_sheets_service(SERVICE_ACCOUNT_FILE, SCOPES)
        sheet = service.spreadsheets()
        values = dataframe_to_sheets_values(data_to_load)
        total_rows, total_cols = len(values), len(values[0])
        range_name = f"{SHEET_NAME}!A1:{col_letter(total_cols)}{total_rows}"
        body = {'values': values}
        result = sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        logger.info(
            f"Successfully updated {result.get('updatedCells')} cells to Google Sheet.")
        return True
    except Exception as e:
        logger.error(f"Error saving to Google Sheets: {e}")
        return False


def get_postgres_connection():
    params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'password': os.getenv('DB_PASSWORD')
    }
    if not all(params.values()):
        logger.error(
            "Satu atau lebih variabel koneksi database (DB_NAME, DB_USER, dll.) tidak ditemukan.")
        raise ValueError(
            "Variabel koneksi database tidak lengkap. Periksa file .env atau environment Anda.")
    return psycopg2.connect(**params)


def create_table_if_not_exists(cursor):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS fashion_products (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        price NUMERIC NOT NULL,
        rating NUMERIC,
        colors INTEGER,
        size VARCHAR(50),
        gender VARCHAR(50) NOT NULL,
        timestamp TIMESTAMP NOT NULL
    )
    '''
    cursor.execute(create_table_query)


def save_to_postgresql(data: pd.DataFrame) -> bool:
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                create_table_if_not_exists(cursor)

                values = [tuple(x) for x in data.to_numpy()]
                columns = data.columns.tolist()

                insert_query = sql.SQL("INSERT INTO fashion_products ({}) VALUES %s").format(
                    sql.SQL(', ').join(map(sql.Identifier, columns))
                )

                extras.execute_values(
                    cursor, insert_query.as_string(cursor), values)

        logger.info(
            "Data successfully saved to PostgreSQL table: fashion_products")
        return True
    except (psycopg2.Error, Exception) as e:
        logger.error(f"Database connection or operation error: {e}")
        return False


def load_data(data: pd.DataFrame) -> bool:
    if data.empty:
        logger.warning("No data to load")
        return False

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    success = True

    try:
        if not save_to_csv(data, f"products_{timestamp}.csv"):
            logger.warning("Failed to save data to CSV")
            success = False
    except Exception as e:
        logger.error(f"Unhandled exception during CSV save: {e}")
        success = False

    try:
        if not save_to_google_sheets(data):
            logger.warning("Failed to save data to Google Sheets")
            success = False
    except Exception as e:
        logger.error(f"Unhandled exception during Google Sheets save: {e}")
        success = False

    try:
        if not save_to_postgresql(data):
            logger.warning("Failed to save data to PostgreSQL")
            success = False
    except Exception as e:
        logger.error(f"Unhandled exception during PostgreSQL save: {e}")
        success = False

    return success
