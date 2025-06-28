import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def transform_data(raw_data):
    """
    Transform the raw product data into a cleaned and structured DataFrame.
    """
    logger.info("Starting data transformation")

    expected_columns = ['title', 'price', 'rating',
                        'colors', 'size', 'gender', 'timestamp']

    if not raw_data or len(raw_data) == 0:
        logger.warning("No data to transform")
        return pd.DataFrame(columns=expected_columns)

    try:
        df = pd.DataFrame(raw_data)

        # Ensure all expected columns exist right after creation, filling missing ones with NaN
        df = df.reindex(columns=expected_columns)

        # --- Data Cleaning and Transformation ---
        df['title'] = df['title'].str.strip()
        df['gender'] = df['gender'].str.strip()
        df = df.dropna(subset=['title', 'gender'])
        df = df[df['title'] != 'Unknown Product']

        df['price'] = pd.to_numeric(
            df['price'], errors='coerce').fillna(0) * 16000

        df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0.0)
        df['colors'] = pd.to_numeric(
            df['colors'], errors='coerce').fillna(1).astype(int)

        df['size'] = df['size'].fillna('One Size')

        df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Drop duplicates after all cleaning
        df = df.drop_duplicates(subset=['title', 'price', 'size', 'gender'])

        logger.info(f"Final data shape: {df.shape}")
        return df

    except Exception as e:
        logger.error(f"An error occurred during data transformation: {e}")
        return pd.DataFrame(columns=expected_columns)
