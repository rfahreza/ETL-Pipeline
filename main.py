import argparse
import logging
import time
import traceback
from dotenv import load_dotenv

from utils.extract import extract_all_products
from utils.transform import transform_data
from utils.load import load_data

load_dotenv()

logger = logging.getLogger(__name__)


def etl_pipeline(base_url: str, max_pages: int) -> bool:
    start_time = time.time()
    logger.info(f"Starting ETL pipeline for {base_url} with {max_pages} pages")

    try:
        raw_data = extract_all_products(base_url, max_pages)
        if not raw_data:
            logger.error("Extraction failed: No data extracted")
            logger.debug("URL: {}".format(base_url))
            logger.debug("Max pages: {}".format(max_pages))

        transformed_data = transform_data(raw_data)
        if transformed_data.empty:
            logger.error(
                "Transformation failed: No valid data after transformation")
            return False

        load_success = load_data(transformed_data)
        if load_success:
            logger.info("Data successfully loaded.")
            return True
        else:
            logger.warning("Issues encountered during loading phase.")
            return False

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        logger.debug(traceback.format_exc())
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Fashion Products ETL Pipeline')
    parser.add_argument('--url', default='https://fashion-studio.dicoding.dev',
                        help='Base URL of the fashion website')
    parser.add_argument('--pages', type=int, default=50,
                        help='Maximum number of pages to scrape')
    args = parser.parse_args()

    success = etl_pipeline(args.url, args.pages)
    if success:
        print("ETL pipeline completed successfully!")
    else:
        print("ETL pipeline encountered errors. Check the logs for details.")


if __name__ == "__main__":
    main()
