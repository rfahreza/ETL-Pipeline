import requests
from bs4 import BeautifulSoup
import logging
import time
import random

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_price(price_text):
    """Convert price string to float."""
    try:
        price_text = price_text.replace('$', '').replace(',', '').strip()
        return float(price_text)
    except Exception:
        return None


def parse_rating(detail_text):
    """Extract and convert rating from detail text."""
    try:
        rating_text = detail_text.split('⭐')[1].split('/')[0].strip()
        return None if rating_text == 'Invalid Rating' else float(rating_text)
    except Exception:
        return None


def parse_colors(detail_text):
    """Extract and convert colors from detail text."""
    try:
        return int(detail_text.split()[0])
    except Exception:
        return None


def parse_product_card(card):
    """Extract product info from a single product card."""
    product = {}
    # Image
    img_tag = card.select_one('img.collection-image')
    if img_tag:
        product['image_url'] = img_tag.get('src', '')
        product['product_alt'] = img_tag.get('alt', '')
    # Title
    title_tag = card.select_one('h3.product-title')
    if title_tag:
        product['title'] = title_tag.text.strip()
    # Price
    price_tag = card.select_one('span.price')
    if price_tag:
        product['price'] = parse_price(price_tag.text)
    # Details
    for detail in card.select('div.product-details p'):
        text = detail.text.strip()
        if 'Rating:' in text:
            product['rating'] = parse_rating(text)
        elif 'Colors' in text:
            product['colors'] = parse_colors(text)
        elif 'Size:' in text:
            product['size'] = text.replace('Size:', '').strip()
        elif 'Gender:' in text:
            product['gender'] = text.replace('Gender:', '').strip()
    return product


def extract_products_from_html(html_content):
    """Extract all products from HTML content."""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        cards = soup.select('div.collection-card')
        products = []
        for card in cards:
            product = parse_product_card(card)
            if product.get('title'):  # Ensure product has at least a title
                products.append(product)
        return products
    except Exception as e:
        logger.error(f"Error parsing HTML content: {e}")
        return []


def fetch_html(url):
    """Fetch HTML content from a URL."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.text
        logger.error(
            f"Failed to fetch page: {url}, Status code: {response.status_code}")
    except Exception as e:
        logger.error(f"Error fetching page {url}: {e}")
    return None


def extract_all_products(base_url, max_pages=50):
    """Extract product data from all pages."""
    all_products = []
    for page_num in range(1, max_pages + 1):
        url = base_url if page_num == 1 else f"{base_url}/page{page_num}"
        logger.info(f"Fetching page {page_num}: {url}")
        time.sleep(random.uniform(1.0, 3.0))
        html = fetch_html(url)
        if html:
            products = extract_products_from_html(html)
            all_products.extend(products)
            logger.info(
                f"Extracted {len(products)} products from page {page_num}")
        else:
            logger.warning(
                f"Failed to fetch page {page_num}, stopping extraction")
            break
    logger.info(f"Total products extracted: {len(all_products)}")
    return all_products
