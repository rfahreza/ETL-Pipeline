from utils.extract import extract_all_products, fetch_html, extract_products_from_html
import requests
import pytest


@pytest.fixture
def sample_html_and_products():
    """Provides sample HTML and the expected extracted product data."""
    sample_html = """
        <div class="collection-card">
            <img class="collection-image" src="http://example.com/image.jpg" alt="Test Product">
            <h3 class="product-title">Test Product</h3>
            <span class="price">$99.99</span>
            <div class="product-details">
                <p>Rating: ⭐4.5/5</p>
                <p>3 Colors</p>
                <p>Size: Medium</p>
                <p>Gender: Unisex</p>
            </div>
        </div>
        <div class="collection-card">
            <img class="collection-image" src="http://example.com/image2.jpg" alt="Second Product">
            <h3 class="product-title">Second Product</h3>
            <span class="price">$199.99</span>
            <div class="product-details">
                <p>Rating: ⭐Invalid Rating/5</p>
                <p>1 Colors</p>
                <p>Size: Large</p>
                <p>Gender: Women</p>
            </div>
        </div>
    """
    expected_products = [
        {
            'image_url': 'http://example.com/image.jpg',
            'product_alt': 'Test Product',
            'title': 'Test Product',
            'price': 99.99,
            'rating': 4.5,
            'colors': 3,
            'size': 'Medium',
            'gender': 'Unisex'
        },
        {
            'image_url': 'http://example.com/image2.jpg',
            'product_alt': 'Second Product',
            'title': 'Second Product',
            'price': 199.99,
            'rating': None,
            'colors': 1,
            'size': 'Large',
            'gender': 'Women'
        }
    ]
    return sample_html, expected_products


def test_extract_products_from_html(sample_html_and_products):
    """Test the extract_products_from_html function."""
    sample_html, expected_products = sample_html_and_products
    result = extract_products_from_html(sample_html)

    assert len(result) == 2
    assert result[0]['title'] == expected_products[0]['title']
    assert result[0]['price'] == expected_products[0]['price']
    assert result[0]['rating'] == expected_products[0]['rating']
    assert result[1]['rating'] is None  # Check invalid rating parsing


def test_extract_products_from_html_empty_and_invalid():
    """Test extract_products_from_html with empty and invalid HTML."""
    assert extract_products_from_html("") == []
    assert extract_products_from_html(
        "<html><body>No products here</body></html>") == []


def test_extract_products_from_html_exception(mocker):
    """Test extract_products_from_html with HTML that would raise exceptions."""
    mocker.patch('bs4.BeautifulSoup', side_effect=Exception("Parsing error"))
    result = extract_products_from_html("<html><body>Bad HTML</body></html>")
    assert result == []


def test_fetch_html_success(mocker):
    """Test fetch_html when successful."""
    mock_get = mocker.patch('requests.get')
    mock_response = mocker.MagicMock()
    mock_response.status_code = 200
    mock_response.text = "HTML content"
    mock_get.return_value = mock_response

    result = fetch_html("http://example.com")
    assert result == "HTML content"
    mock_get.assert_called_once()
    _, kwargs = mock_get.call_args
    assert 'User-Agent' in kwargs['headers']


def test_fetch_html_failure(mocker):
    """Test fetch_html when HTTP request fails."""
    mock_get = mocker.patch('requests.get')
    mock_response = mocker.MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response

    result = fetch_html("http://example.com/nonexistent")
    assert result is None


def test_fetch_html_exception(mocker):
    """Test fetch_html when an exception occurs."""
    mocker.patch('requests.get',
                 side_effect=requests.exceptions.RequestException("Connection error"))
    result = fetch_html("http://example.com")
    assert result is None


def test_extract_all_products(mocker):
    """Test extract_all_products function."""
    mock_fetch_html = mocker.patch('utils.extract.fetch_html')
    mock_extract_products = mocker.patch(
        'utils.extract.extract_products_from_html')
    mocker.patch('time.sleep')  # To avoid actual delay in tests

    # Mock return values
    mock_fetch_html.side_effect = ["page1 content", "page2 content", None]
    mock_extract_products.side_effect = [
        [{'title': 'Product 1'}],
        [{'title': 'Product 2'}]
    ]

    result = extract_all_products("http://example.com", max_pages=3)

    assert len(result) == 2
    assert result[0]['title'] == 'Product 1'
    assert result[1]['title'] == 'Product 2'

    assert mock_fetch_html.call_count == 3
    mock_fetch_html.assert_any_call("http://example.com")
    mock_fetch_html.assert_any_call("http://example.com/page2")
    mock_fetch_html.assert_any_call("http://example.com/page3")

    assert mock_extract_products.call_count == 2
    mock_extract_products.assert_any_call("page1 content")
    mock_extract_products.assert_any_call("page2 content")


def test_extract_all_products_no_content(mocker):
    """Test extract_all_products when no content is fetched."""
    mock_fetch_html = mocker.patch(
        'utils.extract.fetch_html', return_value=None)
    mock_extract_products = mocker.patch(
        'utils.extract.extract_products_from_html')

    result = extract_all_products("http://example.com")

    assert result == []
    mock_extract_products.assert_not_called()
