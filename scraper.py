import requests
from bs4 import BeautifulSoup

class WebScraper:
  """A class for basic web scraping functionalities.

  This class provides methods to fetch a webpage, parse its HTML content,
  and extract data based on provided selectors.
  """

  def __init__(self, base_url):
    """Initializes the scraper with a base URL.

    Args:
        base_url: The base URL of the website to scrape.
    """
    self.base_url = base_url

  def fetch_page(self, url):
    """Fetches the content of a webpage.

    Args:
        url: The URL of the specific page to fetch (relative to base_url if not provided with full path).

    Returns:
        The HTML content of the webpage as a string, or None on error.
    """
    try:
      if not url.startswith('http'):
        url = f"{self.base_url}/{url}"  # Append base_url if needed
      response = requests.get(url)
      response.raise_for_status()  # Raise exception for unsuccessful requests
      return response.content
    except requests.exceptions.RequestException as e:
      print(f"Error fetching page: {url} - {e}")
      return None

  def parse_html(self, html_content):
    """Parses the HTML content using BeautifulSoup.

    Args:
        html_content: The HTML content of a webpage as a string.

    Returns:
        A BeautifulSoup object representing the parsed HTML structure.
    """
    return BeautifulSoup(html_content, 'html.parser')

  def extract_data(self, soup, selectors):
    """Extracts data from the parsed HTML based on provided selectors.

    Args:
        soup: A BeautifulSoup object representing the parsed HTML.
        selectors: A dictionary where keys are data names and values are
                    CSS selectors for locating the corresponding data elements.

    Returns:
        A dictionary containing the extracted data with keys matching the selector names.
    """
    extracted_data = {}
    for data_name, selector in selectors.items():
      elements = soup.select(selector)
      if elements:
        # Extract data based on element type (text, attributes, etc.)
        if len(elements) == 1:
          extracted_data[data_name] = elements[0].text.strip()
        else:
          extracted_data[data_name] = [element.text.strip() for element in elements]
      else:
        extracted_data[data_name] = None  # Handle cases where element is not found
    return extracted_data

  def scrape(self, url, selectors):
    """Scrapes data from a specific webpage URL using provided selectors.

    Args:
        url: The URL of the webpage to scrape (relative to base_url if not provided with full path).
        selectors: A dictionary defining the data to extract and their corresponding CSS selectors.

    Returns:
        A dictionary containing the extracted data from the webpage, or None on error.
    """
    html_content = self.fetch_page(url)
    if html_content:
      soup = self.parse_html(html_content)
      return self.extract_data(soup, selectors)
    else:
      return None


# Example usage
scraper = WebScraper('https://www.example.com')  # Replace with your base URL

# Define selectors for data you want to extract (replace with your actual selectors)
selectors = {
  'title': 'h1.page-title',
  'description': 'p.page-description'
}

scraped_data = scraper.scrape('about-us', selectors)
if scraped_data:
  print(scraped_data)
else:
  print("Scraping failed!")
