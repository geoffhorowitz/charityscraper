import requests
import time
from bs4 import BeautifulSoup
from database import DatabaseManager
import logging
import os
import json
import re
import csv
from tqdm import tqdm
import random

# Setup logging to file
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

class CharityScraper:
  """A class for web scraping functionalities, specialized for Charity Navigator.

  This class provides methods to fetch charity pages from Charity Navigator,
  parse their HTML content, and extract specific data points.
  """

  def __init__(self, base_url="https://www.charitynavigator.org"):
    """Initializes the scraper with a base URL.

    Args:
        base_url: The base URL of the website to scrape.
    """
    self.base_url = base_url
    self.session = requests.Session()
    # It's good practice to set a User-Agent
    self.session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })

  def _log(self, message):
    #print(message)
    logging.info(message)

  def fetch_page(self, url, save_to_file=None):
    """Fetches the content of a webpage and optionally saves it to a file.

    Args:
        url: The URL of the specific page to fetch (relative to base_url if not provided with full path).
        save_to_file: Optional path to save the response content.

    Returns:
        The HTML content of the webpage as bytes, or None on error.
    """
    try:
      if not url.startswith('https'):
        url = f"{self.base_url}{url}" 
      self._log(url)
      response = self.session.get(url, timeout=15)
      response.raise_for_status()
      #self._log(f"Fetched {len(response.content)} bytes")
      if save_to_file:
        with open(save_to_file, 'wb') as f:
          f.write(response.content)
        self._log(f"Saved response content to {save_to_file}")
      return response.content
    except requests.exceptions.RequestException as e:
      self._log(f"Error fetching page: {url} - {e}")
      return None

  def parse_html(self, html_content, save_to_file=None):
    """Parses the HTML content using BeautifulSoup.

    Args:
        html_content: The HTML content of a webpage as a string.

    Returns:
        A BeautifulSoup object representing the parsed HTML structure.
    """
    bs_obj = BeautifulSoup(html_content, 'html.parser')
    if save_to_file:
      with open(save_to_file, 'wb') as f:
        f.write(bs_obj.prettify('utf-8'))
      self._log(f"Saved response content to {save_to_file}")
    
    return bs_obj
  
  def _convert_string_for_json(self, json_string_input):
    json_string_input = json_string_input.replace('"$undefined"', 'null')
    json_string_input = json_string_input.replace('"true"', 'true')
    json_string_input = json_string_input.replace('"false"', 'false')

    # Replace any non-breaking spaces (\xa0) with regular spaces, and remove extra spaces
    #json_string_input = json_string_input.replace('\xa0', ' ').replace('  ', ' ')
    #json_string_input = re.sub(r'"\$D([^"]*)"', r'"\1"', json_string_input)     # Fix malformed date strings that look like "$D2023-06-30..."
    #json_string_input = json_string_input.replace("“", '"').replace("”", '"').replace("’", "'")     # Replace smart quotes if present
    #json_string_input = re.sub(r',\s*([\]}])', r'\1', json_string_input)     # Remove trailing commas (invalid in JSON)
    #json_string_input = json_string_input.encode('utf-8').decode('unicode_escape') # Unescape double-escaped quotes if needed

    #json_string_input = json_string_input.replace('null', 'None') # For Python's json.loads to recognize null as None
    #json_string_input = json_string_input.replace('true', 'True') # For Python's json.loads to recognize true as True
    #json_string_input = json_string_input.replace('false', 'False') # For Python's json.loads to recognize false as False
    #json_string_input = json_string_input.replace('"', "'")
    
    if json_string_input.endswith(','):
       json_string_input = json_string_input[:-1]

    if json_string_input.startswith('"') and json_string_input.endswith('"'):
        json_string_input = json_string_input[1:-1]
    
    json_string_input = '{' + f'{json_string_input}' + '}'
    #json_string_input = f"'{json_string_input}'"

    #self._log(json_string_input)

    return json.loads(json_string_input)
  
  def _convert_individual_entry_for_json(self, entry_string_input):
    entry_string_input = entry_string_input.replace('"$undefined"', 'null')
    entry_string_input = entry_string_input.replace('"true"', 'true')
    entry_string_input = entry_string_input.replace('"false"', 'false')

    if entry_string_input.endswith(','):
       entry_string_input = entry_string_input[:-1]

    entry_string_input = '{' + f'{entry_string_input}' + '}'

    #self._log(entry_string_input)

    return json.loads(entry_string_input)

  def _extract_charity_details(self, soup):
    """Extracts specific details from a charity's page soup.

    Args:
        soup: A BeautifulSoup object of the charity's page.

    Returns:
        A dictionary containing the extracted data.
    """
    details = {}

    # Json data contains the name, website, nonprofit status, and rating
    json_script = soup.find("script", {"type": "application/ld+json"})
    if json_script:
        try:
          data = json.loads(json_script.string)
          details['name'] = data.get("name", {})
          details['website'] = data.get("url", {})
          details['nonprofitStatus'] = data.get("nonprofitStatus", {})
          review = data.get("review")
          if review and isinstance(review, dict):
              review_rating = review.get("reviewRating", {})
              if review_rating and isinstance(review_rating, dict):
                  details['review'] = review_rating.get("ratingValue", {})
              else:
                  details['review'] = None
          else:
              details['review'] = None
        except Exception as e:
          self._log(f"Error parsing ld+json: {e}")
          details['name'], details['website'], details['review'] = None, None, None
    else:
        self._log("json_script not found.")
        details['name'], details['website'], details['review'] = None, None, None

    # Org details script has Physical Address, Phone Number, and Mission Statement (also name, ein, etc, but we already have that)
    script_tags = soup.find_all("script")
    #org_details_json = None
    name_json = None
    address_json = None
    website_json = None
    phone_json = None
    mission_json = None
    causes_json = None
    score_json = None

    for tag in script_tags:
        if tag.string and "orgDetails" in tag.string and "causes" in tag.string:
          if '\\' in tag.string:
            cleaned_tag = tag.string.replace('\\', '')
          else:
            cleaned_tag = tag.string
            
          # Extract the portion of the string that looks like JSON
          #match = re.search(r'"orgDetails":\{.*?\}', cleaned_tag)
          #try:
          #   if match and not org_details_json:
          #    org_details_json = self._convert_string_for_json(match.group())
              #if 'phone' not in org_details_json['orgDetails'] and 'mission' not in org_details_json['orgDetails']:
          #except:
            # due to greedy approach, regex doesn't always get all of the info
          #  json_str = match.group() + '}'
          #  org_details_json = self._convert_string_for_json(json_str)

          #print(org_details_json)

          match = re.search(r'"name":".*?",', cleaned_tag)
          if match and not name_json:
              name_json = self._convert_individual_entry_for_json(match.group())

          match = re.search(r'"url":"http.*?",', cleaned_tag)
          if match and not name_json:
              website_json = self._convert_individual_entry_for_json(match.group())

          match = re.search(r'"addressPhysical":\{.*?\},', cleaned_tag)
          #print(match.group())
          if match and not address_json:
              address_json = self._convert_string_for_json(match.group())

          match = re.search(r'"phone":".*?",', cleaned_tag)
          if match and not phone_json:
              phone_json = self._convert_individual_entry_for_json(match.group())

          match = re.search(r'"mission":".*?",', cleaned_tag)
          if match and not mission_json:
              mission_json = self._convert_individual_entry_for_json(match.group())
              
          match = re.search(r'"causes":\[(.*?)\]', cleaned_tag)
          if match and not causes_json:
              causes_json = self._convert_string_for_json(match.group())

          if tag.string and "ratingDetails" in tag.string and "score" in tag.string:
              match = re.search(r'"score":\s*(\d+)', cleaned_tag)
              if match and not score_json:
                  score_json = self._convert_individual_entry_for_json(match.group())

          if name_json and address_json and phone_json and mission_json and causes_json and score_json:
              break

    #if org_details_json:
    #    try:
    #        #org_details = org_details_json["orgDetails"]
    #        address_obj = address_json.get("addressPhysical", {})
    #        details['address'] = f"{address_obj.get('street','')} {address_obj.get('street2','')}, {address_obj.get('city','')} {address_obj.get('state','')} {address_obj.get('zip','')}"
    #        details['phone'] = phone_json.get("phone", {})
    #        details['mission'] = mission_json.get("mission", {})
    #    except Exception as e:
    #        self._log(f"Error extracting orgDetails fields: {e}")
    #        details['address'], details['phone'], details['mission'] = None, None, None
    #else:
    #    self._log("orgDetails block not found.")
    #    details['address'], details['phone'], details['mission'] = None, None, None

    if not details['website'] and website_json:
        details['website'] = website_json.get("url", {})
    else:
        details['website'] = None


    if address_json:
        address_obj = address_json.get("addressPhysical", {})
        details['address'] = f"{address_obj.get('street','')} {address_obj.get('street2','')}, {address_obj.get('city','')} {address_obj.get('state','')} {address_obj.get('zip','')}"
    else:
        details['address'] = None
        self._log("Address not found.")

    if phone_json:
        details['phone'] = phone_json.get("phone", {})
    else:
        details['phone'] = None
        self._log("Phone number not found.")

    if mission_json:
        details['mission'] = mission_json.get("mission", {})
    else:
        details['mission'] = None
        self._log("Mission statement not found.")



    # 2. Extract and print cause names
    if causes_json:
        #print(causes_json)
        causes_details = causes_json["causes"]
        cause_names = [c["name"] for c in causes_details]
        details['categories'] = ', '.join(cause_names)
    else:
        self._log("Causes not found.")

    # Rating - Look for a script that contains "ratingDetails" and "score"
    if score_json is not None:
        details['rating'] = score_json.get("score", {})
    else:
        details['rating'] = None
        self._log("Rating Score not found.")

    return details

  def scrape_and_store_charities(self, eins: list, db_manager: DatabaseManager = None, html_file: str = None):
    """Scrapes charity data for a list of EINs and stores it in the database.

    Args:
        eins: A list of charity EIN strings.
        db_manager: An instance of DatabaseManager.
        html_file: If provided, path to a txt file containing HTML to use for all EINs.
    """
    # Get all existing EINs in the database once
    existing_eins = set()
    if db_manager:
        rows = db_manager.fetch_data('charities', selection=['ein'])
        existing_eins = set(row['ein'] for row in rows if row.get('ein'))

    # Add tqdm progress bar here
    for ein in tqdm(eins, desc="Processing EINs"):
        try:
          if ein == "EIN": continue

          if db_manager and ein in existing_eins:
              self._log(f"EIN {ein} already exists in the database. Skipping.")
              continue

          self._log(f"Scraping data for EIN: {ein}...")
          if html_file: # for testing
              self._log(f"Reading HTML content from {html_file}")
              try:
                  with open(html_file, 'rb') as f:
                      html_content = f.read()
              except Exception as e:
                  self._log(f"Failed to read {html_file}: {e}")
                  continue
          else:
              url = f"/ein/{ein}"
              #html_content = self.fetch_page(url, save_to_file=os.path.join('html_cache', f"{ein}_html.txt"))
              html_content = self.fetch_page(url)

              if not html_content:
                  self._log(f"Failed to fetch/read page for EIN: {ein}. Skipping.")
                  continue

          #soup = self.parse_html(html_content, save_to_file=os.path.join('soup_cache', f"{ein}_soup.txt"))
          soup = self.parse_html(html_content)
          
          charity_data = self._extract_charity_details(soup)
          charity_data['ein'] = ein

          if charity_data.get('name'):
              if db_manager:
                db_manager.insert_data('charities', charity_data, on_conflict='REPLACE')
              else:
                self._log(str(charity_data))
              self._log(f"Successfully scraped and stored data for {charity_data['name']}.")
          else:
              self._log(f"Could not find required data for EIN: {ein}. Skipping database insertion.")

          time.sleep(random.uniform(1, 10)) # sleep for a random time between 1 and 10 seconds
        except Exception as e:
           self._log(f"an error occured for EIN {ein}: {e}")

def get_eins_from_input_csv(file_path):
    """
    Reads a CSV file and returns each line as an item (list of strings) in a list.

    Args:
        file_path (str): The path to the input CSV file.

    Returns:
        list: A list where each element is a list of strings representing a row from the CSV.
              Returns an empty list if the file is not found or an error occurs.
    """
    lines = []
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            for row in csv_reader:
                if row == "EIN": continue
                lines.append(", ".join(row))
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
    return lines

# Example usage
if __name__ == "__main__":
    #print(get_eins_from_input_csv("ein_data/combined_EIN_sample.csv"))

    db = DatabaseManager('charity_data.db')
    db.create_table(
      'charities',
      [
          {'name': 'name', 'data_type': 'TEXT'},
          {'name': 'rating', 'data_type': 'REAL'},
          {'name': 'ein', 'data_type': 'TEXT UNIQUE'},
          {'name': 'categories', 'data_type': 'TEXT'},
          {'name': 'website', 'data_type': 'TEXT'},
          {'name': 'address', 'data_type': 'TEXT'},
          {'name': 'phone', 'data_type': 'TEXT'},
          {'name': 'mission', 'data_type': 'TEXT'},
          {'name': 'review', 'data_type': 'TEXT'},
          {'name': 'nonprofitStatus', 'data_type': 'TEXT'}
      ]
    )

    scraper = CharityScraper()
    # Example EINs (BBYO, City Harvest)
    #eins_to_scrape = ['311794932', '133170676']
    #eins_to_scrape = get_eins_from_input_csv("ein_data/combined_EIN_sample.csv")
    eins_to_scrape = get_eins_from_input_csv("ein_data/combined_EIN_filtered_by_STATE.csv")
    scraper.scrape_and_store_charities(eins_to_scrape, db)
    #print("\nFetching all data from the database:")
    #all_data = db.fetch_data('charities')
    #for row in all_data:
    #    print(row)
    db.close()
    
    #scraper = CharityScraper()
    #scraper.scrape_and_store_charities(['311794932'], None, "soup_cache/311794932.txt")
