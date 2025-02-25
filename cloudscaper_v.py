from googletrans import Translator
import time
import os
from google.cloud import language_v1
import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import tldextract
from urllib.parse import urlparse
from urllib3.exceptions import NameResolutionError
import logging
import random
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import requests  # Added for exception handling
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type  # Added for retry
# import validators  # Ensure validators is installed

# Configure logging with both file and console handlers
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Capture all levels of logs

# File handler to log all messages to 'web_scraping.log'
fh = logging.FileHandler('web_scraping.log')
fh.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(file_formatter)
logger.addHandler(fh)

# Console handler to display INFO and above messages
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)  # Change to DEBUG for more verbosity during troubleshooting
console_formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(console_formatter)
logger.addHandler(ch)

# Set the path to your Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\tiazh\Downloads\verizon-webscraping-test-fa33e5bd3c51.json"
scraper = cloudscraper.create_scraper()
# Chrome options for headless Selenium browsing
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--window-size=1920,1080")

def correct_url(url):
    """Ensure the URL has a scheme (http:// or https://)."""
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    return url

def translate_with_timeout(text, translator, timeout=10):
    """
    Translate text with a timeout to prevent hanging.
    
    Parameters:
        text (str): The text to translate.
        translator (Translator): An instance of googletrans.Translator.
        timeout (int): The maximum time (in seconds) to wait for the translation.
    
    Returns:
        str: Translated text if successful; original text if timeout or error occurs.
    """
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(translator.translate, text, src='auto', dest='en')
        try:
            result = future.result(timeout=timeout)
            return result.text[:9999]
        except TimeoutError:
            logger.error("Translation timed out.")
            return text[:9999]  # Return untranslated content
        except Exception as e:
            logger.error(f"Translation Error: {e}")
            return text[:9999]  # Return untranslated content

@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff: 2s, 4s, 8s
    stop=stop_after_attempt(3),  # Retry up to 3 times
    retry=retry_if_exception_type((
        cloudscraper.exceptions.CloudflareException,
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        TimeoutError
    )),
    reraise=True  # Re-raise the last exception if all retries fail
)

def fetch_web_content(url): 
    """
    Fetches and processes web content from the given URL.
    Falls back to metadata if main content is insufficient.
    
    Returns:
        Tuple[str or None, bool]: (Processed content or None, is_metadata_used)
    """
    headers = {
        'user-agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/115.0.0.0 Safari/537.36'
        ),
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

    # Define initial URL attempt and whether to try www
    url_attempted = url  # Start with the original URL

    try:
        parsed_url = urlparse(url_attempted)
        if not parsed_url.scheme:
            url_attempted = 'https://' + url_attempted  # Default to HTTPS
            parsed_url = urlparse(url_attempted)
        
        if not parsed_url.netloc:
            logger.error(f"Invalid URL format: {url_attempted}")
            return None, False

        logger.debug(f"Fetching URL: {url_attempted}")
        r = scraper.get(url_attempted, headers=headers, timeout=10)
        r.raise_for_status()

        original_domain = tldextract.extract(url).registered_domain
        final_domain = tldextract.extract(r.url).registered_domain

        if original_domain != final_domain:
            logger.warning(f"URL redirected from {url_attempted} to {r.url} with different base domain. Skipping.")
        return process_content(r.text, r.url)
    
#         soup = BeautifulSoup(r.text, 'html.parser')
#         title_tag = soup.find('title')
#         title = title_tag.text.strip() if title_tag else ''
#         description_tag = soup.find('meta', attrs={'name': 'description'})
#         description = description_tag.get('content', '').strip() if description_tag else ''

#         def extract_text(tags):
#             return ". ".join(tag.get_text(strip=True) for tag in tags)

# #graab html tags that may contain text
#         h1_all = extract_text(soup.find_all('h1'))
#         h2_all = extract_text(soup.find_all('h2'))
#         h3_all = extract_text(soup.find_all('h3'))
#         paragraphs_all = extract_text(soup.find_all('p'))
#         li_all = extract_text(soup.find_all('li'))
#         td_all = extract_text(soup.find_all('td'))
#         a_all = extract_text(soup.find_all('a'))

#         # Combine all content
#         all_content = f"{title} {description} {h1_all} {h2_all} {h3_all} {paragraphs_all} {li_all} {td_all} {a_all}"
#         all_content = all_content.strip()
        
#         if not all_content:
#             logger.warning(f"No content fetched for URL: {url}. Skipping translation.")
#             return None, False  # Skip translation and return None

#         # Log content length for debugging
#         content_length = len(all_content)
#         logger.debug(f"Content length for {url_attempted}: {content_length}")

#         # Check if content is substantial
#         if content_length < 100:
#             logger.info(f"Content too minimal for URL: {url_attempted} (Length: {content_length}). Attempting metadata fallback.")
            
#             # Attempt to use metadata
#             metadata_content = f"{title} {description}".strip()
#             metadata_length = len(metadata_content)
#             logger.debug(f"Metadata content length for {url_attempted}: {metadata_length}")

#             if metadata_length >= 20:
#                 logger.info(f"Using metadata for sentiment analysis for URL: {url_attempted}")
#                 return metadata_content[:9999], True
#             else:
#                 logger.info(f"Metadata too minimal for URL: {url_attempted} (Length: {metadata_length}). Skipping.")
#                 return None, False


#         # Initialize translator
#         translator = Translator()
#         try:
#             logger.debug(f"Translating content for URL: {url}")
#             translation = translate_with_timeout(all_content, translator, timeout=10)
            
#             if not translation:  # Check if translation returned None
#                 logger.warning(f"Translation returned None for URL: {url}. Using original content.")
#                 translation = all_content  # Use original content if translation fails

#             logger.debug(f"Translation successful for URL: {url}")
#             return translation, False
#         except Exception as e:
#             logger.error(f"Translation Error for {url}: {e}")
#             return all_content[:9999], False 

    except NameResolutionError as e:
        # Retry with 'www.' prefix if NameResolutionError is encountered
        if not url.startswith("www."):
            logger.warning(f"Name resolution failed for {url_attempted}, retrying with 'www.' prefix.")
            url_with_www = correct_url("www." + url)
            return fetch_web_content(url_with_www)  # Recursive call with 'www.' prefixed
        else:
            logger.error(f"Failed to resolve {url_attempted} even with 'www.': {e}")
            return None, False
    except requests.exceptions.HTTPError as e:
        # Handle 403 Forbidden by using Selenium as fallback
        if e.response.status_code == 403:
            logger.warning(f"403 Forbidden for URL {url_attempted}. Switching to Selenium.")
            return fetch_with_selenium(url_attempted)
        else:
            logger.error(f"HTTP error for URL {url_attempted}: {e}")
            return None, False
    except Exception as e:
        logger.error(f"Failed to fetch content for {url_attempted} after retries: {e}")
        return None, False

def fetch_with_selenium(url):
    """
    Fetches content using Selenium for pages that return 403 Forbidden errors.
    """
    try:
        logger.debug(f"Fetching URL with Selenium: {url}")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        time.sleep(3)  # Wait for the page to load completely
        content = driver.page_source
        driver.quit()
        return process_content(content, url)
    except Exception as e:
        logger.error(f"Selenium failed to fetch content for {url}: {e}")
        return None, False
    
    
def process_content(content, url):
    """
    Processes HTML content by extracting relevant text, titles, and metadata.
    
    Returns:
        Tuple[str or None, bool]: (Processed content or None, is_metadata_used)
    """
    soup = BeautifulSoup(content, 'html.parser')
    title_tag = soup.find('title')
    title = title_tag.text.strip() if title_tag else ''
    description_tag = soup.find('meta', attrs={'name': 'description'})
    description = description_tag.get('content', '').strip() if description_tag else ''

    def extract_text(tags):
        return ". ".join(tag.get_text(strip=True) for tag in tags)

    # Grab HTML tags that may contain text
    h1_all = extract_text(soup.find_all('h1'))
    h2_all = extract_text(soup.find_all('h2'))
    h3_all = extract_text(soup.find_all('h3'))
    paragraphs_all = extract_text(soup.find_all('p'))
    li_all = extract_text(soup.find_all('li'))
    td_all = extract_text(soup.find_all('td'))
    a_all = extract_text(soup.find_all('a'))

    # Combine all content
    all_content = f"{title} {description} {h1_all} {h2_all} {h3_all} {paragraphs_all} {li_all} {td_all} {a_all}"
    all_content = all_content.strip()

    if not all_content:
        logger.warning(f"No content fetched for URL: {url}. Skipping translation.")
        return None, False  # Skip translation and return None

    # Log content length for debugging
    content_length = len(all_content)
    logger.debug(f"Content length for {url}: {content_length}")

    # Check if content is substantial
    if content_length < 100:
        logger.info(f"Content too minimal for URL: {url} (Length: {content_length}). Attempting metadata fallback.")

        # Attempt to use metadata
        metadata_content = f"{title} {description}".strip()
        metadata_length = len(metadata_content)
        logger.debug(f"Metadata content length for {url}: {metadata_length}")

        if metadata_length >= 20:
            logger.info(f"Using metadata for sentiment analysis for URL: {url}")
            return metadata_content[:9999], True
        else:
            logger.info(f"Metadata too minimal for URL: {url} (Length: {metadata_length}). Skipping.")
            return None, False

    # Truncate content to fit API limits
    all_content = all_content[:9999]
    return all_content, False


"""
breakpoint between functions, below is the sentiment analysis function
"""

def analyze_sentiment(text):
    """
    Analyzes sentiment of the given text using Google Cloud Language API.
    
    Returns:
        Tuple[float or None, float or None]: (Sentiment score, Sentiment magnitude)
    """
    try:
        if len(text.strip()) == 0:
            logger.warning("Empty text provided for sentiment analysis.")
            return None, None

        client = language_v1.LanguageServiceClient()
        document = language_v1.Document(
            content=text, 
            type_=language_v1.Document.Type.PLAIN_TEXT,
            language="en"
        )
        response = client.analyze_sentiment(request={'document': document})
            
        sentiment = response.document_sentiment
        logger.debug(f"Sentiment Score: {sentiment.score}, Sentiment Magnitude: {sentiment.magnitude}")
        return sentiment.score, sentiment.magnitude

    except Exception as e:
        logger.error(f"Sentiment Analysis Error: {e}")
        return None, None
    

def create_csv(input_csv, output_csv):
     # Load the CSV and initializing df
    try:
        df = pd.read_csv(input_csv, header=None, names=["URL", "Label"])
        logger.info(f"Loaded {len(df)} URLs from '{input_csv}'.")
    except FileNotFoundError:
        logger.error(f"Input CSV file '{input_csv}' not found.")
        return
    except Exception as e:
        logger.error(f"Error reading '{input_csv}': {e}")
        return

    # Initialize new columns for sentiment analysis and status
    df['Sentiment Score'] = None
    df['Sentiment Magnitude'] = None
    df['Status'] = None  # New column to indicate processing status



    # List to store successful entries
    successful_entries = []

    # Loop through each row and process the URL
    for index, row in df.iterrows():
        url = row['URL']
        label = row['Label']
        logger.info(f"Processing URL {index + 1}/{len(df)}: {url} (Label: {label})")



        try:
            # Fetch the web content (already translated or metadata)
            web_content, is_metadata = fetch_web_content(url)

            # Skip sentiment analysis if web content is None or empty
            if not web_content:
                logger.warning(f"No content to analyze for URL: {url}. Skipping sentiment analysis.")
                df.at[index, 'Status'] = 'No Content'
                continue  # Skip to the next URL

            # Analyze the sentiment of the content
            sentiment_score, sentiment_magnitude = analyze_sentiment(web_content)

            if sentiment_score is not None:
                logger.info(f"Sentiment Score for {url}: {sentiment_score}")
                logger.info(f"Sentiment Magnitude for {url}: {sentiment_magnitude}")

                # Store the results in the DataFrame
                df.at[index, 'Sentiment Score'] = sentiment_score
                df.at[index, 'Sentiment Magnitude'] = sentiment_magnitude

                # Update status based on whether metadata was used
                if is_metadata:
                    df.at[index, 'Status'] = 'Success (Metadata)'
                    logger.info(f"Sentiment analysis performed on metadata for URL: {url}")
                else:
                    df.at[index, 'Status'] = 'Success'

                # Add to successful entries list
                successful_entries.append({
                    'URL': url,
                    'Label': label,
                    'Sentiment Score': sentiment_score,
                    'Sentiment Magnitude': sentiment_magnitude,
                    'Used Metadata': is_metadata
                })
            else:
                logger.warning(f"Sentiment analysis failed for {url}.")
                df.at[index, 'Status'] = 'Sentiment Analysis Failed'
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            df.at[index, 'Status'] = f"Error: {e}"
            continue  # Skip to the next URL

        # Implement rate limiting with random delays between 1 to 3 seconds
        time.sleep(random.uniform(1, 3))  # Random delay of 1 to 3 seconds



    # Create a DataFrame for successful entries
    successful_df = pd.DataFrame(successful_entries)

    # Save only the successful entries to the output CSV
    try:
        successful_df.to_csv(output_csv, index=False)
        logger.info(f"Successfully processed {len(successful_df)} URLs. Results saved to '{output_csv}'.")
    except Exception as e:
        logger.error(f"Error saving to '{output_csv}': {e}")


#please work im praying
input_csv = "categorizedurls.csv"
output_csv = 'output_with_sentiment.csv'

create_csv(input_csv, output_csv)
