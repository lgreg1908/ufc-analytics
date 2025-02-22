import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Callable, Any
import requests
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
from bs4 import BeautifulSoup

# Import your parsing functions and data classes
from pipeline.src.scrape.parsers.event import parse_event, Event
from pipeline.src.scrape.parsers.result import parse_results, Result
from pipeline.src.scrape.parsers.round import parse_rounds, Round
from pipeline.src.scrape.parsers.fighter import parse_fighter, Fighter

logger = logging.getLogger(__name__)

class BaseScraper:
    def __init__(self, timeout: int = 10, max_workers: int = 5):
        """
        Initialize the base scraper with a timeout for requests and a max number
        of concurrent workers.
        """
        self.timeout = timeout
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/58.0.3029.110 Safari/537.3'
            )
        })

    def get_soup(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch and parse the HTML content from the given URL.
        """
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except (HTTPError, ConnectionError, Timeout, RequestException) as err:
            logger.error(f"Error fetching URL {url}: {err}")
            return None

    def scrape_many(
        self,
        urls: List[str],
        parser: Callable[[BeautifulSoup], Any],
        attach_url_attr: Optional[str] = None
    ) -> List[Any]:
        """
        Concurrently scrape multiple URLs and parse the responses with the given parser.
        
        Args:
            urls: List of URLs to scrape.
            parser: A callable that accepts a BeautifulSoup object and returns parsed data.
            attach_url_attr: If provided, each parsed object (or each item in a parsed list)
                             will have an attribute with this name set to the URL it came from.
                             
        Returns:
            A list of parsed objects.
        """
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.get_soup, url): url for url in urls}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                soup = future.result()
                if soup is None:
                    logger.warning(f"Failed to retrieve content for URL: {url}")
                    continue

                try:
                    data = parser(soup)
                except Exception as e:
                    logger.error(f"Error parsing content from URL {url}: {e}")
                    continue

                if not data:
                    logger.warning(f"No data parsed from {url}")
                    continue

                # Optionally attach the URL to each parsed item
                if attach_url_attr:
                    if isinstance(data, list):
                        for item in data:
                            setattr(item, attach_url_attr, url)
                    else:
                        setattr(data, attach_url_attr, url)

                if isinstance(data, list):
                    results.extend(data)
                else:
                    results.append(data)
        return results


class EventsScraper(BaseScraper):
    def scrape_events(self, event_urls: List[str]) -> List[Event]:
        """
        Concurrently scrape event data from the list of event URLs.
        """
        logger.info("Starting concurrent scraping of events.")
        events = self.scrape_many(event_urls, parse_event)
        logger.info("Completed scraping events.")
        return events


class ResultsScraper(BaseScraper):
    def scrape_results(self, event_urls: List[str]) -> List[Result]:
        """
        Concurrently scrape fight results from a list of event URLs.
        The parser attaches the event URL to each result using the attribute 'event_url'.
        """
        logger.info("Starting concurrent scraping of fight results.")
        results = self.scrape_many(event_urls, parse_results, attach_url_attr='event_url')
        logger.info("Completed scraping fight results.")
        return results


class FightersScraper(BaseScraper):
    def scrape_fighters(self, fighter_urls: List[str]) -> List[Fighter]:
        """
        Concurrently scrape fighter details from a list of fighter URLs.
        The parser attaches the fighter URL to each fighter object using 'fighter_url'.
        """
        logger.info("Starting concurrent scraping of fighter details.")
        fighters = self.scrape_many(fighter_urls, parse_fighter, attach_url_attr='fighter_url')
        logger.info("Completed scraping fighter details.")
        return fighters


class RoundsScraper(BaseScraper):
    def scrape_rounds(self, fight_urls: List[str]) -> List[Round]:
        """
        Concurrently scrape rounds data from a list of fight URLs.
        The parser attaches the fight URL to each round using 'fight_url'.
        """
        logger.info("Starting concurrent scraping of rounds data.")
        rounds = self.scrape_many(fight_urls, parse_rounds, attach_url_attr='fight_url')
        logger.info("Completed scraping rounds data.")
        return rounds
