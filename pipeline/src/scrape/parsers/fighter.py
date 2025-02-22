import logging
from typing import Optional
from bs4 import BeautifulSoup
from pydantic import ValidationError, BaseModel, Field

logger = logging.getLogger(__name__)

class Fighter(BaseModel):
    """
    A Pydantic model representing the details of a UFC fighter.
    
    Attributes:
        full_name (str): The full name of the fighter.
        nickname (Optional[str]): The nickname of the fighter.
        height (Optional[str]): The height of the fighter in inches.
        reach (Optional[str]): The reach of the fighter in inches.
        stance (Optional[str]): The fighting stance of the fighter.
        date_of_birth (Optional[str]): The date of birth of the fighter.
        record (str): The fight record of the fighter.
    """
    full_name: str
    fighter_url: Optional[str] = Field(default=None)
    nickname: Optional[str] = Field(default=None)
    height: Optional[str] = Field(default=None)
    reach: Optional[str] = Field(default=None)
    stance: Optional[str] = Field(default=None)
    date_of_birth: Optional[str] = Field(default=None)
    record: str

def parse_fighter(soup: BeautifulSoup) -> Fighter:
    """
    Parses fighter details from a BeautifulSoup object, extracting key information
    such as name, nickname, height, reach, stance, date of birth, and record.

    Args:
        soup (BeautifulSoup): A BeautifulSoup object representing the parsed HTML content 
                              from the fighter details page.

    Returns:
       List[Fighter]: A list with a single Fighter object containing parsed details if successfully 
                           extracted and validated; otherwise, returns None if an error 
                           occurs during parsing or validation.
    """

    try:

        # Extract and split the name into first and last names
        full_name = soup.find('span', class_='b-content__title-highlight').text.strip()
        
        # Extract nickname
        nickname = soup.find('p', class_='b-content__Nickname')
        nickname = nickname.text.strip('"').strip() if nickname else ''

        # Extract record
        record = soup.find('span', class_='b-content__title-record')
        record = record.text.strip().replace('Record: ', '') if record else ''

        # Extract height, reach, stance, and date of birth
        stats = soup.find_all('li', class_='b-list__box-list-item')

        for stat in stats:

            title = stat.find('i', class_='b-list__box-item-title').text.strip()
            value = stat.text.replace(title, '').strip()

            if 'height' in title.lower() and value != '--':
                try:
                    height = value
                except ValueError:
                    height = None
                    logger.warning(f"Invalid height format: {value}")
            elif 'height' in title.lower() and value == '--':
                height = None
            elif 'reach' in title.lower() and value != '--':
                try:
                    reach = value
                except ValueError:
                    reach = None
                    logger.warning(f"Invalid reach format: {value}")
            elif 'reach' in title.lower() and value == '--':
                reach = None
            elif 'stance' in title.lower():
                stance = value if value != '--' else None
            elif 'dob' in title.lower():
                date_of_birth = value if value != '--' else None
    except AttributeError as e:
        logger.error(f"Error parsing fighter details: {e}")
        return None
    try:
        fighter = Fighter(
            full_name = full_name,
            nickname = nickname,
            height = height,
            reach = reach,
            stance = stance,
            date_of_birth = date_of_birth,
            record = record)
        return fighter
    except ValidationError as ve:
        logger.error(f"Validation error: {ve}")
        raise ve