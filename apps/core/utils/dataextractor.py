import re
import logging
from typing import Dict, List
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ExtractionPatterns:
    """Centralized regex patterns for data extraction."""

    # Phone number patterns: international and local
    PHONE_PATTERNS = re.compile(r"""
        (?:\+\d{1,3}[-.\s]?)?           # Optional country code with separators
        (?:                             # Main phone number group
            \(\d{3}\)\s?\d{3}[-.\s]?\d{4}[-.\s]?\d? |  # (xxx) xxx-xxxx-x
            \d{3}[-.\s]?\d{3}[-.\s]?\d{4} |             # xxx-xxx-xxxx
            \+\d{10,15} |                               # +xxxxxxxxxx (10-15 digits)
            \d{11} |                                    # 11 digit local
            \(\+\d{1,3}\)[-.\s]?\d{10,12}               # (+xxx)-xxxxxxxxxx
        )
    """, re.VERBOSE)

    # Email pattern
    EMAIL_PATTERNS = re.compile(r"""
        \b[a-zA-Z0-9._%+-]+             # Username part
        @                               # @ symbol
        [a-zA-Z0-9.-]+                  # Domain name
        \.[a-zA-Z]{2,}                  # Top-level domain
        \b
    """, re.VERBOSE)


class DataExtractor:

    def __init__(self):
        self.patterns = ExtractionPatterns()

    def extract_phone_numbers(self, text: str) -> List[str]:
        """
        Extract phone numbers from text using comprehensive patterns.

        Args:
            text (str): Input text to search

        Returns:
            List[str]: List of found phone numbers
        """
        try:
            matches = self.patterns.PHONE_PATTERNS.findall(text)
            clean_matches = [self._clean_phone_number(match) for match in matches if match.strip()]
            logger.info(f"Found {len(clean_matches)} phone numbers")
            return clean_matches
        except Exception as e:
            logger.error(f"Error extracting phone numbers: {e}")
            return []

    def extract_emails(self, text: str) -> List[str]:
        """
        Extract email addresses from text.

        Args:
            text (str): Input text to search

        Returns:
            List[str]: List of found email addresses
        """
        try:
            matches = self.patterns.EMAIL_PATTERNS.findall(text)
            clean_matches = [email.lower().strip() for email in matches]
            # Remove duplicates while preserving order
            unique_emails = list(dict.fromkeys(clean_matches))
            logger.info(f"Found {len(unique_emails)} unique email addresses")
            return unique_emails
        except Exception as e:
            logger.error(f"Error extracting emails: {e}")
            return []

    def extract_all(self, text: str) -> Dict[str, List[str]]:
        """
        Extract all supported data types from text.

        Args:
            text (str): Input text to search

        Returns:
            Dict[str, List[str]]: Dictionary with extraction results
        """
        if not text or not isinstance(text, str):
            logger.warning("Invalid input text provided")
            return {"phone_numbers": [], "emails": []}

        return {
            "phone_numbers": self.extract_phone_numbers(text),
            "emails": self.extract_emails(text)
        }

    @staticmethod
    def _clean_phone_number(phone: str) -> str:
        """Clean and standardize phone number format."""
        # Remove extra whitespace and common separators for display
        cleaned = re.sub(r'[-.\s()]+', '', phone.strip())
        # Re-add formatting for readability if it's a standard format
        if len(cleaned) == 11 and cleaned.isdigit():
            return f"{cleaned[:3]}-{cleaned[3:6]}-{cleaned[6:]}"
        return phone.strip()
