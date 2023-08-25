from dataclasses import dataclass
import logging
import os
from typing import List

@dataclass
class Anime:
    """
    Model representing an anime in the database
    """
    anilist: int
    mal: int
    title: str
    cover_url: str
    format: str
    status: str
    genres: List[str]
    release_year: int
    normalized_score: float

    def validate(self):
        if self.release_year is not None and self.release_year < 1940:
            self.realease_year = 1940
        
        self.normalized_score = min(max(self.normalized_score, 0), 1)


    
################################################################################
# utils

def get_env(name: str, default_value = None,):
    """Get environment variable with a default value

    Args:
        name: env variable to search
        default_value: given default value for the variable
    
    Returns: 
        Variable value or the default value if provided.
        If no default value is provided the application is crashed
    """
    if name in os.environ:
        return os.environ[name]
    elif default_value is not None:
        logging.info("Using fallback env for %s", name)
        return default_value
    
    logging.critical("Missing env var %s", name)
    exit(5)