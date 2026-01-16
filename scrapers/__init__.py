from abc import ABC, abstractmethod

class Scraper(ABC):
    def __init__(self, settings):
        self.settings = settings
        self.data = []

    @abstractmethod
    def run(self):
        """Execute the scraping logic."""
        pass
        
    def save_data(self):
        """Save collected data to file based on settings."""
        # This will be implemented in the base or specific classes
        pass
