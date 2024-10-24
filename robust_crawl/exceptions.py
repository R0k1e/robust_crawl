class AntiCrawlingDetectedException(Exception):
    """Custom exception for when anti-crawling measures are detected."""
    def __init__(self, message):
        super().__init__(message)
        self.message = message