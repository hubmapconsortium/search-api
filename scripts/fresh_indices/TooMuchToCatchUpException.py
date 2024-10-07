class TooMuchToCatchUpException(Exception):
    """Exception raised when the number of UUIDs to catch-up exceeds the configured value."""

    def __init__(self, message="The number of UUIDs to catch-up exceeds the configured maximum."):
        self.message = message
        super().__init__(self.message)