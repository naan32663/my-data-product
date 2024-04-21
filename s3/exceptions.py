class S3Exception(Exception):
    """S3 exception"""

    DEFAULT_MESSAGE = "Undefined S3 exception."

    def __init__(self, message: str = None):
        if not message:
            message = self.DEFAULT_MESSAGE
        self._message = message

    @property
    def Message(self):
        """Retrieve the exception message."""
        return self._message

    @Message.setter
    def Message(self, message: str) -> None:
        """Set the exception message."""
        self._message = message

    def __str__(self) -> str:
        return self.Message