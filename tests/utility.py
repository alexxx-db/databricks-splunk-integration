class Response:
    """Sample Response Class."""

    def __init__(self, status_code, json_data=None):
        """Init Method for Response."""
        self.status_code = status_code
        self._json_data = json_data if json_data is not None else {"status_code": self.status_code}

    def json(self):
        """Set json value."""
        return self._json_data

    def raise_for_status(self):
        """Raise exception for non-2xx status codes."""
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")